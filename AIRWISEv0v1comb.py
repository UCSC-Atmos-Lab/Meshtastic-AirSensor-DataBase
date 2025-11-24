import paho.mqtt.client as mqtt
import json
import time
from datetime import datetime, timedelta
import os
import psycopg2
from psycopg2 import sql
import threading
import requests

# MQTT broker details
broker_address = "mqtt.meshtastic.org"
port = 1883
pg_options = {"user":"postgres", "host":"localhost", "database":"eureka", "password":"Charan", "port":5432}
username = "meshdev"
password = "large4cats"

# Ntfy
NTFY_URL = "https://ntfy.sh/FIRESMART_Alerts"
OFFLINE_THRESHOLD_MINUTES = 100 # every 100 minutes right now because nodealert sends 1 message every 15 minutes


# maps nodes
node_dict = {} 

# tracks last time we saw a message from each node
node_heartbeats = {}

#prevents multiple alerts for one offline node
node_alerts_sent = {}

# Topics
topics = [
    "msh/US/2/json/SensorData/!ba69aec8"
    # "msh/US/2/json/SensorData/!ba6562e4"
    # "msh/US/2/json/SensorData/!7d528614"
]

# Initialize node_dict with known nodes
node_dict = {
    2102560276: ('!7d528614', 'Farm6'),

}

#payload":{"text":"23.35,41.69,985.34,185623.00,1.00,1.00,1.00,4.98,148.62\n"
#Received message on topic: msh/US/2/json/SensorData/!7d528614
# payload:  {"channel":0,"from":2102560276,"hop_start":7,"hops_away":0,"id":4072374199,"payload":{"air_util_tx":0.438472211360931,"battery_level":101,"channel_utilization":2.71000003814697,"uptime_seconds":1263,"voltage":0},"sender":"!7d528614","timestamp":1760748340,"to":4294967295,"type":"telemetry"}


#Battery inserted -> node 2102560276, V=0, Lvl=101
#payload:  {"channel":0,"from":2102560288,"hop_start":7,"hops_away":0,"id":3572686211,
# "payload":{"text":"23.35,41.69,985.34,185623.00,1.00,1.00,1.00,4.98,148.62\n"},"rssi":-5,
# "sender":"!7d528614","snr":7.25,"timestamp":1760748353,"to":4294967295,"type":"text"}


def send_ntfy_alert(node_id, longname=None):
    try:
        if longname:
            message = f" Node OFFLINE: {longname} (**ID: {node_id}**) - No message for {OFFLINE_THRESHOLD_MINUTES} minutes"
        else:
            message = f" Node OFFLINE: **{node_id}** - No message for {OFFLINE_THRESHOLD_MINUTES} minutes"


        response = requests.post(NTFY_URL, data=message.encode('utf-8'), headers={"Title": "FIRESMART Node Alert", "Priority": "high", "Tags": "rotating_light", "Markdown": "yes"})
        #could add an image in the message of where the node is located on the farm, once we place the nodes...

        if response.status_code == 200:
            print(f"Alert sent successfully for node {node_id}")
        else:
            print(f"Failed to send alert for node {node_id}: {response.status_code}")
            
    except Exception as e:
        print(f"Error sending ntfy alert: {e}")



# checks every minute for offline nodes
def check_node_heartbeats():
    while True:
        try:
            current_time = datetime.now()
            threshold = timedelta(minutes=OFFLINE_THRESHOLD_MINUTES)
            
            #check every node
            for node_id, last_seen in list(node_heartbeats.items()):
                time_since_last = current_time - last_seen
                
                # check if node is offline
                if time_since_last > threshold:
                    if not node_alerts_sent.get(node_id, False): # send one alert
                        node_info = node_dict.get(node_id, (None, None))
                        longname = node_info[1] if node_info else None
                        
                        print(f"Node {node_id} ({longname}) is OFFLINE - Last seen: {last_seen}")
                        send_ntfy_alert(node_id, longname)
                        node_alerts_sent[node_id] = True
                else:
                    # node is back online
                    if node_alerts_sent.get(node_id, False):
                        node_alerts_sent[node_id] = False
                        print(f"Node {node_id} is back ONLINE")
            
            
            time.sleep(600) # EVERY 10 MINUTES
            
        except Exception as e:
            print(f"Error in heartbeat checker: {e}")
            time.sleep(600)  




#payload:  {"channel":0,"from":2102560288,"hop_start":7,"hops_away":0,"id":3572686211,
# "payload":{"text":"23.35,41.69,985.34,185623.00,1.00,1.00,1.00,4.98,148.62\n"},"rssi":-5,
# "sender":"!7d528614","snr":7.25,"timestamp":1760748353,"to":4294967295,"type":"text"}

def parse_text_data(payload):
    try:
        data = json.loads(payload)
        
        node = data.get('from', None)

        if data.get('type', None) != "text":
            print("Not data packet, ignoring")
            return None

        payload_data = data.get('payload') or {}
        text = payload_data.get('text', '')

        parts = []
        for field in text.strip().split(","):
            trimmed = field.strip()
            if trimmed:
                parts.append(trimmed)

        values = []
        for p in parts:
            values.append(float(p))

        # get node info from dictionary
        topic_info = node_dict.get(node, (None, None))

        return {
            'node': node,
            'topic_id': topic_info[0],
            'longname': topic_info[1],
            'temperature': values[0],
            'humidity': values[1],
            'pressure': values[2],
            'gas': values[3],
            'pm1_0': values[4],
            'pm2_5': values[5],
            'pm10': values[6],
            'bus_voltage': values[7],
            'current_mA': values[8],

            'timestamp_node': data.get('timestamp', None),
            'pst_time': datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')
        }
        
    except (json.JSONDecodeError, ValueError, KeyError) as e:
        print(f"Error parsing sensor data: {e}")
        return None



def parse_battery_data(payload):
    try:
        data = json.loads(payload)
        
        node = data.get('from', None)

        # ignore packet if it is not battery telemetry
        if data.get('type', None) != "telemetry":
            print("Not data packet, ignoring")
            return None
        
        payload_data = data.get('payload') or {}

        # get node info from dictionary
        topic_info = node_dict.get(node, (None, None))

        return {
                'node': node,
                'topic_id': topic_info[0],
                'longname': topic_info[1],
                'voltage': payload_data.get('voltage', None),
                'battery_level': payload_data.get('battery_level', None),
                'timestamp_node': data.get('timestamp', None),
                'pst_time': datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')
        }

    except (json.JSONDecodeError, ValueError, KeyError) as e:
        print(f"Error parsing sensor data: {e}")
        return None



def parse_v0telemetry_data(payload):
    try:
        data = json.loads(payload)
        
        node = data.get('from', None)

        # ignore packet if it is not telemetry
        if data.get('type', None) != "telemetry":
            print("Not telemetry packet, ignoring")
            return None
        
        payload_data = data.get('payload', {})




        # get node info from dictionary
        topic_info = node_dict.get(node, (None, None))


        #save telemetry data
        if 'battery_level' in payload_data:
            return None #already handled battery data

        return {
            'node': node,
            'topic_id': topic_info[0],
            'longname': topic_info[1],
            'pressure': payload_data.get('barometric_pressure', None),
            'gas': payload_data.get('gas_resistance', None),
            'iaq': payload_data.get('iaq', None),
            'humidity': payload_data.get('relative_humidity', None),
            'temperature': payload_data.get('temperature', None),
            'timestamp_node': data.get('timestamp', None),
            'pst_time': datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')
        }
        
    except (json.JSONDecodeError, ValueError, KeyError) as e:
        print(f"Error parsing sensor data: {e}")
        return None

def insert_to_database(sensor_data, battery_data, telemetry_data):

    try:
        pg_client = psycopg2.connect(**pg_options)
        pg_cursor = pg_client.cursor()
        if(battery_data):

            insert_query = """
                INSERT INTO battery_data (node, topic_id, longname, voltage, battery_level, pst_time)
                VALUES (%s, %s, %s, %s, %s, %s)
            """

            params = (
            battery_data['node'],
            battery_data['topic_id'],
            battery_data['longname'],
            battery_data.get('voltage'),
            battery_data.get('battery_level'),
            battery_data.get('pst_time'),
        )
            pg_cursor.execute(insert_query, params)
            pg_client.commit()

            print(f"Battery inserted -> node {battery_data['node']}, "
            f"V={battery_data.get('voltage')}, "
            f"Lvl={battery_data.get('battery_level')}")
        else:
            print("Skipping battery insertion, no battery data received")

        if(sensor_data):

            insert_query = """
                INSERT INTO airwise_datav1 (node, topic_id, longname, temperature, humidity, pressure, gas, pm1_0, pm2_5, pm10, timestamp_node, pst_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = (
                sensor_data['node'],
                sensor_data['topic_id'],
                sensor_data['longname'],
                sensor_data.get('temperature'),
                sensor_data.get('humidity'),
                sensor_data.get('pressure'),
                sensor_data.get('gas'),
                sensor_data.get('pm1_0'),
                sensor_data.get('pm2_5'),
                sensor_data.get('pm10'),
                sensor_data.get('timestamp_node'),
                sensor_data.get('pst_time'),
        )
            pg_cursor.execute(insert_query, params)
            pg_client.commit()
            print(f"Env inserted -> node {sensor_data['node']}, "
                  f"T={sensor_data.get('temperature')}°C, "
                  f"RH={sensor_data.get('humidity')}%, "
                  f"PM2.5={sensor_data.get('pm2_5')}µg/m3")
        
        if(telemetry_data):
            insert_query = """
                INSERT INTO airwise_data (node, topic_id, longname, pressure, gas, iaq, humidity, temperature, timestamp_node, pst_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = (
                telemetry_data['node'],
                telemetry_data['topic_id'],
                telemetry_data['longname'],
                telemetry_data.get('pressure'),
                telemetry_data.get('gas'),
                telemetry_data.get('iaq'),
                telemetry_data.get('humidity'),
                telemetry_data.get('temperature'),
                telemetry_data.get('timestamp_node'),
                telemetry_data.get('pst_time'),
            )
            pg_cursor.execute(insert_query, params)
            pg_client.commit()
            print(f"Env inserted -> node {telemetry_data['node']}, "
                  f"T={telemetry_data.get('temperature')}°C, "
                  f"RH={telemetry_data.get('humidity')}%"
                  f" Time={telemetry_data.get('pst_time')}")
        
        else:
            print("Skipping sensor insertion, no sensor data received")
        print()
        pg_cursor.close()
        pg_client.close()
        
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        if 'pg_client' in locals():
            pg_client.rollback()
    except Exception as e:
        print(f"Unexpected error during database insertion: {e}")

    finally:
        # close connections 
        if 'pg_cursor' in locals() and pg_cursor:
            pg_cursor.close()
        if 'pg_client' in locals() and pg_client:
            pg_client.close()



# maps the unique "from" number to the topic id and longname using node_info packets
def map_nodes(payload):

    data = json.loads(payload)

    if data.get('type', None) != "nodeinfo":
        return None
    
    print("--------------------Node info packet received, mapping node------------------------------")
    
    payload_data = data.get('payload', {})
    topic_id = payload_data.get('id')
    longname = payload_data.get('longname')
    node = data.get('from', None)

    if node is not None and topic_id is not None and longname is not None:
        node_dict[node] = (topic_id, longname)
    
    # Update heartbeat timestamp
    if node is not None:
        node_heartbeats[node] = datetime.now()
        # reset alert flag if back online
        if node_alerts_sent.get(node, False):
            node_alerts_sent[node] = False
            print(f"Node {node} ({longname}) is back ONLINE - heartbeat received")



def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected with result code {reason_code}")
    if reason_code == 0:
        print("Successfully connected to the broker.")
        for topic in topics:
            print(f"Subscribing to topic: {topic}")
            client.subscribe(topic)
    else:
        print(f"Failed to connect. Reason code: {reason_code}")

def on_message(client, userdata, msg):
    print(f"Received message on topic: {msg.topic}")
    try:
        #decode payload
        payload = msg.payload.decode()

        #maps nodes
        map_nodes(payload)


        print("Here is the payload: ", payload)
        
        #parse data
        sensor_data = parse_text_data(payload)
        battery_data = parse_battery_data(payload)
        telemetry_data = parse_v0telemetry_data(payload)


        print()
        print("Node mapping: ", node_dict)
        print()

        #insert data

        insert_to_database(sensor_data, battery_data, telemetry_data)

            
    except Exception as e:
        print(f"An unexpected error occurred while processing the message: {e}")

def on_disconnect(client, userdata, rc, properties=None, reason_code=None):
    if rc == 0:
        print("Disconnected successfully")
    else:
        print("Unexpected disconnection with result code:", rc)
        requests.post(NTFY_URL, data=b"Disconnected, (laptop closed) ", headers={"Title":"FIRESMART Monitor OFFLINE","Priority":"default","Tags":"warning"})


def test_database_connection():
    try:
        pg_client = psycopg2.connect(**pg_options)
        pg_cursor = pg_client.cursor()
        pg_cursor.execute("SELECT version();")
        version = pg_cursor.fetchone()
        print(f"Successfully connected to PostgreSQL: {version[0]}")
        pg_cursor.close()
        pg_client.close()
        return True
    except Exception as e:
        print(f"Failed to connect to database: {e}")
        return False







# main loop

if __name__ == "__main__":
    if not test_database_connection():
        print("Exiting due to database connection failure")
        
        exit(1)



    # # --- TEST: pretend farm1 (!ba654d80, from=3127201152) went silent ---
    # node_dict[3127201152] = ("!ba654d80", "Farm1")          
    # node_heartbeats[3127201152] = datetime.now() - timedelta(minutes=10) 
    # node_alerts_sent.pop(3127201152, None)                  
    # # ------------------------------------------------------------------------------
    
    # Start the heartbeat checker thread
    heartbeat_thread = threading.Thread(target=check_node_heartbeats, daemon=True)
    heartbeat_thread.start()
    print("Started heartbeat monitoring thread")
    print(node_dict)
    
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(username, password)

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    print(f"Connecting to {broker_address}:{port}")
    client.connect(broker_address, port, 60)


    # online alert- don't need if trying to get under max 250 messages per day
    topics_str = ", ".join(topics)
    requests.post(NTFY_URL,data=f"Monitor started: subscribed and listening to {topics_str}".encode("utf-8"), headers={"Title": "FIRESMART Monitor Online", "Priority": "default", "Tags": "white_check_mark"},)


    try:
        print("Starting the MQTT loop...")
        client.loop_forever()
    except KeyboardInterrupt:
        print("Script interrupted by user")
        requests.post(NTFY_URL, data=b"Keyboard interrupt, disconnecting", headers={"Title":"FIRESMART Monitor OFFLINE","Priority":"high","Tags":"warning"})

        client.disconnect()
    except Exception as e:
        print(f"An error occurred: {e}")
        requests.post(NTFY_URL, data=b"Error occurred: disconnecting", headers={"Title":"FIRESMART Monitor OFFLINE","Priority":"high","Tags":"warning"})

        client.disconnect()