import paho.mqtt.client as mqtt
import json
import time
from datetime import datetime
import os
import psycopg2
from psycopg2 import sql

# MQTT broker details
broker_address = "mqtt.meshtastic.org"
port = 1883
pg_options = {"user":"postgres", "host":"localhost", "database":"eureka", "password":"Charan", "port":5432}
username = "meshdev"
password = "large4cats"


node_dict = {}


# Topics
topics = [
    "msh/US/2/json/SensorData/!ba69aec8" 
]



def parse_sensor_data(payload):
    try:
        data = json.loads(payload)
        
        node = data.get('from', None)

        # ignore packet if it is not telemetry
        if data.get('type', None) != "telemetry":
            print("Not telemetry packet, ignoring")
            return None
        
        payload_data = data.get('payload', {})

        # ignore packet if it is power telemetry (for now)
        if 'battery_level' in payload_data:
            print("Power telemetry packet, ignoring")
            return None


        # get node info from dictionary
        topic_info = node_dict.get(node, (None, None))


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
            
            # add accurate time later datetime.utcnow()
        }
        
    except (json.JSONDecodeError, ValueError, KeyError) as e:
        print(f"Error parsing sensor data: {e}")
        return None

def insert_to_database(sensor_data):

    try:
        pg_client = psycopg2.connect(**pg_options)
        pg_cursor = pg_client.cursor()
        
        insert_query = """
            INSERT INTO airwise_data (node, topic_id, longname, pressure, gas, iaq, humidity, temperature, timestamp_node) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        


        pg_cursor.execute(insert_query, (
            sensor_data['node'],
            sensor_data['topic_id'],
            sensor_data['longname'],
            sensor_data['pressure'],
            sensor_data['gas'],
            sensor_data['iaq'],
            sensor_data['humidity'],
            sensor_data['temperature'],
            sensor_data['timestamp_node']
        ))
        
        pg_client.commit()
        print(f"Data successfully inserted into database - Node: {sensor_data['node']}, Temp: {sensor_data['temperature']}°C, Humidity: {sensor_data['humidity']}%")
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
        sensor_data = parse_sensor_data(payload)

        print()
        print("Node mapping: ", node_dict)
        print()

        #insert data
        if sensor_data:
            insert_to_database(sensor_data)
        else:
            print("Failed to parse sensor data, skipping database insertion")
            print()
        
            
    except Exception as e:
        print(f"An unexpected error occurred while processing the message: {e}")

def on_disconnect(client, userdata, rc, properties=None, reason_code=None):
    if rc == 0:
        print("Disconnected successfully")
    else:
        print("Unexpected disconnection with result code:", rc)

# Test database connection on startup
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
    
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(username, password)

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    print(f"Connecting to {broker_address}:{port}")
    client.connect(broker_address, port, 60)

    try:
        print("Starting the MQTT loop...")
        client.loop_forever()
    except KeyboardInterrupt:
        print("Script interrupted by user")
        client.disconnect()
    except Exception as e:
        print(f"An error occurred: {e}")
        client.disconnect()