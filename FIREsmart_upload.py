import paho.mqtt.client as mqtt
import json
import time
import pymongo
from datetime import datetime
import os
import psycopg2
# MQTT broker details
broker_address = "mqtt.meshtastic.org" #
port = 1883
pg_options = {"user":"joey", "host":"localhost", "database":"eureka", "password":"1234", "port":5432} # might have to change the host because my computer is not running the database
username = "meshdev"
password = "large4cats"
#cluster = pymongo.MongoClient("mongodb+srv://jurvilla:myPword811$@testcluster.1ikxgwz.mongodb.net/?retryWrites=true&w=majority&appName=TestCluster", tls=True, tlsAllowInvalidCertificates=True) #
#db = cluster["todo_db"] #
#mqttCollections = [db["todo_collection"]] #
# Topics
topics = [ #
    "msh/US/2/e/LongFast/!7d527f20"  # Replace this with your second topic #
]   #

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
        #time.sleep(15) #added_sleep
        payload = msg.payload.decode() 
        print("Here is the payload: ", payload)
        pg_client = psycopg2.connect(pg_options)
        pgQuery = " INSERT INTO sensor_data (temp, humidity, soil_moisture, wind_speed, wind_direction, 2.5PM, 1PM, 10PM, AIQ, barometric_pressure, rainfall, methane) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)"
        values = payload # we will need to get out values out of string format
        pg_cursor = pg_client.cursor()
        pg_cursor.execute(pgQuery, values)
        #mqttCollections[0].insert_one({"collection" : "todo_collection",
                                        #"message" : payload,
                                        #})
        

        # elif payload["type"] == "telemetry" and "battery_level" not in payload["payload"]: # TODO: add barometry if relevant
        #     mqttCollections[1].insert_one({"id" : payload["id"],
        #                                    #"device" : payload["sender"],
        #                                    "date" : datetime.fromtimestamp(int(payload["timestamp"])),
        #                                    "barometric_pressure" : payload["payload"]["barometric_pressure"] if "barometric_pressure" in payload["payload"] else None,
        #                                    "gas_resistance" : payload["payload"]["gas_resistance"] if "gas_resistance" in payload["payload"] else None,
        #                                    "air_quality" : payload["payload"]["iaq"] if "iaq" in payload["payload"] else None,
        #                                    "humidity" : payload["payload"]["relative_humidity"] if "relative_humidity" in payload["payload"] else None,
        #                                    "wind_speed" : payload["payload"]["wind_speed"] if "wind_speed" in payload["payload"] else None,
        #                                    "light" : payload["payload"]["lux"] if "lux" in payload["payload"] else None,
        #                                    "temperature_f" : round(payload["payload"]["temperature"] * 9/5 + 32, 1) if "temperature" in payload["payload"] else None
        #                                    })
        # else:
        #     mqttCollections[2].insert_one({"id" : payload["id"],
        #                                    #"device" : payload["sender"],
        #                                    "battery" : payload["payload"]["battery_level"] if "battery_level" in payload["payload"] else None, # battery screws up everything in telemetry, so it is added here
        #                                    "type" : payload["type"]
        #                                    })
        # print("Upload Service working!")
        # print("Decoded JSON:")
        # print(json.dumps(payload, indent=2))
    except json.JSONDecodeError as e:
        print(f"Failed to decode JSON. Error: {e}")
        print(f"Raw message content: {msg.payload.decode()}")
    except Exception as e:
        print(f"An unexpected error occurred while processing the message: {e}")

def on_disconnect(client, userdata, rc, properties=None, reason_code=None):
    if rc == 0:
        print("Disconnected successfully")
    else:
        print("Unexpected disconnection with result code:", rc)

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
