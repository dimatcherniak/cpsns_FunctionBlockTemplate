import math
from paho.mqtt.client import Client as MQTTClient
#from paho.mqtt.client import CallbackAPIVersion
from paho.mqtt.client import MQTTv311
import time
import queue
import argparse
import json
import sys
import os
import uuid
import re
import numpy as np
import struct

PRIVATE_CONFIG_FILE_DEFAULT = "private_config.json" # locate this file in the private folder and chmod 600 it.
PUBLIC_CONFIG_FILE_DEFAULT = "public_config.json"   # this file can be located in a public folder 

bReading = False
bWriting = False

jobs = {}

# Replaces the subtopics of the topic by the strings in the list
def replace_subtopics(topic, replacements):
    subtopics = topic.split('/')
    for i in range(min(len(subtopics), len(replacements))):
        if replacements[i]:
            subtopics[i] = replacements[i]
    return '/'.join(subtopics)

def on_publish(client, userdata, mid):
    #print(f"Message {mid} published.")
    pass

def on_connect_in(mqttc_in, userdata, flags, rc, properties=None):
    global json_config_public
    print("MQTT_IN: Connected with response code %s" % rc)
    for topic in json_config_public["MQTT_IN"]["TopicsToSubscribe"]:
        print(f"MQTT_IN: Subscribing to the topic {topic}...")
        mqttc_in.subscribe(topic, qos=json_config_public["MQTT_IN"]["QoS"])

def on_connect_out(mqttc_in, userdata, flags, rc, properties=None):
    print("MQTT_OUT: Connected with response code %s" % rc)

def on_subscribe(self, mqttc_in, userdata, msg):
    print("Subscribed. Message: " + str(msg))

def on_message(client, userdata, msg):
    global bReading, bWriting
    global jobs

    #print(f"Topic: {msg.topic}\nPayload:\n{msg.payload}")

    while bReading:        
        time.sleep(0.01) # make the thread sleep
    bWriting = True

    # Generate a "job" and add it to the jobs
    job = {"State": "New", "Topic": msg.topic, "Payload": msg.payload}
    jobs[uuid.uuid4()] = job

    bWriting = False

def process_the_job(my_job):
    # As an example, I extract data from the data MQTT message ...
    # Is it data or metadata?
    topic = my_job["Topic"]
    substrings = topic.split('/')
    bIsMetadata = True if substrings[-1] == "metadata" else False
    
    # For the data messages, unpack and put to numpy array
    if bIsMetadata:
        pass
    else:
        payload = my_job["Payload"]
        # Trying to load in big-endian and little-endian ways
        char_LE, char_BE = '<','>'
        temp, metadataVer_LE = struct.unpack_from(char_LE+'HH', payload) # assumes little endian here
        temp, metadataVer_BE = struct.unpack_from(char_BE+'HH', payload) # assumes big endian here
        char_Endian = char_LE if metadataVer_LE < metadataVer_BE else char_BE
        
        # re-read the descriptor
        descriptorLength, metadataVer = struct.unpack_from(char_Endian+'HH', payload) 

        # how many samples and what's its type, float or double?
        cType = 'f' # for simplicity assuming float. The true type to be found in the metadata
        # calculate nSamples from the payload length
        payload_len = len(payload)
        nSamples = round((payload_len-descriptorLength)/struct.calcsize(cType))

        # Data
        strBinFormat = char_Endian + str(nSamples) + str(cType)  # e.g., '>640f' for 640 floats, big-endian
        # data
        data = np.array(struct.unpack_from(strBinFormat, payload, descriptorLength))
        # time stamp of the payload
        secFromEpoch = struct.unpack_from(char_Endian + 'Q', payload, 4)[0]
        nanosec = struct.unpack_from(char_Endian + 'Q', payload, 12)[0]
        # nSamples
        nSamplesFromDAQStart = 0
        if metadataVer >= 2:
            nSamplesFromDAQStart = struct.unpack_from(char_Endian + 'Q', payload, 20)[0]
        else:
            raise Exception("Too old version!")

        my_job["Data"] = data
        my_job["SamplesFromDAQStart"] = nSamplesFromDAQStart
        my_job["AbsTime"] = {"Seconds": secFromEpoch, "Nanosec": nanosec}

    # Change the status
    my_job["State"] = "Executing"

def finalize_the_job(mqttc_out, my_job):
    global json_config_public
    # As an example, I publish the message...
    newTopic = replace_subtopics(my_job["Topic"], json_config_public["MQTT_OUT"]["ModifySubtopics"])
    #print(f"Publishing with topic {newTopic}")
    mqttc_out.publish(newTopic, my_job["Payload"], qos=json_config_public["MQTT_OUT"]["QoS"],)
    #
    # Change the status
    my_job["State"] = "Finished"

def main():
    global json_config_private, json_config_public
    global bReading, bWriting
    global jobs

    # Parse command line parameters
    # Create the parser
    parser = argparse.ArgumentParser(description="Write the description here...")
    parser.add_argument('--config_private', type=str, help='Specify the JSON configuration file for PRIVATE data. Defaults to ' + PRIVATE_CONFIG_FILE_DEFAULT, default=PRIVATE_CONFIG_FILE_DEFAULT)
    parser.add_argument('--config_public', type=str, help='Specify the JSON configuration file for PUBLIC data. Defaults to ' + PUBLIC_CONFIG_FILE_DEFAULT, default=PUBLIC_CONFIG_FILE_DEFAULT)

    # Parse the arguments
    args = parser.parse_args()

    # Name of the configuration file
    strConfigFile = args.config_private
    # Read the configuration file
    print(f"Reading private configuration from {strConfigFile}...")
    if os.path.exists(strConfigFile):
        try:
            # Open and read the JSON file
            with open(strConfigFile, 'r') as file:
                json_config_private = json.load(file)
        except json.JSONDecodeError:
            print(f"Error: The file {strConfigFile} exists but could not be parsed as JSON.", file=sys.stderr)
            sys.exit(1)
    else:
        print(f"Error: The file {strConfigFile} does not exist.", file=sys.stderr)    
        sys.exit(1)

    # Name of the configuration file
    strConfigFile = args.config_public
    # Read the configuration file
    print(f"Reading public configuration from {strConfigFile}...")
    if os.path.exists(strConfigFile):
        try:
            # Open and read the JSON file
            with open(strConfigFile, 'r') as file:
                json_config_public = json.load(file)
        except json.JSONDecodeError:
            print(f"Error: The file {strConfigFile} exists but could not be parsed as JSON.", file=sys.stderr)
            sys.exit(1)
    else:
        print(f"Error: The file {strConfigFile} does not exist.", file=sys.stderr)    
        sys.exit(1)

    # MQTT_IN stuff
    #mqttc_in = MQTTClient(callback_api_version=CallbackAPIVersion.VERSION2, protocol=MQTTv311)
    mqttc_in = MQTTClient()

    # Set username and password
    if json_config_private["MQTT_IN"]["userId"] != "":
        mqttc_in.username_pw_set(json_config_private["MQTT_IN"]["userId"], json_config_private["MQTT_IN"]["password"])

    mqttc_in.on_connect = on_connect_in
    mqttc_in.on_message = on_message
    mqttc_in.on_subscribe = on_subscribe
    mqttc_in.connect(json_config_private["MQTT_IN"]["host"], json_config_private["MQTT_IN"]["port"], 60) # we subscribe to the topics in on_connect callback

    mqttc_in.loop_start()
    # MQTT_IN done

    # MQTT_OUT stuff
    #mqttc_out = MQTTClient(callback_api_version=CallbackAPIVersion.VERSION2, protocol=MQTTv311)
    mqttc_out = MQTTClient()

    # Set username and password
    if json_config_private["MQTT_OUT"]["userId"] != "":
        mqttc_out.username_pw_set(json_config_private["MQTT_OUT"]["userId"], json_config_private["MQTT_OUT"]["password"])

    # TLS configuration (ONLY if enabled)
    if json_config_private["MQTT_IN"].get("useTLS", False):
        mqttc_in.tls_set(
            ca_certs=json_config_private["MQTT_IN"]["caFile"],
            certfile=None,
            keyfile=None,
            tls_version=ssl.PROTOCOL_TLS_CLIENT
        )

        # Optional but recommended
        mqttc_in.tls_insecure_set(False)

    mqttc_out.on_connect = on_connect_out
    mqttc_out.on_publish = on_publish
    mqttc_out.connect(json_config_private["MQTT_OUT"]["host"], json_config_private["MQTT_OUT"]["port"], 60) 

    mqttc_out.loop_start()
    # MQTT_OUT done

    while True:
        time.sleep(0.1)

        while bWriting:        
            time.sleep(0.01) # make the thread sleep
        bReading = True

        # Check the jobs...
        for key, value in jobs.items():
            if value["State"] == "New":
                print(f"New job found. Topic = {value['Topic']}")
                process_the_job(value)

            if value["State"] == "Executing":
                print(f"Job is processed. Topic = {value['Topic']}")
                finalize_the_job(mqttc_out, value)
            
            if value["State"] == "Finished":
                print(f"Job is finished. Topic = {value['Topic']}")

        # Clean up the jobs dictionary
        for key in list(jobs.keys()):
            if jobs[key]["State"] == "Finished":
                del jobs[key]
                print(f"Finished job {key} is deleted.")
                continue
            if jobs[key]["State"] == "Invalid":
                del jobs[key]
                print(f"Invalid job {key} is deleted.")
                continue

        bReading = False

if __name__ == "__main__":
    main()
