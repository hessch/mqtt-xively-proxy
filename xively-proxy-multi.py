#!/usr/bin/env python
#
# xively-proxy-multi - receive values via mqtt and send to xively
# Hessel Schut, hessel@isquared.nl, 2016-03-28
#
# v0.6 - allow for multiple variables in json messages, 2016-06-19
# v0.5 - map multiple mqtt topics to xively feeds, 2016-06-12

# xively-proxy-multi expects to receive simple json encoded key-value
# messages, e.g. { 'key1': value1, 'key2':, value2, ... }



import paho.mqtt.client as mqtt
import xively
import time
import datetime
import json

XIVELY_API_KEY = "insert master key here"

MQTT_HOST='my.mqtt.host.fqdn'
MQTT_PORT=1883
MQTT_USER='username'
MQTT_PASS='password'

# example feed map
TOPIC_TO_FEED_MAP = {
	'node-10668763/power': [('vdd33', 674440820)],
	'node-8395148/power': [('vdd33', 712404878)],
	'node-14044197/power': [('vdd33', 1559948308)],
	'node-14044197/environment': [
		('temperature', 1559948308),
		('humidity', 1559948308)
	],
	'node-10669137/power': [('vdd33', 1013443607)],
	'node-10669137/environment': [('temperature', 1013443607)]
}

def on_connect(client, userdata, rc):
	print("Connected with result code "+str(rc))
	for topic in TOPIC_TO_FEED_MAP:
		print('subscribing to topic %s' % topic)
		client.subscribe(topic)

def on_xively_connect(client, userdata, rc):
	print("Connected with result code "+str(rc))

def on_message(client, userdata, msg):
	now = datetime.datetime.utcnow()
	print(now.isoformat() + ": " + msg.topic+" "+str(msg.payload))
	
	for param in TOPIC_TO_FEED_MAP[msg.topic]:
		val = json.loads(msg.payload)[param[0]]
		api = xively.XivelyAPIClient(XIVELY_API_KEY)
		feed = api.feeds.get(param[1])
		feed.datastreams = [
			xively.Datastream(
				id=param[0], 
				current_value=val, 
				at=now)
		]
		feed.update()

def on_disconnect(client, userdata, rc):
	client.reconnect()

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect

client.username_pw_set(username=MQTT_USER, password=MQTT_PASS)
client.connect(MQTT_HOST, MQTT_PORT, 60)

client.loop_forever()
