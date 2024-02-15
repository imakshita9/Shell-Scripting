#A program to write subscriber
#Broker: Mosquitto localhost

import paho.mqtt.client as mqtt
import json

subscriber = mqtt.Client(client_id="cdacsensor01",protocol=mqtt.MQTTv5)
TOPIC = 'cdac/temp'

#Setting the username password: USERNAME | PASSWORD
subscriber.username_pw_set("diot","diot123")

WILL_TOPIC = 'device/dead'
#set my will to the broker : TOPIC | Payload | Qos | RETAIN
subscriber.will_set(WILL_TOPIC,"Sensor DHT22 is offline",qos=1,retain=True)

#define a callback method to check broker connection
def on_connection(client,userdata,flags,rc,properties):
    if rc == mqtt.MQTT_ERR_SUCCESS:
        print("I'm connected to mqtt broker now")
        subscriber.subscribe(TOPIC)
    else:
        print("some error during connection")

#define callback to receive MQTT messages
def on_receive(client,userdata,msg):
    print(f'TOPIC: {msg.topic}, Message: {str(msg.payload)}')
    data = json.loads(msg.payload)
    if(data["temperature"] > 30):
        print("Temperature is greater than 30")
        print ("data is between 10 and 20")
        f = open("demofile22.json", "a")
        f.write(json.dumps(data))
        f.write("\n")
        f.close()
        
subscriber.on_connect = on_connection
subscriber.on_message = on_receive

# requesting connection to broker here: host,port,keepalive
subscriber.connect("localhost",1883)
subscriber.loop_forever()