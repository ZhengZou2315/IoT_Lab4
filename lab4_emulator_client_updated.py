# Import SDK packages
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import json
import pandas as pd
import numpy as np


#TODO 1: modify the following parameters
#Starting and end index, modify this
device_st = 0
device_end = 5

#Path to the dataset, modify this
data_path = "vehicle{}.csv"

#Path to your certificates, modify this
certificate_formatter = "thing{}/thing{}-certificate.pem.crt"
key_formatter = "thing{}/thing{}-private.pem.key"
root_ca = "thing0/AmazonRootCA1.pem"

class MQTTClient:
    def __init__(self, device_id, cert, key):
        # For certificate based connection
        self.device_id = str(device_id)
        self.state = 0
        self.client = AWSIoTMQTTClient(self.device_id)
        #TODO 2: modify your broker address
        self.client.configureEndpoint("a2nqt5125qbvm8-ats.iot.us-west-2.amazonaws.com", 8883)
        self.client.configureCredentials(root_ca, key, cert)
        self.client.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
        self.client.configureDrainingFrequency(2)  # Draining: 2 Hz
        self.client.configureConnectDisconnectTimeout(10)  # 10 sec
        self.client.configureMQTTOperationTimeout(5)  # 5 sec
        self.client.onMessage = self.customOnMessage
        

    def customOnMessage(self,message):
        #TODO3: fill in the function to show your received message
        # print("client {} received payload {} from topic {}".format(self.device_id, message.payload, message.topic))
        payload = json.loads(message.payload)
        payload = {'client_id':self.device_id,'co2_max':payload['data']}
        # print('type payload:',type(payload))
        print("client {} received payload {} from topic {}".format(self.device_id, payload, message.topic))
        self.client.publishAsync("iot_core_topic_filter", json.dumps(payload), 0, ackCallback=self.customPubackCallback)


    # Suback callback
    def customSubackCallback(self,mid, data):
        #You don't need to write anything here
        pass


    # Puback callback
    def customPubackCallback(self,mid):
        #You don't need to write anything here
        pass


    def publish(self, index, data):
        #TODO4: fill in this function for your publish
        print('publish index:',index,'   length of data:',len(data))
        self.client.subscribeAsync("myTopic{index}_receive".format(index=index), 0, ackCallback=self.customSubackCallback)
        
        for i,num in enumerate(data):
            # print('the ith:',i)
            payload = {'index':index,'data':str(num)}
            self.client.publishAsync("myTopic{index}_send".format(index=index), json.dumps(payload), 0, ackCallback=self.customPubackCallback)



print("Loading vehicle data...")
data = []
for i in range(5):
    a = pd.read_csv(data_path.format(i))
    data.append(a)

print("Initializing MQTTClients...")
clients = []
for device_id in range(device_st, device_end):
    client = MQTTClient(device_id,certificate_formatter.format(device_id,device_id) ,key_formatter.format(device_id,device_id))
    client.client.connect()
    clients.append((device_id,client))
 

while True:
    print("send now?")
    x = input()
    if x == "s":
        for i,c in clients:
            data = pd.read_csv('vehicle{index}.csv'.format(index=i))
            co2 = data['vehicle_CO2'].tolist()
            # print('co2:',co2)
            c.publish(i,co2)

    elif x == "d":
        for c in clients:
            c.client.disconnect()
        print("All devices disconnected")
        exit()
    else:
        print("wrong key pressed")

    time.sleep(3)





