#!/usr/bin/env python
# coding: utf-8

# In[1]:



from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTShadowClient
import random, time
from datetime import datetime
import pandas as pd
import json

# A random programmatic shadow client ID.
SHADOW_CLIENT = "myShadowClient"

# The unique hostname that &IoT; generated for 
# this device.
HOST_NAME = "a2y73ktw97olkq-ats.iot.us-east-2.amazonaws.com"

# The relative path to the correct root CA file for &IoT;, 
# which you have already saved onto this device.
ROOT_CA = "C:/Users/lenovo/Desktop/5111CA1/thing2/AmazonRootCA1.pem.txt"

# The relative path to your private key file that 
# &IoT; generated for this device, which you 
# have already saved onto this device.
PRIVATE_KEY = "C:/Users/lenovo/Desktop/5111CA1/thing2/664f0a4c81-private.pem.key"

# The relative path to your certificate file that 
# &IoT; generated for this device, which you 
# have already saved onto this device.
CERT_FILE = "C:/Users/lenovo/Desktop/5111CA1/thing2/664f0a4c81-certificate.pem.crt"

# A programmatic shadow handler name prefix.
SHADOW_HANDLER = "A0195020R2"

# Automatically called whenever the shadow is updated.
def myShadowUpdateCallback(payload, responseStatus, token):
  print()
  print('UPDATE: $aws/things/' + SHADOW_HANDLER + 
    '/shadow/update/#')
  print("payload = " + payload)
  print("responseStatus = " + responseStatus)
  print("token = " + token)

# Create, configure, and connect a shadow client.
myShadowClient = AWSIoTMQTTShadowClient(SHADOW_CLIENT)
myShadowClient.configureEndpoint(HOST_NAME, 8883)
myShadowClient.configureCredentials(ROOT_CA, PRIVATE_KEY,
  CERT_FILE)
myShadowClient.configureConnectDisconnectTimeout(10)
myShadowClient.configureMQTTOperationTimeout(5)
myShadowClient.connect()

# Create a programmatic representation of the shadow.
myDeviceShadow = myShadowClient.createShadowHandlerWithName(
  SHADOW_HANDLER, True)

data = pd.read_csv(r'C:/Users/lenovo/Desktop/5111CA1/train_FD002.txt', delim_whitespace=True, header=None)
sensor = ['id', 'te', 'os1', 'os2', 'os3'] + ['s'+ str(i) for i in range(1,22)]
data = pd.DataFrame(data.values, columns=sensor)
data['id'] = data['id'].map(lambda s: 'FD002_'+str(s))
for i in range(5000):
    dat = data.iloc[i]    
    tim = datetime.utcnow()    
    dat = dat.append(pd.Series(['A0195020R', str(tim)], index=['Matric number', 'timestamp']))    
    dat = dat.to_dict()                 
    jsonPayload = json.dumps(dat)    
    print(jsonPayload)    
    myDeviceShadow.shadowUpdate(str(jsonPayload),myShadowUpdateCallback, 5)
    time.sleep(0.5)

