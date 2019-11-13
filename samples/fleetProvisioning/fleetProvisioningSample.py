'''
/*
 * Copyright 2010-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
 '''

from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

import threading
import logging
import time
import datetime
import argparse
import json
createKeysAndCertificateRequestTopic = '$aws/certificates/create/json'
createKeysAndCertificateAcceptedTopic = '$aws/certificates/create/json/accepted'
createKeysAndCertificateRejectedTopic = '$aws/certificates/create/json/rejected'

registerThingRequestTopicFormat = '$aws/provisioning-templates/{}/provision/json'
registerThingAcceptedTopicFormat = '$aws/provisioning-templates/{}/provision/json/accepted'
registerThingRejectedTopicFormat = '$aws/provisioning-templates/{}/provision/json/rejected'

class FleetProvisioningProcessor(object):
    def __init__(self, awsIoTMQTTClient, templateName, clientToken):
        self.clientToken = clientToken
        self.templateName = templateName
        self.awsIoTMQTTClient = awsIoTMQTTClient
        self._setupCallbacks(self.awsIoTMQTTClient)
        self.createKeysAndCertificateResponse = None
        self.registerThingResponse = None
        self.failureResponse = None
        self.done = False

    def _setupCallbacks(self, awsIoTMQTTClient):
        print('Subscribing to topic %s' % (createKeysAndCertificateAcceptedTopic))
        myAWSIoTMQTTClient.subscribe(createKeysAndCertificateAcceptedTopic, 1, self.createKeysAndCertificateCallback)
        print('Subscribing to topic %s' % (createKeysAndCertificateRejectedTopic))
        myAWSIoTMQTTClient.subscribe(createKeysAndCertificateRejectedTopic, 1, self.failureCallback)
        registerThingAcceptedTopic = registerThingAcceptedTopicFormat.format(self.templateName)
        print('Subscribing to topic %s' % (registerThingAcceptedTopic))
        myAWSIoTMQTTClient.subscribe(registerThingAcceptedTopic, 1, self.registerThingCallback)
        registerThingRejectedTopic = registerThingRejectedTopicFormat.format(self.templateName)
        print('Subscribing to topic %s' % (registerThingRejectedTopic))
        myAWSIoTMQTTClient.subscribe(registerThingRejectedTopic, 1, self.failureCallback)

    def failureCallback(self, client, userdata, message):
        print("Received error from topic: " + message.topic)
        print(message.payload)
        self.failureResponse = json.loads(message.payload.decode('utf-8'))

    def _waitForCreateKeysAndCertificateResponse(self):
        # Wait for the response.
        loopCount = 0
        while loopCount < 10 and self.createKeysAndCertificateResponse is None:
            if self.createKeysAndCertificateResponse is not None:
                break
            print('Waiting... CreateKeysAndCertificateResponse: ' + json.dumps(self.createKeysAndCertificateResponse))
            loopCount += 1
            time.sleep(1)

    def callCreateKeysAndCertificate(self):
        createKeysAndCertificateRequest = {}
        createKeysAndCertificateRequestJson = json.dumps(createKeysAndCertificateRequest)
        self.awsIoTMQTTClient.publish(createKeysAndCertificateRequestTopic, createKeysAndCertificateRequestJson, 1)
        print('Published topic %s: %s\n' % (createKeysAndCertificateRequestTopic, createKeysAndCertificateRequestJson))
        self._waitForCreateKeysAndCertificateResponse()

    def createKeysAndCertificateCallback(self, client, userdata, message):
        print("Received a new message from topic: " + message.topic)
        print(message.payload)
        self.createKeysAndCertificateResponse = json.loads(message.payload.decode('utf-8'))

    def _waitForRegisterThingResponse(self):
        # Wait for the response.
        loopCount = 0
        while loopCount < 10 and self.registerThingResponse is None:
            if self.registerThingResponse is not None:
                break
            loopCount += 1
            print('Waiting... RegisterThingResponse: ' + json.dumps(self.registerThingResponse))
            time.sleep(1)

    def callRegisterThing(self):
        registerThingRequest = {}
        registerThingRequest['certificateId'] = self.createKeysAndCertificateResponse['certificateId']
        ######
        # Add parameters of your own 
        # TODO send it through args.
        registerThingRequest['deviceContext'] = {
            'DeviceLocation': 'Seattle'
        }
        ######
        registerThingRequestTopic = registerThingRequestTopicFormat.format(self.templateName)
        registerThingRequestJson = json.dumps(registerThingRequest)
        self.awsIoTMQTTClient.publish(registerThingRequestTopic, registerThingRequestJson, 1)
        print('Published topic %s: %s\n' % (registerThingRequestTopic, registerThingRequestJson))
        self._waitForRegisterThingResponse()

    def registerThingCallback(self, client, userdata, message):
        print("Received a new message from topic: " + message.topic)
        print(message.payload)
        self.registerThingResponse = json.loads(message.payload.decode('utf-8'))
        self.done = True

    def isDone(self):
        return self.done


# Read in command-line parameters
parser = argparse.ArgumentParser()
parser.add_argument("-e", "--endpoint", action="store", required=True, dest="host", help="Your AWS IoT custom endpoint")
parser.add_argument("-r", "--rootCA", action="store", required=True, dest="rootCAPath", help="Root CA file path")
parser.add_argument("-c", "--cert", action="store", dest="certificatePath", required=True, help="Certificate file path")
parser.add_argument("-k", "--key", action="store", dest="privateKeyPath", required=True, help="Private key file path")
parser.add_argument("-t", "--templateName", action="store", required=True, dest="templateName", help="Template name")
parser.add_argument("-id", "--clientId", action="store", dest="clientId", default="Fleet Provisioning Python Sample",
                    help="Targeted client id")

args = parser.parse_args()
host = args.host
rootCAPath = args.rootCAPath
certificatePath = args.certificatePath
privateKeyPath = args.privateKeyPath
clientId = args.clientId
templateName = args.templateName
port = 8883

# Configure logging
logger = logging.getLogger("AWSIoTPythonSDK.core")
logger.setLevel(logging.INFO)
streamHandler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)


# Init AWSIoTMQTTClient
myAWSIoTMQTTClient = None
myAWSIoTMQTTClient = AWSIoTMQTTClient(clientId)
myAWSIoTMQTTClient.configureEndpoint(host, port)
myAWSIoTMQTTClient.configureCredentials(rootCAPath, privateKeyPath, certificatePath)

# AWSIoTMQTTClient connection configuration
myAWSIoTMQTTClient.configureAutoReconnectBackoffTime(1, 32, 20)
myAWSIoTMQTTClient.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
myAWSIoTMQTTClient.configureDrainingFrequency(2)  # Draining: 2 Hz
myAWSIoTMQTTClient.configureConnectDisconnectTimeout(10)  # 10 sec
myAWSIoTMQTTClient.configureMQTTOperationTimeout(5)  # 5 sec

myAWSIoTMQTTClient.connect()
foundryProc = FleetProvisioningProcessor(myAWSIoTMQTTClient, templateName, clientId)
print('Starting provisioning...')
foundryProc.callCreateKeysAndCertificate()
foundryProc.callRegisterThing()
if not foundryProc.isDone():
    print('Did not get finished')
else:
    print('Done')

myAWSIoTMQTTClient.disconnect()