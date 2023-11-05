import json
import logging
import sys

import greengrasssdk

# Logging
logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

# SDK Client
client = greengrasssdk.client("iot-data")

# Counter
my_counter = 0
max_co2 = [0 for _ in range(5)]
def lambda_handler(event, context):
    global my_counter
    #TODO1: Get your data
    print('event:',event)
    index = int(event['index'])
    data = float(event['data'])
    
    
    #TODO2: Calculate max CO2 emission
    max_co2[index] = max(data,max_co2[index])

    #TODO3: Return the result
    client.publish(
        # topic="hello/world/counter",
        topic = "myTopic{index}_receive".format(index=index),
        payload=json.dumps(
            {
                "message": "Hello world! Sent from Greengrass Core.  Invocation Count: {}".format(my_counter),
                "data": max_co2[index]
            }
        ),
    )
    my_counter += 1

    return