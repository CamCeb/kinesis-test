import boto3
import json
import time

#setting up the kinesis client
kinesis_client = boto3.client('kinesis', region_name='us-east-1')

def send_to_kinesis(latitude, longitude, stream_name):
    
    data = { # creating json data structure to send to Kinesis datastream
        'latitude': latitude,
        'longitude': longitude
    }
    
    response = kinesis_client.put_record(
        StreamName=stream_name,
        Data=json.dumps(data),  #serializing json to send to kinesis
        PartitionKey='1'  # Partition key name does not only using one shard. 
    )
    print("Record sent:", response) # verifying data is sent with no errors

#test data
stream_name = "test-stream"
lat = 2.00000
long = 50.00000


for i in range(10):
    time.sleep(1)
    lat += 0.0001
    long += 0.0001
    lat_str = str(lat)
    long_str = str(long)
    send_to_kinesis(lat_str, long_str, stream_name)