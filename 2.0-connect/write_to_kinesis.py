import json
import boto3


def lambda_handler(event, context):
    
    trans_record = event['body-json']
    trans_json = json.dumps(trans_record)
    
    client = boto3.client('kinesis')
    response = client.put_record(
        StreamName='cc-trans-kinesis-stream',
        Data=trans_json,
        PartitionKey='string'
    )
        
    return {
        'statusCode': 200,
        'body': trans_json
    }
