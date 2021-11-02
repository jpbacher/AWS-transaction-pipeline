from __future__ import print_function

import base64
import json
import boto3
from datetime import datetime


time_now = datetime.now()
time_str = time_now.strftime('%Y/%m/%d, %H/%M/%S')

s3 = boto3.client('s3')


def lambda_handler(event, context):
    trans_records = []
    for record in event['Records']:
        # decode kinesis data
        decoded_record = base64.b64decode(record["kinesis"]["data"])
        trans_records.append(decoded_record)
    
    event_count = len(trans_records)
    # convert list to string
    trans_records_str = '/n'.join(map(str, trans_records))
    # file with a timestamp
    timestamp_file = f'{time_str}-transaction.txt'
    # send file to s3
    response = s3.put_object(Body=trans_records_str, Bucket='s3-transactions', Key=timestamp_file)
    
    return f'Successfully placed {len(event_count)} records into s3' 
