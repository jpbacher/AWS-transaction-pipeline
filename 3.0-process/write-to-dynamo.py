import base64
import json
import boto3


def put_transaction(cc_num,
                    trans_date_trans_time,
                    trans_num,
                    amt,
                    merchant,
                    is_fraud,
                    first,
                    last,
                    gender,
                    dob,
                    job,
                    street,
                    city,
                    state,
                    zip,
                    lat,
                    long,
                    city_pop,
                    category,
                    merch_lat,
                    merch_long):
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('cc_transactions')

    response = table.put_item(
        Item={
            'cc_num': cc_num,
            'trans_date_trans_time': trans_date_trans_time,
            'trans_num': trans_num,
            'amt': amt,
            'merchant': merchant,
            'is_fraud': is_fraud,
            'customer_info': {
                'first': first,
                'last': last,
                'gender': gender,
                'dob': dob,
                'job': job,
                'street': street,
                'city': city,
                'state': state,
                'zip': zip,
                'lat': lat,
                'long': long,
                'city_pop': city_pop
            },
            'merchant_info': {
                'category': category,
                'merch_lat': merch_lat,
                'merch_long': merch_long
            }
        }
    )
    return response
            

def lambda_handler(event, context):
    
    for record in event['Records']:
        # decode kinesis data
        decoded_record = base64.b64decode(record["kinesis"]["data"])
        # decode bytes into a str
        str_record = str(decoded_record, 'utf-8')
        # transform string into a dict
        deserialized_record = json.loads(str_record)
        
        trans_response = put_transaction(
            deserialized_record['cc_num'],
            deserialized_record['trans_date_trans_time'],
            deserialized_record['trans_num'],
            deserialized_record['amt'],
            deserialized_record['merchant'],
            deserialized_record['is_fraud'],
            deserialized_record['first'],
            deserialized_record['last'],
            deserialized_record['gender'],
            deserialized_record['dob'],
            deserialized_record['job'],
            deserialized_record['street'],
            deserialized_record['city'],
            deserialized_record['state'],
            deserialized_record['zip'],
            deserialized_record['lat'],
            deserialized_record['long'],
            deserialized_record['city_pop'],
            deserialized_record['category'],
            deserialized_record['merch_lat'],
            deserialized_record['merch_long']
        )
        print(trans_response)
