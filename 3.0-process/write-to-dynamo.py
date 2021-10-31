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
    
    # decode kinesis data
    #decoded_data = [base64.b64decode(record["kineses"]["data"]) for record in event["Records"]]
    #deserialized_data = [json.loads(decoded_record) for decoded_record in decoded_data]

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
        trans_response = put_transaction(
            record['cc_num'],
            record['trans_date_trans_time'],
            record['trans_num'],
            record['amt'],
            record['merchant'],
            record['is_fraud'],
            record['first'],
            record['last'],
            record['gender'],
            record['dob'],
            record['job'],
            record['street'],
            record['city'],
            record['state'],
            record['zip'],
            record['lat'],
            record['long'],
            record['city_pop'],
            record['category'],
            record['merch_lat'],
            record['merch_long']
        )
        print(trans_response)
