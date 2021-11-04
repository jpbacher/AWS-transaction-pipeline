import logging
import boto3
from botocore.exceptions import ClientError
import os
import configparser
from datetime import datetime



PROJ_ROOT = os.path.join(os.pardir)
data_path = os.path.join(PROJ_ROOT, 'data', 'train.csv')
aws_path = os.path.join(PROJ_ROOT, 'aws.cfg')

date_now = datetime.now()
time_stamp_str = date_now.strftime("%Y/%m/%d-%H:%M:%S")

def upload_file(access_key, secret_key, file_path, bucket, s3_obj_name):
    """[summary]

    Args:
        access_key: AWS access key
        secret_key: AWS secret access key
        file_path (string]): file path to file on local computer
        bucket (string): bucket name created on AWS console
        s3_obj_name (string): name of file appearing on AWS 

    Returns:
        [type]: [description]
    """
    
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    s3 = session.resource('s3')
    try: 
        response = s3.Bucket(bucket).upload_file(file_path, s3_obj_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


if __name__ == '__main__':
    
    config = configparser.ConfigParser()
    config.read_file(open(aws_path))
    
    access_key = config.get('S3', 'KEY')
    secret_key = config.get('S3', 'SECRET')
    
    bucket_name = 's3cctransactions'
    obj_name = f'transactions-{time_stamp_str}.csv'
    
    upload_file(access_key, secret_key, data_path, bucket_name, obj_name)
