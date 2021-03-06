import os 
import csv
import json
import requests


PROJ_ROOT = os.path.join(os.pardir)
data_path = os.path.join(PROJ_ROOT, 'data', 'train.csv')
config_file = os.path.join(PROJ_ROOT, 'aws.cfg')


def convert_json_to_api(filepath, columns, url, header_row=True):
    """Converts csv file to json document and sends to API.

    Args:
        filepath (string): file path to the csv
        columns (list): list of column names
        url (string): url to API endpoint
        header_row (boolean): whether csv file has a header row
    """
    with open(filepath, "r") as csvfile:
        reader = csv.DictReader(csvfile, fieldnames=columns)
        # skip first row of column names
        if header_row:
            next(reader)
        for i, row in enumerate(reader):
            if i > 10:
                break
            # convert columns to correct data types
            doc = {
                "record_no": int(row["record_no"]),
                "trans_date_trans_time": row["trans_date_trans_time"],
                "cc_num": int(row["cc_num"]),
                "merchant": row["merchant"],
                "category": row["category"],
                "amt": float(row["amt"]),
                "first": row["first"],
                "last": row["last"],
                "gender": row["gender"],
                "street": row["street"],
                "city": row["city"],
                "state": row["state"],
                "zip": int(row["zip"]),
                "lat": float(row["lat"]),
                "long": float(row["long"]),
                "city_pop": int(row["city_pop"]),
                "job": row["job"],
                "dob": row["dob"],
                "trans_num": row["trans_num"],
                "unix_time": int(row["unix_time"]),
                "merch_lat": float(row["merch_lat"]),
                "merch_long": float(row["merch_long"]),
                "is_fraud": int(row["is_fraud"])
            }
            # convert to json document
            json_doc = json.dumps(doc) 
            # send to API
            response = requests.post(url, data=json_doc)
            print(response)
            
            
def main():
    
    columns = ["record_no",
               "trans_date_trans_time",
               "cc_num",
               "merchant",
               "category",
               "amt",
               "first",
               "last",
               "gender",
               "street",
               "city",
               "state",
               "zip",
               "lat",
               "long",
               "city_pop",
               "job",
               "dob",
               "trans_num",
               "unix_time",
               "merch_lat",
               "merch_long",
               "is_fraud"]
    url_api = 'https://0jjopxnuia.execute-api.us-east-2.amazonaws.com/prod/transactions'
    
    convert_json_to_api(filepath=data_path, columns=columns, url=url_api, header_row=True)
    
    
if __name__ == '__main__':
    main()
