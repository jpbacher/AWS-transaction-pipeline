import csv
import json
import os 
import requests


PROJ_ROOT = os.path.join(os.pardir)
data_path = os.path.join(PROJ_ROOT, 'data', 'train.csv')


def convert_json_to_api(filepath, columns, url):
    
    with open(filepath, "r") as csvfile:
        reader = csv.DictReader(csvfile, fieldnames=columns)
        # skip first row of column names
        next(reader)
        for row in reader:
            # convert row to json object
            json_doc = json.dumps(row, indent=2)
            # send to API
            response = requests.post(url, data=json_doc)
            
            
def main():
    
    columns = ["record",
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
    url = ""
    convert_json_to_api(filepath=data_path, columns=columns, url=url)
    
    
if __name__ == '__main__':
    main()
