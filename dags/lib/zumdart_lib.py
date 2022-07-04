import os
import sys
import csv
import json
import pandas
import logging

def read(file):
    try:
        with open(file, 'r') as f:
            if file.endswith('json'):
                result = json.load(f)
            elif file.endswith('csv'):
                with open(file, newline='') as f:
                    result = list(csv.reader(f, delimiter=','))[0]
            else:
                result = f.read()
                #result = f.readlines()
        return result
    except FileNotFoundError as e:
        logging.error(e)
        return None

def write(file, obj):
    os.system(f'rm {file}')
    if file.endswith('json'):
        with open(file, 'w', encoding='utf-8') as f:
            json.dump(obj, f, ensure_ascii=False)
    elif file.endswith('csv'):
        with open(file, 'w') as f:
            csv.writer(f).writerow(obj)
    elif file.endswith('s3'):
        with open(file, 'w') as f:
            pandas.json_normalize(obj).to_parquet(file)
    else :
        with open(file, 'w') as f:
            f.write(obj)
