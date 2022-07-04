import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

import sys
import csv
import json
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
    os.system('rm {}'.format(file))
    if file.endswith('json'):
        with open(file, 'w', encoding='utf-8') as f:
            json.dump(obj, f, ensure_ascii=False)
    elif file.endswith('csv'):
        with open(file, 'w') as f:
            csv.writer(f).writerow(obj)
    else :
        with open(file, 'w') as f:
            f.write(obj)

default_args = {
    "owner": "changwoolee",
    "email_on_failure": "changwoolee@zuminternet.com",
    "email_on_retry": "changwoolee@zuminternet.com",
    "email": "changwoolee@zuminternet.com",
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}

def _test():
    home = os.getcwd()+"/../"
    print(read(home+"conf/apis.json"))
    write(home+"conf/home_path.txt", home)
    print(read(home+"conf/home_path.txt"))

with DAG("test", start_date=datetime(2022, 6, 2),
    schedule_interval="00 * * * *", default_args=default_args, catchup=False) as dag:

    test = PythonOperator(
        task_id="test",
        python_callable=_test,
    )
