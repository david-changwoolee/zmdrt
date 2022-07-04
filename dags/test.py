import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from lib.zumdart_lib import read, write

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
