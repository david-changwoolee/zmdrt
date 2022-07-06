import os
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "changwoolee",
    "email_on_failure": "changwoolee@zuminternet.com",
    "email_on_retry": "changwoolee@zuminternet.com",
    "email": "changwoolee@zuminternet.com",
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}

with DAG("kube", start_date=datetime(2022, 6, 2),
    schedule_interval="00 * * * *", default_args=default_args, catchup=False) as dag:

    kube = KubernetesPodOperatorkube(
        task_id="kube",
        image="centos:latest",
        cmds=["echo 'hello world!'"],
        namespace="airflow",
        name="kube",
        in_cluster=False,
        is_delete_operator_pod=True
    )
