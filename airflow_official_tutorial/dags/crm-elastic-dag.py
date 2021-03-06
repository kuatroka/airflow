import sys

sys.path.append(r"C:/Users/yo_fanpc/Documents/dev/airflow/airflow_official_tutorial")
from scripts.test import hello

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


# https://www.youtube.com/watch?v=IsWfoXY_Duk

args = {
    "owner": "Shminsent Shmivenson",
    "start_date": days_ago(1),  # make start date in the past
}

dag = DAG(
    dag_id="crm-elastic-dag",
    default_args=args,
    schedule_interval="@daily",  # make this workflow happen every day
)


# def hello():
#     print("Hello!")


with dag:
    hello_world = PythonOperator(
        task_id="hello",
        python_callable=hello,
        # provide_context=True
    )
