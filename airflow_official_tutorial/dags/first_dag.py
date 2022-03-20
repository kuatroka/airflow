try:

    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime, timedelta
    import pandas as pd
    import requests

    print("All DAGs modules are ok.........")
except Exception as e:
    print("Error  {}".format(e))


def first_function_execute(**context):
    print("First function is executed")
    context["ti"].xcom_push(key="mykey", value="first_function_execute says hello")


def second_function_execute(**context):
    instance = context["ti"].xcom_pull(key="mykey")
    print(f"I'm the second function and I got a value {instance} form 1st function ")


# def first_function_execute(*args, **kwargs):
#     variable = kwargs.get("name", "Did not get the key")
#     print(f"Hello World from the second function {variable}")
#     return "Hello World from the second function" + variable


with DAG(
    dag_id="first_dag",
    schedule_interval="*/1 * * * *",
    # schedule_interval="@daily",
    catchup=False,
    default_args={
        "owner": "airflow",
        "start_date": datetime(2022, 1, 1),
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
) as f:
    first_function_execute = PythonOperator(
        task_id="first_function_execute",
        python_callable=first_function_execute,
        provide_context=True,
        op_kwargs={"name": "Say my name is Pedro"},
    )

    second_function_execute = PythonOperator(
        task_id="second_function_execute",
        provide_context=True,
        python_callable=second_function_execute,
    )

first_function_execute >> second_function_execute
