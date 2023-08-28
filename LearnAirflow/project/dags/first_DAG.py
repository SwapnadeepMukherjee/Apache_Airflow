try:

    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    import pandas as pd

    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))


# def first_function_execute(*args, **kwargs):
#     variable = kwargs.get("name", "Didn't get the key")
#     print("Hello World: {}".format(variable))
#     return "Hello World"

def first_function_execute(**context):
    print("first_function_execute  ")
    context['ti'].xcom_push(key='mykey', values="first_function_execute says Hello")


def second_function_execute(**context):
    instance = context.get("ti").xcom_pull(key="mykey")
    print("I am in second_function_execute got value: {} from first_function_execute".format(instance))
    # return "Hello World"


# */2 * * * * - Execute every two minutes

with DAG(
        dag_id='first_DAG',
        schedule_interval="@daily",
        default_args={
            "owner": "airflow",
            "retires": 3,
            "retry_daley": timedelta(minutes=5),
            "start_date": datetime(2023, 8, 28),
        },
        catchup=False) as f:
    first_function_execute = PythonOperator(
        task_id='first_function_execute',
        python_callable=first_function_execute,
        provide_context=True,
        op_kwargs={"name": "Swapnadeep Mukherjee"}
    )

    second_function_execute = PythonOperator(
        task_id="second_function_execute",
        python_callable=second_function_execute,
        provide_context=True,
        op_kwargs={"name": "Swapnadeep Mukherjee"}
    )

    first_function_execute >> second_function_execute