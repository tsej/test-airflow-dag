"""
Code that goes along with the Airflow tutorial.
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
}

with DAG(
    dag_id='airflow_tutorial_v01',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=None, # Set to None for manual triggers, or a cron expression like '@daily'
    tags=['example'],
) as dag:

    # Task 1: Print the execution date
    t1 = BashOperator(
        task_id='print_date',
        bash_command='echo {{ ds }}',
    )

    # Task 2: Sleep for 5 seconds and print 'Done'
    t2 = BashOperator(
        task_id='sleep',
        bash_command='sleep 5 && echo "Done"',
    )

    # Task 3: Create a templated command that prints the dag run's logical date
    templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, i) }}"
    {% endfor %}
    """

    t3 = BashOperator(
        task_id='templated',
        bash_command=templated_command,
    )

    # Set task dependencies
    t1 >> [t2, t3] # t1 runs first, then t2 and t3 run in parallel

