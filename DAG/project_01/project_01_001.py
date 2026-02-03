
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.decorators import dag, task
from kubernetes.client import models as k8s


default_executor_config = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="base",
                    resources=k8s.V1ResourceRequirements(
                        requests={"cpu": "100m", "memory": "128Mi"},
                        limits={"cpu": "300m", "memory": "256Mi"}
                    )
                )
            ]
        )
    )
} # end of default_executor_config

with DAG(dag_id="project_01_001",
         start_date=datetime(2025,3,27),
         schedule=none,
         tags=["project_01"],
         catchup=False
        ) as dag:

    @task(
        task_id="hello_world",
        executor_config=default_executor_config
    )
    def hello_world():
        print('Hello World - From Github Repository')




    hello_world_task = hello_world()


    hello_world_task 

             
