from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

@dag(start_date=datetime(2025, 3, 6), schedule=None, tags=["project_02"])
def data_cleaning_dag():

    @task()
    def extract_data():
        # Code to extract data
        return "Raw data extracted"

    @task()
    def clean_data():
        # Code to clean the extracted data
        return "Data cleaned"

    @task()
    def trigger_next_dag():
        # Trigger DAG 2 (Data Processing)
        trigger = TriggerDagRunOperator(
            task_id='trigger_data_processing',
            trigger_dag_id='data_processing_dag',
            conf={"clean_data": "processed"}
        )
        return trigger

    extract = extract_data()
    clean = clean_data()
    trigger = trigger_next_dag()

    extract >> clean >> trigger

data_cleaning_instance = data_cleaning_dag()