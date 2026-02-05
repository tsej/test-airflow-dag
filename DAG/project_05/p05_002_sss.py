from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.microsoft.azure.sensors.wasb import WasbBlobSensor

AZURE_CONN_ID = "azure_data_lake_default"
CONTAINER = "raw"
BLOB_NAME = "incoming/daily/report_{{ ds_nodash }}.csv"

@dag(dag_id="adls_wait_exact_blob", start_date=datetime(2026,1,1), schedule=None, catchup=False
    tags=["project_05", "adls", "azure", "sensor"])
def adls_wait_exact_blob():
    wait = WasbBlobSensor(
        task_id="wait_exact_blob",
        wasb_conn_id=AZURE_CONN_ID,
        container_name=CONTAINER,
        blob_name=BLOB_NAME,
        deferrable=True,
        mode="reschedule",
        poke_interval=30,
        timeout=6*60*60,
    )

    @task
    def print_name():
        print(f"Arrived file: {BLOB_NAME}")

    wait >> print_name()

adls_wait_exact_blob()
