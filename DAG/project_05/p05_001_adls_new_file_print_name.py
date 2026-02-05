# dags/adls_new_file_print_name.py
from __future__ import annotations
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.microsoft.azure.sensors.wasb import WasbPrefixSensor

# ---- Configure these for your environment ----
AZURE_CONN_ID = "azure_data_lake_default"  # Airflow connection to ADLS Gen2 / Blob
CONTAINER = "raw"                          # e.g., your container (file system)
PREFIX = "incoming/"                       # e.g., the folder/prefix to watch
POKE_INTERVAL = 30                         # seconds between checks
TIMEOUT_SEC = 12 * 60 * 60                 # 12h safety timeout
# ---------------------------------------------

@dag(
    dag_id="adls_new_file_print_name",
    start_date=datetime(2026, 1, 1),
    schedule=None,             # trigger manually, sensor waits until a file arrives
    catchup=False,
    tags=["project_05", "adls", "azure", "sensor"]
)
def adls_new_file_print_name():

    # Wait until at least one blob exists under the prefix
    wait_for_new_blob = WasbPrefixSensor(
        task_id="wait_for_new_blob",
        wasb_conn_id=AZURE_CONN_ID,
        container_name=CONTAINER,
        prefix=PREFIX,
        poke_interval=POKE_INTERVAL,
        timeout=TIMEOUT_SEC,
        mode="reschedule",     # free worker slots between pokes
        deferrable=True,       # use triggerer for true async waiting (if available)
    )

    @task(task_id="print_new_file_name")
    def print_newest_blob_name() -> str:
        """
        Lists blobs under the prefix and prints the most recently modified one.
        Returns the blob name (also visible in XCom).
        """
        from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

        hook = WasbHook(wasb_conn_id=AZURE_CONN_ID)
        blob_service = hook.get_conn()  # azure.storage.blob.BlobServiceClient
        container_client = blob_service.get_container_client(CONTAINER)

        # Iterate and find the latest by last_modified
        latest = None
        for blob in container_client.list_blobs(name_starts_with=PREFIX):
            if latest is None or blob.last_modified > latest.last_modified:
                latest = blob

        if not latest:
            raise RuntimeError("Sensor succeeded but no blobs were listed under the prefix.")

        print(f"New file detected: {latest.name}")
        return latest.name

    wait_for_new_blob >> print_newest_blob_name()

adls_new_file_print_name()
