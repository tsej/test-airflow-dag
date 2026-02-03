from __future__ import annotations

import pendulum

from airflow.decorators import dag, task

@dag(
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule_interval=None,
    catchup=False,
    doc_md="""
    ## Dynamic Task Mapping Example
    This DAG dynamically creates a number of 'process_item' tasks based on the list returned by the 'prepare_list' task.
    """
)
def dynamic_array_processing():

    @task
    def prepare_list() -> list[str]:
        """
        Generates the input array (list of strings) at runtime.
        This data is automatically pushed to XCom.
        """
        return ["apple", "banana", "cherry", "date", "elderberry"]

    @task
    def process_item(item: str):
        """
        Processes a single item from the input array.
        Each item results in a separate, parallel task instance.
        """
        print(f"Processing the item: {item}")
        # Your processing logic here
        return f"processed_{item}"

    @task
    def collect_results(processed_items: list[str]):
        """
        A 'reduce' step that collects the output of all mapped tasks.
        """
        print(f"All items processed. Results: {processed_items}")

    # Define dependencies and mapping
    items_to_process = prepare_list()
    # The .expand() method dynamically maps over the output of prepare_list
    processed_items_list = process_item.expand(item=items_to_process)
    # The downstream task automatically collects all results from the mapped tasks
    collect_results(processed_items_list)

# Register the DAG
dynamic_array_processing()
