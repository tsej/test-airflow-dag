from datetime import datetime
from airflow import DAG
from airflow.providers.sas_ci360.operators import CI360TaskJobOperator, CI360SegmentMapJobOperator


with DAG(
  dag_id='ci360_sample_dag_3',
  start_date=datetime(2025, 1, 1),
  schedule=None,
  catchup=False,
  tags=["project_7", "SAS", "CI360"]
) as dag:


  segment_map = CI360SegmentMapJobOperator(conn_id='ci360_bct30', 
        task_id='MAP_26',  
        ci360_segment_map_id='173f10d9-86f6-4b2c-87af-0da543880b56')
  dm_task_one = CI360TaskJobOperator(conn_id='ci360_bct30', 
        task_id='TSK_121', 
        ci360_task_id='797c057c-b497-4f77-ac61-be9316f16ed8')
  dm_task_two = CI360TaskJobOperator(conn_id='ci360_bct30', 
        task_id='TSK_122', 
        ci360_task_id='c796af40-ef26-477d-8171-6c6394acfac7')

  segment_map >> [dm_task_one, dm_task_two ]
