from datetime import datetime, timedelta
from airflow import DAG, Dataset
from airflow.sensors.filesystem import FileSensor
from sas_airflow_provider.operators.sas_studio import SASStudioOperator
from sas_airflow_provider.operators.sas_jobexecution import SASJobExecutionOperator
from sas_airflow_provider.operators.sas_create_session import SASComputeCreateSession
from sas_airflow_provider.operators.sas_delete_session import SASComputeDeleteSession

dag = DAG(dag_id="airflowStudio01",
   schedule=None,
   start_date=datetime(2026,2,4),
   tags=["project_05", "sas"],
   catchup=False)

task1_code = '''
%put OK 01 ;
'''

task1 = SASStudioOperator(task_id="task-01",
   exec_type="program",
   path_type="raw",
   path=task1_code,
   compute_context="SAS Studio compute context",
   connection_name="sas_default",
   exec_log=True,
   codegen_init_code=False,
   codegen_wrap_code=False,
   trigger_rule='all_success',
   dag=dag)

task2_code = '''
%put OK 2 ;
'''

task2 = SASStudioOperator(task_id="task-02",
   exec_type="program",
   path_type="raw",
   path=task2_code,
   compute_context="SAS Studio compute context",
   connection_name="sas_default",
   exec_log=True,
   codegen_init_code=False,
   codegen_wrap_code=False,
   trigger_rule='all_success',
   dag=dag)

task1 >> task2
