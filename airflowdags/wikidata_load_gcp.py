from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook


with DAG(
    'wikidata_load',
    default_args={
        'depends_on_past': True,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=1,
    description='movie',
    schedule="10 10 * * *",
    start_date=datetime(2024, 9, 10),
    end_date=datetime(2025, 1, 1),
    catchup=True,
    tags=['wiki','load'],
) as dag:
    
    # load_data = BashOperator(
    #     task_id='load_data',
    #     bash_command="""
    #     ssh -i ~/.ssh/gcp_tom tom@34.64.87.127 '/home/tom/code/wikidata/pyspark/run.sh {{ ds }} /home/tom/code/wikidata/pyspark/wiki_load.py'
    #     """
    # )
    
    GCE_INSTANCE = 'spark-tom-1'
    GCE_ZONE = 'asia-northeast3-c'
    GCP_PROJECT_ID = 'praxis-bond-455400-a4'
    
    load_data = SSHOperator(
      task_id='composer_compute_ssh_task',
      ssh_hook=ComputeEngineSSHHook(
          instance_name=GCE_INSTANCE,
          zone=GCE_ZONE,
          project_id=GCP_PROJECT_ID,
          use_oslogin=False,
          use_iap_tunnel=False,
          use_internal_ip=True),
      cmd_timeout=600,
      command='sudo -u tom /home/tom/code/wikidata/pyspark/run.sh {{ ds }} /home/tom/code/wikidata/pyspark/wiki_load.py'
    )
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    start >> load_data >> end
