from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


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
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2025, 1, 1),
    catchup=True,
    tags=['wiki','load'],
) as dag:
    
    load_data = BashOperator(
        task_id='load_data',
        bash_command="""
        ssh -i ~/.ssh/gcp_tom tom@34.64.87.127 '/home/tom/code/wikidata/pyspark/run.sh { ds } wiki_load.py'
        """
    )
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    start >> load_data >> end
