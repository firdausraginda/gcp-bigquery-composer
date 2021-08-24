from airflow import models
from datetime import datetime, timedelta
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 8, 24),
    'retries': 1,
    'retry_delay': timedelta(seconds=50),
    'dataflow_default_options': {
        'project': 'try-dummy-project',
        'region': 'us-central1',
        'runner': 'DataflowRunner'
    }
}

with models.DAG(
    'food_orders_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    t1 = DataFlowPythonOperator(
        task_id='beam_task',
        py_file='gs://us-central1-demo-food-order-6772f774-bucket/gcp-dataflow-bigquery/data_pipelining.py',
        options={
            'input': 'gs://us-central1-demo-food-order-6772f774-bucket/gcp-dataflow-bigquery/src/food_daily.csv'}
    )
