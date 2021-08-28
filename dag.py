import pathlib
from airflow import models
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

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

# setting path to gcs/data folder in gcs environment
current_path = pathlib.Path(__file__).absolute()
config_dir_path = current_path.parent.parent.joinpath("data/food_orders")

# setting path to each file in gcs environment
config_file_path = config_dir_path.joinpath('config.json')
data_pipelining_file_path = config_dir_path.joinpath('data_pipelining.py')
input_file_path = config_dir_path.joinpath('src/food_daily.csv')

with models.DAG(
    'food_orders_dag_2',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    t1 = BashOperator(
        task_id='beam_task',
        bash_command='python {main_script} --config {config_file} --input {input_file}'.format(
            main_script=data_pipelining_file_path, 
            config_file=config_file_path,
            input_file=input_file_path
        )
    )