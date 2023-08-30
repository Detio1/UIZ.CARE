from airflow import DAG
#from airflow.decorators import task
from datetime import timedelta, datetime
from EL import *

default_args = {
    'owner' : 'airflow',
    'depends_on_past': False,
    'email_failure': False,
    'retries': 1,
    'retry_delay':  timedelta(minutes = 5)
}

with DAG(
    'task_1',
    default_args = default_args,
    start_date = datetime(2023, 8, 30),
    schedule_interval = '@daily'
)as dag:
    
    #@task()
    def execute_extract_load():
        data1, data2 = load_data()
        df = data1_preprocessing(data1)
        df = combining_the_two_datasets(df, data2)
        symptoms_table_creation_and_saving(df)
        wearable_table_creation(df)
        emotional_table_creation_saving(df)
        GenZ_table_creation_saving(df)
        GP_table_creation_saving(df)

    execute_extract_load()      