from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

import pandas as pd
from datetime import datetime, timedelta


# Define Input Path
TRAFFY_RECORDS_API = "https://publicapi.traffy.in.th/dump-csv-chadchart/bangkok_traffy.csv"

default_args = {
    'owner':'Sukatat',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval':'0 8 * * *'
}


@task()
def etl_traffy_data(output_path):
    df_traffy = pd.read_csv(TRAFFY_RECORDS_API)
    
    # Requirement 1: Fitler only Bangkok
    df_traffy = df_traffy[(df_traffy['province'] == 'กรุงเทพมหานคร') | (df_traffy['province'] == 'จังหวัดกรุงเทพมหานคร')]
    df_traffy['province'] = df_traffy['province'].replace('จังหวัดกรุงเทพมหานคร', 'กรุงเทพมหานคร')
    
    # Requirement 2: Fitler only State of Cases
    # Define the allowed states
    allowed_states = ["finish", "inprogress", "forward", "follow", "irrelevant", "start"]

    # filter rows with valid states only
    df_traffy = df_traffy[df_traffy["state"].isin(allowed_states)]

    # Requirement 3: Sorted data by timestamp
    df_traffy = df_traffy.sort_values(by='timestamp', ascending = False)

    # Load Data as Parquet File to Storage
    df_traffy.to_parquet(output_path + 'cleaned_traffy.parquet')


@task()
def print_success():
    print("SUCCESS: The data is loaded as Parquet.")


@dag(default_args=default_args, start_date=days_ago(1), tags=['Traffy'])
def traffy_pipeline():
    # Create task
    etl_traffy_data_task = etl_traffy_data(output_path= '/opt/airflow/dags/')

    print_load_success = print_success()

    # Crate Task Dependency (Create DAG)
    etl_traffy_data_task >> print_load_success


traffy_pipeline()