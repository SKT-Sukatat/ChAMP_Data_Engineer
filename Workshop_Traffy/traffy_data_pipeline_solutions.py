from pydantic import BaseModel
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowFailException

import pandas as pd
from datetime import datetime, timedelta
import requests
from io import StringIO

# Define Input Path
TRAFFY_RECORDS_API = "https://publicapi.traffy.in.th/dump-csv-chadchart/bangkok_traffy.csv"

# Define default arguments
default_args = {
    'owner':'Sukatat',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval':'0 8 * * *'
}


# Define the today date
today_date = datetime.now().strftime("%d_%m_%Y")

# Define a Pydantic model for your schema
class TraffySchema(BaseModel):
    ticket_id: object
    type: object
    organization: object
    comment: object
    photo: object
    photo_after: object
    coords: object
    address: object
    subdistrict: object
    district: object
    province: object
    timestamp: object
    state: object
    star: float
    count_reopen: int
    last_activity: object
            

@task()
def save_raw_data(output_path):
    df_traffy_raw = pd.read_csv(TRAFFY_RECORDS_API)
    # Declare file name of the raw data
    parquet_filename = "traffy_raw_data_" + today_date + ".parquet"
    # Export the DataFrame to a Parquet file
    df_traffy_raw.to_parquet(output_path + parquet_filename, engine="pyarrow", index=False)


@task()
def validate_columns(input_path):
    # Load the Parquet file (replace with your actual file path or file source)
    parquet_filename = "traffy_raw_data_" + today_date + ".parquet"
    df = pd.read_parquet(input_path + parquet_filename)

    # Get required columns from the Pydantic model
    required_columns = set(TraffySchema.__fields__.keys())
    file_columns = set(df.columns)

    # Check for missing or extra columns
    missing_columns = required_columns - file_columns

    if missing_columns:
        raise AirflowFailException(f"Missing columns: {missing_columns}")
    else:
        print("All required columns are present.")


@task()
def etl_traffy_data(input_path, output_path):
    df_traffy = pd.read_parquet(input_path + "/traffy_raw_data_" + today_date + ".parquet")
    
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
    df_traffy.to_parquet(output_path + 'cleaned_traffy_' + today_date +'.parquet')


@task()
def print_success():
    print("SUCCESS: The data is loaded as Parquet.")


@dag(default_args=default_args, start_date=days_ago(1), tags=['Traffy'])
def traffy_pipeline():
    # Create task
    save_raw_data_task = save_raw_data(output_path= '/opt/airflow/dags/traffy_data/')

    etl_traffy_data_task = etl_traffy_data(input_path= '/opt/airflow/dags/traffy_data/', output_path= '/opt/airflow/dags/traffy_data/')

    print_load_success = print_success()

    validate_columns_task = validate_columns(input_path= '/opt/airflow/dags/traffy_data/')

    # Crate Task Dependency (Create DAG)
    save_raw_data_task >> validate_columns_task >> etl_traffy_data_task >> print_load_success


traffy_pipeline()