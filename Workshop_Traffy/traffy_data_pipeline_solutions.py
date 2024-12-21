from pydantic import BaseModel
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowFailException

import pandas as pd
from datetime import datetime, timedelta
import os


# Define Input Path
TRAFFY_RECORDS_API = "https://storage.googleapis.com/traffy-de-workshop/bangkok_traffy.parquet"

# Define default arguments
default_args = {
    'owner':'Sukatat',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval':'0 8 * * *'
}


# Define the today date
today_date = datetime.now().strftime("%d_%m_%Y")


@task()
def create_folder_if_not_exists(folder_path):
    """Creates a folder if it doesn't already exist.

    Args:
        folder_path: The path to the folder to create.
    """
    if not os.path.exists(folder_path):
        try:
            os.makedirs(folder_path)
            print(f"Created folder: {folder_path}")
        except OSError as e:
            print(f"Error creating folder: {folder_path}. {e}")
    else:
        print(f"Folder already exists: {folder_path}")


@task()
def save_raw_data(output_path):
    df_traffy_raw = pd.read_parquet(TRAFFY_RECORDS_API)
    # Declare file name of the raw data
    parquet_filename = "traffy_raw_" + today_date + ".parquet"
    # Export the DataFrame to a Parquet file
    df_traffy_raw.to_parquet(output_path + parquet_filename, compression="zstd", index=False)


@task()
def check_schema(input_path):
    # Extract Traffy Data from API
    df_traffy_raw = pd.read_parquet(input_path)

    # Check if the DataFrame has exactly the same columns
    actual_columns = set(df_traffy_raw.columns)

    # Define all required columns as a set
    required_columns_set = {'ticket_id', 'type', 'organization', 'comment', 'photo', 'photo_after',
                            'coords', 'address', 'subdistrict', 'district', 'province', 'timestamp',
                            'state', 'star', 'count_reopen', 'last_activity'}

    if actual_columns == required_columns_set:
        print("The DataFrame has exactly the required columns.")
    else:
        extra_columns = actual_columns - required_columns_set
        missing_columns = required_columns_set - actual_columns
        if missing_columns:
            print(f"The DataFrame is missing these columns: {missing_columns}")
            raise AirflowFailException(f"Missing columns: {missing_columns}")

        if extra_columns:
            print(f"The DataFrame has extra columns: {extra_columns}")
            raise AirflowFailException(f"Missing columns: {extra_columns}")


@task()
def etl_traffy_data(input_path, output_path):
    df_traffy_raw = pd.read_parquet(f"{input_path}traffy_raw_{today_date}.parquet")
    
    # Requirement 1: Fitler only Bangkok
    df_traffy_cleaned = df_traffy_raw[(df_traffy_raw['province'] == 'กรุงเทพมหานคร') | (df_traffy_raw['province'] == 'จังหวัดกรุงเทพมหานคร')]
    df_traffy_cleaned['province'] = df_traffy_cleaned['province'].replace('จังหวัดกรุงเทพมหานคร', 'กรุงเทพมหานคร')
    
    # Requirement 2: Fitler only State of Cases
    # Define the allowed states
    allowed_states = ["เสร็จสิ้น", "กำลังดำเนินการ", "รอรับเรื่อง"]

    # filter rows with valid states only
    df_traffy_cleaned = df_traffy_cleaned[df_traffy_cleaned["state"].isin(allowed_states)]

    # Requirement 3: Sorted data by timestamp
    df_traffy_cleaned = df_traffy_cleaned.sort_values(by='timestamp', ascending = False)

    # Load Data as Parquet File to Storage
    df_traffy_cleaned.to_parquet(f"{output_path}/traffy_cleaned_{today_date}.parquet", compression="zstd", index=False)


@task()
def print_success():
    print("SUCCESS: The data is loaded as Parquet.")


@dag(default_args=default_args, start_date=days_ago(1), tags=['Traffy'])
def traffy_pipeline():
    # Define Folders
    raw_data_folder = '/opt/airflow/dags/traffy_data/raw/'
    cleaned_data_folder = '/opt/airflow/dags/traffy_data/cleaned/'

    # Create task
    create_raw_folder_task = create_folder_if_not_exists(folder_path=raw_data_folder)
    create_cleaned_folder_task = create_folder_if_not_exists(folder_path=cleaned_data_folder)

    save_raw_data_task = save_raw_data(output_path=raw_data_folder)

    check_schema_task = check_schema(input_path=raw_data_folder)

    etl_traffy_data_task = etl_traffy_data(input_path=raw_data_folder, output_path=cleaned_data_folder)

    print_load_success = print_success()

    # Crate Task Dependency (Create DAG)
    [create_raw_folder_task, create_cleaned_folder_task] >> save_raw_data_task >> check_schema_task >> etl_traffy_data_task >> print_load_success


traffy_pipeline()