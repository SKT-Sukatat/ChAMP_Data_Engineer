from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

# TODO: import prerequisite library


# TODO: Define Traffy Fondue API for data request
TRAFFY_RECORDS_API = ""

# TODO: Define default arguments for DAGs
default_args = {
    'owner': ,
    'retries': ,
    'retry_delay': ,
    'schedule_interval':  # TODO: Requirement 4 Automated data everyday 8.00 AM
}


@task()
def etl_traffy_data(output_path):
    # TODO: Ingest data as Pandas DataFrames
    df_traffy = pd.read_csv(TRAFFY_RECORDS_API)
    
    # TODO: Requirement 1 Fitler data only Bangkok

    
    # TODO: Requirement 2 Fitler only allowed State of Cases


    # TODO: Requirement 3 Sorted data by timestamp
    df_traffy = df_traffy.sort_values(by='timestamp', ascending = False)

    # TODO: Load Data as Parquet File to Storage
    today_date = datetime.now().strftime("%d_%m_%Y")
    file_name = "traffy_cleaned_" + today_date + ".parquet"
    df_traffy.to_parquet(output_path + file_name)


@task()
def print_success():
    print("SUCCESS: The data is loaded as Parquet.")


@dag(default_args=default_args, start_date=days_ago(1), tags=['Traffy'])
def traffy_pipeline():
    # TODO: Create Tasks


    # Crate Task Dependency (Create DAG)


# TODO: Called DAG
traffy_pipeline()