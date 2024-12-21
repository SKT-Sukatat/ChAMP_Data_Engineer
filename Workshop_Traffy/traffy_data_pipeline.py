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


# TODO: Define the today date
today_date = 

# TODO: Define create_folder_if_not_exists function
@task()
def create_folder_if_not_exists(folder_path):


# TODO: Define save_raw_data function
@task()
def save_raw_data(output_path):
    # TODO: Declare df_traffy_raw DataFrames from TRAFFY_RECORDS_API
    df_traffy_raw = pd.read_parquet()
    # Declare file name of the raw data
    parquet_filename = 
    # Export the DataFrame to a Parquet file
    df_traffy_raw.to_parquet(output_path + parquet_filename, compression="zstd", index=False)


@task()
def check_schema(input_path):
    # Extract Traffy Data from API
    df_traffy_raw = pd.read_parquet(input_path)

    # TODO: Check if the DataFrame has exactly the same columns
    actual_columns = 

    # TODO: Define all required columns as a set
    required_columns_set = {}

    # TODO: Create Logic for raise an exception
    if actual_columns == required_columns_set:
        print("The DataFrame has exactly the required columns.")
    else:
        extra_columns =
        missing_columns = 
        if missing_columns:
            print(f"The DataFrame is missing these columns: {missing_columns}")
            raise AirflowFailException(f"Missing columns: {missing_columns}")

        if extra_columns:
            print(f"The DataFrame has extra columns: {extra_columns}")
            raise AirflowFailException(f"Missing columns: {extra_columns}")


@task()
def etl_traffy_data(input_path, output_path):
    df_traffy_raw = pd.read_parquet(f"{input_path}traffy_raw_{today_date}.parquet")
    
    # TODO: Requirement 1 Fitler only Bangkok
    df_traffy_cleaned = df_traffy_raw[]
    df_traffy_cleaned['province'] =

    # TODO: Requirement 2 Fitler only State of Cases
    # Define the allowed states
    allowed_states = []

    # TODO: filter rows with valid states only
    df_traffy_cleaned =

    # TODO: Requirement 3 Sorted data by timestamp
    df_traffy_cleaned = df_traffy_cleaned.sort_values(by='', ascending = False)

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
    create_raw_folder_task = 
    create_cleaned_folder_task = 

    save_raw_data_task = 

    check_schema_task = 

    etl_traffy_data_task = 

    print_load_success = 

    # Crate Task Dependency (Create DAG)
    


traffy_pipeline()