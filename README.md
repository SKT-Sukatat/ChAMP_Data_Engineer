# ChAMP_Data_Engineer

This repository containe all resources related to data enginering of ChAMP (Chulalongkorn Alumni Mentorship Program)

## Prerequisite
- [Docker Desktop](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html) installed with Docker Compose for running Airflow containers

## Getting Started
Running Airflow in Docker following this steps:
https://airflow.apache.org/docs/apache-airflow/stable/tutorial/pipeline.html
```
# Download the docker-compose.yaml file
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'

# Make expected directories and set an expected environment variable
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env

# Initialize the database
docker compose up airflow-init

# Start up all services
docker compose up
```

See more about [running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).
