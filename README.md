# ELT
## Tools

* Postgres: Data Source
* Docker: Manages application dependencies and ensures environment consistency.
* Python: Ingestion data
* BigQuery: Functions as the data warehouse solution for querying and analyzing data.
* Astro CLI: Used to manage and orchestrate data workflows.
* Airflow: Handles the orchestration of tasks within the data pipeline
* DBT: Transformation

## ELT Pipeline: Data Extraction, Loading, and Transformation with Airflow and dbt

* Data Extraction: Retrieve data from a PostgreSQL database and CSV files using Airflow (PythonOperator).
* Data Loading: Load raw data into Google BigQuery.
* Data Transformation: Use dbt to transform data directly in BigQuery.
* Task Scheduling: Automate the ELT process using Apache Airflow.

## Create an Astro project
```
Astro dev init
```
This command creates all the necessary project files for running Airflow locally, including pre-built example DAGs that you can execute immediately.

##  Create a Service Account and Assign Roles:
* Go to “IAM & Admin” > “Service accounts” in the Google Cloud Console.
* Click “Create Service Account”.
* Name your service account.
* Assign the “Owner” roles to the service account.
* Finish the creation process.
* Make a JSON key to let the service account sign in.
* Find the service account in the “Service accounts” list.
* Click on the service account name.
* In the “Keys” section, click “Add Key” and pick JSON.
* The key will download automatically. Keep it safe and don’t share it.

## Move to direktori dags and create a folder name dbt
```
cd dags
mkdir dbt
cd dbt
```
## Installation & Configuration virtual environments 
```
python3 -m venv dbt-venv   
```

### Activate virtual environment
```
source dbt-venv/bin/activate
```

## Install and Setup dbt
Install dbt-bigquery
```
pip install dbt-bigquery==1.8.2
```

Run dbt cli to init dbt with BigQuery as data platform
```
dbt init project_saya
```





![image](https://github.com/user-attachments/assets/0badfd39-cc7d-4b30-800f-ca67d2624b1c)


