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

## Setup `dbt_project.yml` configuration
```
models:
  project_saya:
    # Config indicated by + and applies to all files under models/example/
    _stg:
      +materialized: view
      +schema: _stg
      +enabled: true
    _int:
      +materialized: table
      +schema: _int
      +enabled: true

    _fct:
      +materialized: table
      +schema: _fct
      +enabled: true

    mart:
      +materialized: table
      +schema: mart
      +enabled: true
```

## Setup DBT Profile
By default, DBT will create a dbt profile at your home directory ~/.dbt/profiles.yml You can update the profiles, or you can make a new dbt-profile directory. To make a new dbt-profie directory, you can invoke the following:
```
mkdir profiles
touch profiles/profiles.yml
export DBT_PROFILES_DIR=$(pwd)/profiles
```
Also, create a keyfile folder:
```
mkdir key_file
```
Then copy the service account JSON file and place it inside the key_file folder.

Set profiles.yml as follow:
```
project_saya:
  outputs:
    dev:
      dataset: transformed_data
      job_execution_timeout_seconds: 300
      job_retries: 1
      keyfile: /usr/local/airflow/dags/dbt/project_saya/file_json/your_bigquery_key.json
      location: US
      method: service-account
      priority: interactive
      project: myfarhan54875
      threads: 1
      type: bigquery
  target: dev
```
`Note` for the keyfile: make sure to match it with the name of your service account JSON file.

## Run Airflow locally
Before you run Airflow locally you need to add the following command to the dockerfile.
```
FROM quay.io/astronomer/astro-runtime:12.1.0

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-bigquery google-cloud-bigquery && deactivate
```

And you need to add following to the requirements.txt
```
# Astro Runtime includes the following pre-installed providers packages: https://www.astronomer.io/docs/astro/runtime-image-architecture#provider-packages
astronomer-cosmos
apache-airflow
apache-airflow-providers-google
apache-airflow-providers-postgres
```

Before running Airflow, by default airflow will run on localhost:8080, but we can set the port as desired. In the .astro folder, there is a file called config.yml. Here, I set the PostgreSQL port to 5435 and the Airflow port to 8089. So i have to use localhost:8089 to open airflow.
```
project:
  name: my-dbt-dag-project
webserver:
  port: 8089
postgres:
  port: 5435
```

After that run
```
Astro dev start
```
In dbt dags folder add dag.py and add this code
```
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from extract_postgres.brands import extract_brands
from extract_postgres.products import extract_products
from extract_postgres.orders import extract_orders
from extract_postgres.order_details import extract_order_details

# Konfigurasi dataset
dataset_id = "my_data"
start = DummyOperator(task_id="start")

dag = DAG(
    dag_id="extract_data",
    description="Extract data from PostgreSQL and load to BigQuery",
    schedule_interval="@daily",
    start_date=datetime(2024, 9, 1),
    catchup=False,
)

# Buat tabel BigQuery
create_bq_table_brands = BigQueryCreateEmptyTableOperator(
    task_id="create_bq_table_brands",
    dataset_id=dataset_id,
    table_id="raw_brands",
    schema_fields=[
        {"name": "brand_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "name", "type": "STRING", "mode": "REQUIRED"},
    ],
    gcp_conn_id="bigquery_con",
    dag=dag,
)

create_bq_table_products = BigQueryCreateEmptyTableOperator(
    task_id="create_bq_table_products",
    dataset_id=dataset_id,
    table_id="raw_products",
    schema_fields=[
        {"name": "product_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "brand_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "price", "type": "NUMERIC", "mode": "NULLABLE"},
    ],
    gcp_conn_id="bigquery_con",
    dag=dag,
)

create_bq_table_orders = BigQueryCreateEmptyTableOperator(
    task_id="create_bq_table_orders",
    dataset_id=dataset_id,
    table_id="raw_orders",
    schema_fields=[
        {"name": "order_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "order_date", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "customer_phone", "type": "STRING", "mode": "NULLABLE"},
    ],
    gcp_conn_id="bigquery_con",
    dag=dag,
)

create_bq_table_order_details = BigQueryCreateEmptyTableOperator(
    task_id="create_bq_table_order_details",
    dataset_id=dataset_id,
    table_id="raw_order_details",
    schema_fields=[
        {"name": "order_detail_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "order_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "product_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "quantity", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "price", "type": "NUMERIC", "mode": "NULLABLE"},
    ],
    gcp_conn_id="bigquery_con",
    dag=dag,
)

# Extract data
extract_brands_data = PythonOperator(
    task_id="extract_brands",
    python_callable=extract_brands,
    dag=dag,
)

extract_products_data = PythonOperator(
    task_id="extract_products",
    python_callable=extract_products,
    dag=dag,
)

extract_orders_data = PythonOperator(
    task_id="extract_orders",
    python_callable=extract_orders,
    dag=dag,
)

extract_order_details_data = PythonOperator(
    task_id="extract_order_details",
    python_callable=extract_order_details,
    dag=dag,
)

dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="cd /usr/local/airflow/dags/dbt/project_saya; source /usr/local/airflow/dbt_venv/bin/activate; dbt run --profiles-dir /usr/local/airflow/dags/dbt/project_saya/dbt-profiles/",
    dag=dag,
)

dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command="cd /usr/local/airflow/dags/dbt/project_saya; source /usr/local/airflow/dbt_venv/bin/activate; dbt test --profiles-dir /usr/local/airflow/dags/dbt/project_saya/dbt-profiles/",
    dag=dag,
)


end = DummyOperator(task_id="end")

# Mengatur urutan tugas
start >> [
    create_bq_table_brands,
    create_bq_table_products,
    create_bq_table_orders,
    create_bq_table_order_details,
]

create_bq_table_brands >> extract_brands_data 
create_bq_table_products >> extract_products_data 
create_bq_table_orders >> extract_orders_data 
create_bq_table_order_details >> extract_order_details_data 

(
    [
        extract_brands_data,
        extract_products_data,
        extract_orders_data,
        extract_order_details_data,
    ]
    >> dbt_run
    >> dbt_test
    >> end
)
```

And then open localhost:8089 to access airflow. The username is admin and the password is admin. After that, click on Admin and then click on connections to create big querry connection.
`Note` Copy the contents of the service account JSON file and paste them into the Keyfile JSON.

## Trigger dag
You can trigger the DAG and monitor its progress in the Airflow UI.
![image](https://github.com/user-attachments/assets/0badfd39-cc7d-4b30-800f-ca67d2624b1c)


