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
