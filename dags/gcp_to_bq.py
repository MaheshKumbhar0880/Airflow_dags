from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable
from google.cloud import bigquery
from datetime import datetime
import pandas as pd
import json
from airflow.operators.python import PythonOperator

# Define your DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG('postgres_to_bigquery', 
         default_args=default_args, 
         description='Load data from PostgreSQL to BigQuery', 
         schedule_interval='@daily', 
         start_date=datetime(2024, 7, 21), 
         catchup=False) as dag:

    # Task to extract data from PostgreSQL
    def extract_from_postgres():
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        sql = "SELECT * FROM your_table"
        df = postgres_hook.get_pandas_df(sql)
        df.to_csv('/tmp/data.csv', index=False)

    extract_task = PythonOperator(
        task_id='extract_from_postgres',
        python_callable=extract_from_postgres
    )

    # Task to load data to BigQuery
    def load_to_bigquery():
        client = bigquery.Client()
        table_id = 'your_project.your_dataset.your_table'
        
        # Load CSV file into a DataFrame
        df = pd.read_csv('/tmp/data.csv')
        
        # Convert DataFrame to JSON Lines format
        json_data = df.to_json(orient='records', lines=True)
        with open('/tmp/data.json', 'w') as f:
            f.write(json_data)
        
        # Load data from JSON Lines file to BigQuery
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
        )

        with open('/tmp/data.json', 'rb') as source_file:
            load_job = client.load_table_from_file(source_file, table_id, job_config=job_config)

        load_job.result()  # Wait for the job to complete

    load_task = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_to_bigquery
    )

    # Define the task dependencies
    extract_task >> load_task
