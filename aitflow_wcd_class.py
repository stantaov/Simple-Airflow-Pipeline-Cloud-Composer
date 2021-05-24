from datetime import datetime, timedelta
from airflow import DAG
from google.cloud import bigquery
import os
from airflow.contrib.operators import bigquery_operator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.models import DagBag
import unittest


YESTERDAY = datetime.now() - timedelta(days=1)
DATASET_NAME = os.environ.get("GCP_MOCK", 'airflow_test')
TABLE_NAME = os.environ.get("GCP_MOCK_TABLE", 'gcs_to_bq_table')

default_args = {
    'owner': 'Stan Taov',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=10),
    'start_date': YESTERDAY
}

dag = DAG(
        'wcd-airflow-class',
        default_args=default_args,
        schedule_interval='@once'
        )

create_mock_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_airflow_test_dataset', dataset_id=DATASET_NAME, dag=dag
)


load_csv = GCSToBigQueryOperator(
    task_id='gcs_to_bigquery',
    bucket='stan-wcd-bucket',
    source_objects=['MOCK_DATA.csv'],
    destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
    schema_fields=[
        {'name': 'id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'gender', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'app', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)

    
bq_query_mock = bigquery_operator.BigQueryOperator(
        dag=dag,
        task_id='mock_test',
        sql="""
        SELECT id, first_name, last_name
        FROM `stan-wcd.airflow_test.gcs_to_bq_table` 
        """,
        use_legacy_sql=False)

create_mock_dataset >> load_csv >> bq_query_mock