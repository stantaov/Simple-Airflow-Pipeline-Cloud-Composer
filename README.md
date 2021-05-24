## Simple Airflow Pipeline Cloud Composer

This pipeline allows taking a file from cloud storage and loading it into the BigQuery table and running a simple SQL query. 

Steps to take

1. Upload MOCK_DATA.csv to cloud bucket
2. Create Cloud Composer cluster
3. Load DAG aitflow_wcd_class.py file in Cloud Composer storage dags folder
4. Trigger it in the Airflow web interface
