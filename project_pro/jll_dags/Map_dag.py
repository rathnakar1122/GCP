from airflow import models
from datetime import *
import airflow
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.operators.python import PythonOperator
import os

os.environ["AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT"] = 'google-cloud-platform://'
IMPERSONATION_CHAIN = "dbs-csdtplatform-svc-account@hsbc-11359979-dbsrefinery-prod.iam.gserviceaccount.com"


#TEMPLATE LOCATION FOR CS-MIST-REFERENCE-MAP
TEMPLATE='gs://cs-data-platform-dataflow-prod/df-template/cs-mist-reference-map.pb'
PARAMETER={
    'runner':'DataflowRunner',
    'region':'europe-west2',
    'project':'hsbc-11359979-dbsrefinery-prod',
    'process_name':'map',
    'process_date':'2024-09-03',
    }
ENVIRONMENT={
    'maxWorkers': 2,
    'tempLocation':'gs://cs-data-platform-dataflow-prod/tmp/',
    'kmsKeyName':'projects/hsbc-6320774-kms-prod/locations/europe-west2/keyRings/computeEngine/cryptoKeys/vtDataflowKey',
    'subnetwork':'regions/europe-west2/subnetworks/dataflow-europe-west2',
    }
        
default_args = {
    'owner':'DBSR',
    'depends_on_past':False,
    'start_date': days_ago(1),
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':0
}
# Define the DAG
with models.DAG(
    'cs_mist_reference_map_prod_dag',
    default_args=default_args,
    schedule_interval='0 7 * * *',
    catchup=False,
    tags=['GCP']
) as dag:
    
    start_mist_reference_map = DataflowTemplatedJobStartOperator(
        task_id='start_mist_reference_map',
        template=TEMPLATE,
        job_name='cs-mist-reference-map',
        parameters=PARAMETER,
        environment=ENVIRONMENT,
        project_id='hsbc-11359979-dbsrefinery-prod',
        location='europe-west2',
        impersonation_chain=IMPERSONATION_CHAIN,
    )