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
PROCESS_DATE= datetime.now().strftime('%Y-%m-%d')
#TEMPLATE LOCATION FOR cs-jll-floor-space
TEMPLATE='gs://cs-data-platform-dataflow-prod/df-template/cs-jll-floor-space.pb'
PARAMETER={
    'runner':'DataflowRunner',
    'region':'europe-west2',
    'project':'hsbc-11359979-dbsrefinery-prod',
    'process_name':'floor-space',
    'process_date':f'{PROCESS_DATE}',
    'out_file_name_with_path':f'gs://cs_data_platform_jll_prod/out_files/floor-space/floor-space-{PROCESS_DATE}'
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
    'cs_jll_floor_space_prod_dag',
    default_args=default_args,
    schedule_interval='30 7 * * *',
    catchup=False,
    tags=['GCP']
) as dag:
    
    start_jll_floor_space = DataflowTemplatedJobStartOperator(
        task_id='start_jll_floor_space',
        template=TEMPLATE,
        job_name='cs-jll-floor-space',
        parameters=PARAMETER,
        environment=ENVIRONMENT,
        project_id='hsbc-11359979-dbsrefinery-prod',
        location='europe-west2',
        impersonation_chain=IMPERSONATION_CHAIN,
    )