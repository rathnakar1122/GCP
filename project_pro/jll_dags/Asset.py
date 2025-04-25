from airflow import models
from datetime import *
import airflow
from airflow import DAG 
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage
import os
import sys
import time 
from datetime import datetime, timedelta

os.environ["AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT"] = 'google-cloud-platform://'
IMPERSONATION_CHAIN = "dbs-csdtplatform-svc-account@hsbc-11359979-dbsrefinery-prod.iam.gserviceaccount.com"

current_date= datetime.now().strftime('%Y%m%d')
PROCESS_DATE= datetime.now().strftime('%Y-%m-%d')

# Function to print message if file not present
def file_not_present():
    print(f"Assets_{current_date}.csv file is not present in GCS bucket")

# query to delete current_date records from RAW_CS_DP_REAL_ESTATE_COMPONENT_ASSET
src_sql_1 = f"""DELETE FROM `hsbc-11359979-dbsrefinery-prod.dcoo_cs_analytics_data_prod.RAW_CS_DP_REAL_ESTATE_COMPONENT_ASSET` WHERE REPORT_DATE = '{PROCESS_DATE}'"""

# query to delete current_date records from DA_CS_DP_REAL_ESTATE_COMPONENT_ASSET
src_sql_2 = f"""DELETE FROM `hsbc-11359979-dbsrefinery-prod.dcoo_cs_analytics_work_prod.DA_CS_DP_REAL_ESTATE_COMPONENT_ASSET` WHERE REPORT_DATE = '{PROCESS_DATE}'"""

#TEMPLATE LOCATION FOR ETL1
TEMPLATE_FOR_ETL1='gs://cs-data-platform-dataflow-prod/df-template/cs-jll-etl-1-asset-work-ds.pb'


#TEMPLATE LOCATION FOR ETL2
TEMPLATE_FOR_ETL2='gs://cs-data-platform-dataflow-prod/df-template/cs-jll-etl-2-asset-work-ds.pb'


PARAMETERS_FOR_ETL1={
    'runner':'DataflowRunner',
    'region':'europe-west2',
    'project':'hsbc-11359979-dbsrefinery-prod',
    'process_name':'asset',
    'process_date':f'{PROCESS_DATE}',
    'file_name_with_path':f'gs://cs_data_platform_jll_prod/asset/{current_date}/Assets_{current_date}.csv',
    }


PARAMETERS_FOR_ETL2={
    'runner':'DataflowRunner',
    'region':'europe-west2',
    'project':'hsbc-11359979-dbsrefinery-prod',
    'process_name':'asset',
    'process_date':f'{PROCESS_DATE}',
    }

#ENVIRONMENT for both ETL1 & ETL2

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
    'cs_jll_asset_work_prod_dag',
    default_args=default_args,
    schedule_intervel = timedelta(months=1),
    start_date=datetime(2025,5,1,1,0,0),
    catchup=False,
    tags=['GCP']
) as dag:
    

    check_gcs_file = GCSObjectExistenceSensor(
        task_id='check_gcs_file_task',
        bucket='cs_data_platform_jll_prod',
        object=f'asset/{current_date}/asset_{current_date}.csv',
        impersonation_chain=IMPERSONATION_CHAIN,
        mode='poke',
        timeout=300,
    )

    delete_bq_record_RAW = BigQueryInsertJobOperator(
        task_id="delete_bq_record_RAW",
        configuration={"query": {"query": src_sql_1,
                                 "useLegacySql": False}},
        impersonation_chain=IMPERSONATION_CHAIN,
        location="europe-west2",
    )
    start_cs_jll_etl1_asset_work = DataflowTemplatedJobStartOperator(
        task_id='start_cs_jll_etl1_asset_work',
        template=TEMPLATE_FOR_ETL1,
        job_name='cs_jll_asset_work_etl1',
        parameters=PARAMETERS_FOR_ETL1,
        environment=ENVIRONMENT,
        project_id='hsbc-11359979-dbsrefinery-prod',
        location='europe-west2',
        impersonation_chain=IMPERSONATION_CHAIN,
    )
    delete_bq_record_WORK = BigQueryInsertJobOperator(
        task_id="delete_bq_record_WORK",
        configuration={"query": {"query": src_sql_2,
                                 "useLegacySql": False}},
        impersonation_chain=IMPERSONATION_CHAIN,
        location="europe-west2",
    )

    start_cs_jll_etl2_asset_work = DataflowTemplatedJobStartOperator(
        task_id='start_cs_jll_etl2_asset_work',
        template=TEMPLATE_FOR_ETL2,
        job_name='cs_jll_asset_work_etl2',
        parameters=PARAMETERS_FOR_ETL2,
        environment=ENVIRONMENT,
        project_id='hsbc-11359979-dbsrefinery-prod',
        location='europe-west2',
        impersonation_chain=IMPERSONATION_CHAIN,
    )
    file_not_present_task = PythonOperator(
        task_id = "file_not_present",
        python_callable= file_not_present,
    )

    #define task dependencies
    check_gcs_file >> [delete_bq_record_RAW, file_not_present_task]


    delete_bq_record_RAW >> start_cs_jll_etl1_asset_work
    start_cs_jll_etl1_asset_work >> delete_bq_record_WORK
    delete_bq_record_WORK >> start_cs_jll_etl2_asset_work



    #Using triggerrule
    file_not_present_task.trigger_rule = TriggerRule.ONE_FAILED
    delete_bq_record_RAW.trigger_rule = TriggerRule.ALL_SUCCESS
    start_cs_jll_etl1_asset_work = TriggerRule.ALL_SUCCESS
    delete_bq_record_WORK = TriggerRule.ALL_SUCCESS
    start_cs_jll_etl2_asset_work = TriggerRule.ALL_SUCCESS