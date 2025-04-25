from airflow import models
from datetime import *
import airflow
from airflow import DAG 
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryValueCheckOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage
import os
import sys

os.environ["AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT"] = 'google-cloud-platform://'
IMPERSONATION_CHAIN = "dbsr-pdtest-dev-svc-account@hsbc-11359979-dbsrefinery-dev.iam.gserviceaccount.com"


def print_bq_records(**kwargs):
    ti = kwargs['ti']
    # Pulling the output of the previous task (BashOperator)
    bq_data = ti.xcom_pull(task_ids='fetch_bq_data')
    
    arid = ti.xcom_pull(task_ids='fetch_bq_data').split(",")[0]
    ti.xcom_push(key='arid', value=arid)

    date_to_process = ti.xcom_pull(task_ids='fetch_bq_data').split(",")[1]
    ti.xcom_push(key='date_to_process', value=date_to_process)

    target_date = datetime.strptime(date_to_process, '%Y-%m-%d').strftime('%Y%m%d')
    ti.xcom_push(key='target_date', value=target_date)

    print(f"BQ_DATA: {bq_data}, ARID: {arid}, DATE_TO_PROCESS: {date_to_process}, TARGET_DATE: {target_date}")

def file_not_present(**kwargs):
    ti = kwargs['ti']
    target_date = ti.xcom_pull(task_ids="print_bq_record", key="target_date")
    print(f"Incidents_{target_date}.csv, file is not present in GCS bucket")

# Function to count rows in the GCS CSV file
def count_gcs_file_rows(**kwargs):
    ti = kwargs['ti']
    target_date = ti.xcom_pull(task_ids='print_bq_record', key='target_date')
    bucket_name = 'cs_data_platform_jll_dev'
    file_name = f"incident/{target_date}/Incidents_{target_date}.csv"
    
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    content = blob.download_as_text()
    rows = content.strip().split('\n')
    row_count = len(rows) - 1  # Exclude header row if applicable
    
    print(f"Row count for {file_name}: {row_count}")
    ti.xcom_push(key='row_count', value=row_count)

def print_bq_run_id(**kwargs):
    ti = kwargs['ti']
    # Pulling the output of the get_run_id task (BashOperator)
    get_bq_run_id = ti.xcom_pull(task_ids='get_run_id')
    ti.xcom_push(key='get_bq_run_id', value=get_bq_run_id)

    print(f"BQ_RUN_ID: {get_bq_run_id}")

# Task to verify status and raise error if pending
def verify_status(ti):
    status = ti.xcom_pull(task_ids="check_process_status")
    if status == 'pending':
        raise AirflowException("Status is pending, failing the task.")
    else :
        print("Status is not pending, proceeding.")


# QUERIES_FOR_CONTROL_TABLES
sql_for_fetch_bq_data = """bq query --use_legacy_sql=false --format=csv 'SELECT (SELECT COALESCE(MAX(AUTO_RUN_ID),0)+1 FROM `hsbc-11359979-dbsrefinery-dev.dcoo_cs_analytics_data_dev.CS_DP_CONTROL_TABLE`) as AR_ID, COALESCE(DATE_ADD(MAX(PROCESS_DATE), INTERVAL 1 DAY),CURRENT_DATE) DATE_TO_PROCESS FROM `hsbc-11359979-dbsrefinery-dev.dcoo_cs_analytics_data_dev.CS_DP_CONTROL_TABLE` WHERE FILE_NAME LIKE "Incident%" AND PROCESS_DATE < CURRENT_DATE ' """

sql_for_check_process_status =  """bq query --use_legacy_sql=false --format=csv 'SELECT PROCESS_STATUS FROM `hsbc-11359979-dbsrefinery-dev.dcoo_cs_analytics_data_dev.CS_DP_CONTROL_TABLE` WHERE FILE_NAME LIKE "Incident%" AND PROCESS_STATUS = "pending' ' """

sql_for_get_run_id = """bq query --use_legacy_sql=false --format=csv 'SELECT COALESCE(MAX(RUN_ID), 0) + 1 as MAX_RUN_ID FROM `hsbc-11359979-dbsrefinery-dev.dcoo_cs_analytics_data_dev.CS_DP_CONTROL_TABLE` where PROCESS_DATE = "{{task_instance.xcom_pull(task_ids="print_bq_record", key="date_to_process")}}" AND  FILE_NAME LIKE "Incident%" ' """

sql_for_insert_into_bq = """
                    INSERT INTO `hsbc-11359979-dbsrefinery-dev.dcoo_cs_analytics_data_dev.CS_DP_CONTROL_TABLE`
                    (process_date, run_id, file_name, file_count, process_status, auto_run_id, process_start_time)
                    VALUES (
                        '{{ task_instance.xcom_pull(task_ids="print_bq_record", key="date_to_process") }}',
                        {{ task_instance.xcom_pull(task_ids="print_run_id", key="get_bq_run_id") }},
                        'Incident_{{task_instance.xcom_pull(task_ids="print_bq_record", key="target_date")}}.csv',
                        {{ task_instance.xcom_pull(task_ids="count_gcs_file_rows", key="row_count") }},
                        'pending',
                        {{ task_instance.xcom_pull(task_ids="print_bq_record", key="arid") }},
                        CURRENT_DATETIME()
                    )
                """

sql_for_insert_raw_count = """
                    UPDATE `hsbc-11359979-dbsrefinery-dev.dcoo_cs_analytics_data_dev.CS_DP_CONTROL_TABLE` set RAW_TABLE_COUNT = {{task_instance.xcom_pull(task_ids="get_raw_count")}} where AUTO_RUN_ID = {{ task_instance.xcom_pull(task_ids="print_bq_record", key="arid") }}
                """
sql_for_insert_da_cs_count = """
                    UPDATE `hsbc-11359979-dbsrefinery-dev.dcoo_cs_analytics_data_dev.CS_DP_CONTROL_TABLE` set DA_TABLE_COUNT = {{task_instance.xcom_pull(task_ids="get_ds_cs_count")}}, DA_ERROR_TABLE_COUNT = {{task_instance.xcom_pull(task_ids="get_ds_cs_error_count")}}, PROCESS_END_TIME = CURRENT_DATETIME(), PROCESS_STATUS = 'completed' where AUTO_RUN_ID = {{ task_instance.xcom_pull(task_ids="print_bq_record", key="arid") }}
                """

# QUERIES_FOR_RAW_BQ_TABLES
src_sql_1 = """DELETE FROM `hsbc-11359979-dbsrefinery-dev.dcoo_cs_analytics_data_dev.RAW_CS_DP_REAL_ESTATE_PROPERTY_INCIDENT` WHERE REPORT_DATE = '{{ task_instance.xcom_pull(task_ids="print_bq_record", key="date_to_process") }}'"""

sql_for_get_raw_count = """ bq query --use_legacy_sql=false --format=csv 'SELECT count(*) FROM `hsbc-11359979-dbsrefinery-dev.dcoo_cs_analytics_data_dev.RAW_CS_DP_REAL_ESTATE_PROPERTY_INCIDENT` WHERE REPORT_DATE = "{{ task_instance.xcom_pull(task_ids="print_bq_record", key="date_to_process") }}" ' """

# QUERIES_FOR_DA_CS_TABLES
sql_for_get_ds_cs_count = """ bq query --use_legacy_sql=false --format=csv 'SELECT count(*) FROM `hsbc-11359979-dbsrefinery-dev.dcoo_cs_analytics_work_dev.DA_CS_DP_REAL_ESTATE_PROPERTY_INCIDENT` WHERE REPORT_DATE = "{{ task_instance.xcom_pull(task_ids="print_bq_record", key="date_to_process") }}" ' """

sql_for_get_ds_cs_error_count = """ bq query --use_legacy_sql=false --format=csv 'SELECT count(*) FROM `hsbc-11359979-dbsrefinery-dev.dcoo_cs_analytics_work_dev.DA_CS_DP_REAL_ESTATE_PROPERTY_INCIDENT_ERROR` WHERE REPORT_DATE = "{{ task_instance.xcom_pull(task_ids="print_bq_record", key="date_to_process") }}" ' """

#TEMPLATE LOCATION FOR ETL1
TEMPLATE_FOR_ETL1='gs://cs-data-platform-dataflow-dev/df-template/cs-jll-etl-1-incident-work-ds.pb'


#TEMPLATE LOCATION FOR ETL2
TEMPLATE_FOR_ETL2='gs://cs-data-platform-dataflow-dev/df-template/cs-jll-etl-2-incident-work-ds.pb'

PARAMETERS_FOR_ETL1={
    'runner':'DataflowRunner',
    'region':'europe-west2',
    'project':'hsbc-11359979-dbsrefinery-dev',
    'process_name':'incident',
    'process_date':'{{ task_instance.xcom_pull(task_ids="print_bq_record", key="date_to_process") }}',
    'file_name_with_path':'gs://cs_data_platform_jll_dev/incident/{{task_instance.xcom_pull(task_ids="print_bq_record", key="target_date")}}/Incidents_{{task_instance.xcom_pull(task_ids="print_bq_record", key="target_date")}}.csv',
    }


PARAMETERS_FOR_ETL2={
    'runner':'DataflowRunner',
    'region':'europe-west2',
    'project':'hsbc-11359979-dbsrefinery-dev',
    'process_name':'incident',
    'process_date':'{{ task_instance.xcom_pull(task_ids="print_bq_record", key="date_to_process") }}',
    }


#ENVIRONMENT for both ETL1 & ETL2
ENVIRONMENT={
    'maxWorkers': 2,
    'tempLocation':'gs://cs-data-platform-dataflow-dev/tmp/',
    'kmsKeyName':'projects/hsbc-6320774-kms-dev/locations/europe-west2/keyRings/computeEngine/cryptoKeys/vtDataflowKey',
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
    'incident_ct_bash_9',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['GCP']
) as dag:

    fetch_bq_data = BashOperator(
        task_id='fetch_bq_data',
        bash_command=(
            sql_for_fetch_bq_data
        ),
        do_xcom_push=True,
    )


    print_bq_record = PythonOperator(
        task_id='print_bq_record',
        python_callable=print_bq_records,
        provide_context=True,
    )


    check_process_status = BashOperator(
        task_id='check_process_status',
        bash_command=(
            sql_for_check_process_status
        ),
        do_xcom_push=True,
    )


    verify_status_task = PythonOperator(
        task_id="verify_status_task",
        python_callable=verify_status,
    )


    check_gcs_file = GCSObjectExistenceSensor(
        task_id='check_gcs_file_task',
        bucket='cs_data_platform_jll_dev',
        object='incident/{{task_instance.xcom_pull(task_ids="print_bq_record", key="target_date")}}/Incidents_{{task_instance.xcom_pull(task_ids="print_bq_record", key="target_date")}}.csv',
        impersonation_chain=IMPERSONATION_CHAIN,
        mode='poke',
        timeout=300,
    )


#Count the number of rows in the GCS file if it exists
    count_rows_task = PythonOperator(
        task_id='count_gcs_file_rows',
        python_callable=count_gcs_file_rows,
        provide_context=True,
    )


    get_run_id = BashOperator(
        task_id='get_run_id',
        bash_command=(
            sql_for_get_run_id
        ),
        do_xcom_push=True,
    )


    print_run_id = PythonOperator(
        task_id='print_run_id',
        python_callable=print_bq_run_id,
        provide_context=True,
    )

    insert_into_bq_task = BigQueryInsertJobOperator(
        task_id='insert_into_bq',
        configuration={
            "query": {
                "query": sql_for_insert_into_bq,
                "useLegacySql": False,
            }
        },
        impersonation_chain=IMPERSONATION_CHAIN,
    )


    delete_bq_record_RAW = BigQueryInsertJobOperator(
        task_id="delete_bq_record_RAW",
        configuration={"query": {"query": src_sql_1,
                                 "useLegacySql": False}},
        impersonation_chain=IMPERSONATION_CHAIN,
        location="europe-west2",
    )


    start_cs_jll_etl1_incident_work = DataflowTemplatedJobStartOperator(
        task_id='start_cs_jll_etl1_incident_work_control',
        template=TEMPLATE_FOR_ETL1,
        job_name='cs_jll_incident_work_etl1_ct',
        parameters=PARAMETERS_FOR_ETL1,
        environment=ENVIRONMENT,
        project_id='hsbc-11359979-dbsrefinery-dev',
        location='europe-west2',
        impersonation_chain=IMPERSONATION_CHAIN,
    )


    get_raw_count = BashOperator(
        task_id='get_raw_count',
        bash_command=(
            sql_for_get_raw_count
        ),
        do_xcom_push=True,
    )

    
    insert_raw_count_task = BigQueryInsertJobOperator(
        task_id='insert_raw_count',
        configuration={
            "query": {
                "query": sql_for_insert_raw_count,
                "useLegacySql": False,
            }
        },
        impersonation_chain=IMPERSONATION_CHAIN,
    )


    start_cs_jll_etl2_incident_work = DataflowTemplatedJobStartOperator(
        task_id='start_cs_jll_etl2_incident_work_control',
        template=TEMPLATE_FOR_ETL2,
        job_name='cs_jll_incident_work_etl2_ct',
        parameters=PARAMETERS_FOR_ETL2,
        environment=ENVIRONMENT,
        project_id='hsbc-11359979-dbsrefinery-dev',
        location='europe-west2',
        impersonation_chain=IMPERSONATION_CHAIN,
    )


    get_ds_cs_count = BashOperator(
        task_id='get_ds_cs_count',
        bash_command=(
            sql_for_get_ds_cs_count
        ),
        do_xcom_push=True,
    )


    get_ds_cs_error_count = BashOperator(
        task_id='get_ds_cs_error_count',
        bash_command=(
            sql_for_get_ds_cs_error_count
        ),
        do_xcom_push=True,
    )


    insert_da_cs_count_task = BigQueryInsertJobOperator(
        task_id='insert_da_cs_count',
        configuration={
            "query": {
                "query": sql_for_insert_da_cs_count,
                "useLegacySql": False,
            }
        },
        impersonation_chain=IMPERSONATION_CHAIN,
    )


    file_not_present_task = PythonOperator(
        task_id="file_not_present",
        python_callable=file_not_present,
    )



fetch_bq_data >> print_bq_record >> check_process_status >> verify_status_task
verify_status_task >> check_gcs_file
check_gcs_file.trigger_rule = TriggerRule.ALL_SUCCESS
check_gcs_file >> count_rows_task >> get_run_id >> print_run_id >> insert_into_bq_task >> delete_bq_record_RAW >> start_cs_jll_etl1_incident_work >> get_raw_count >> insert_raw_count_task >> start_cs_jll_etl2_incident_work >> get_ds_cs_count >> get_ds_cs_error_count >> insert_da_cs_count_task
check_gcs_file >> file_not_present_task
file_not_present_task.trigger_rule = TriggerRule.ONE_FAILED