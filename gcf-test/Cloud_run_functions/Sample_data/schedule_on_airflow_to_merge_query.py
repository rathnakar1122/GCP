from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 26),  # Change this to your desired start date
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'customer_info_merge',
    default_args=default_args,
    description='A DAG to execute MERGE SQL query for customer data',
    schedule_interval='0 2 * * *',  # This will run the query every day at 2 AM. Change as per your requirement
    catchup=False,
)

# SQL query to be executed
merge_sql = """
MERGE INTO `rathnakar-18m85a0320-hiscox.prod.CUSTOMER_INFO_f` tgt
USING `rathnakar-18m85a0320-hiscox.MAIN.CUSTOMER_INFO` src
ON tgt.cust_id = src.cust_id

WHEN MATCHED AND tgt.source_system_date < CURRENT_DATE() THEN
  UPDATE SET
    tgt.cust_name = src.cust_name,
    tgt.phone_number = src.phone_number,
    tgt.email_address = src.email_address,
    tgt.address = src.address,
    tgt.source_system_date = src.source_system_date,
    tgt.status = 'active'

WHEN NOT MATCHED THEN
  INSERT (
    cust_id, cust_name, phone_number, email_address, address, source_system_date, source_update_date, start_date, end_date, status
  )
  VALUES (
    src.cust_id, src.cust_name, src.phone_number, src.email_address, src.address, src.source_system_date, 
    src.source_system_date, DATE '2025-01-16', NULL, 'active'
  );
"""

# Task to run the SQL query
run_merge_sql = PostgresOperator(
    task_id='run_merge_customer_info',
    sql=merge_sql,
    postgres_conn_id='your_postgres_conn_id',  # Replace with your Airflow Postgres connection ID
    autocommit=True,
    dag=dag,
)

run_merge_sql
