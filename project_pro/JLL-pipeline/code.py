import os
import re
import logging
import argparse
import configparser
from datetime import datetime
from dateutil import parser
import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from google.cloud import storage, bigquery


class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--env_file', type=str, help='Environment file path')
        parser.add_argument('--env', choices=['dev', 'qa', 'uat', 'prod'], type=str, help='Environment name')
        parser.add_argument('--table', default=None, type=str, help='BigQuery table name')


def get_job_details(env, env_file):
    try:
        global env_conf_path
        env_conf_path = env_file.strip('gs://').split('/')
        client = storage.Client()
        bucket = client.bucket(env_conf_path[0])
        blob = bucket.blob('/'.join(env_conf_path[1:]))

        if not blob.exists():
            raise FileNotFoundError(f'Config file not found in GCS: {env_file}')

        content = blob.download_as_text(encoding='utf-8')
        config = configparser.ConfigParser()
        config.read_string(content)
        details = dict(config.items(env.upper()))
        logging.info(f'Environment details: {details}')
        return details

    except Exception as e:
        logging.error(f'Error in get_job_details: {str(e)}')
        raise


def files_check(raw_files_bucket, raw_files_folder, tables_details_file, input_tbl):
    try:
        client = storage.Client()
        tbl_bucket = client.bucket(env_conf_path[0])
        tbl_blob = tbl_bucket.blob('/'.join(env_conf_path[1:-1]) + '/' + tables_details_file)

        if not tbl_blob.exists():
            raise FileNotFoundError(f'Tables config file not found: {tables_details_file}')

        tbl_content = tbl_blob.download_as_text(encoding='utf-8')
        config = configparser.ConfigParser()
        config.read_string(tbl_content)
        tbl_details = dict(config.items(input_tbl))

        header = int(tbl_details['headers'])
        delimiter = tbl_details['delimiter']
        input_files = tbl_details['files'][1:-1].replace(' ', '').split(',')
        job_name = tbl_details['job-name']

        raw_bucket = client.bucket(raw_files_bucket)
        available_blobs, unavailable_files = [], []

        for file in input_files:
            blob = raw_bucket.blob(raw_files_folder + file)
            if blob.exists():
                available_blobs.append(blob)
            else:
                unavailable_files.append(file)

        return header, delimiter, available_blobs, unavailable_files, job_name

    except Exception as e:
        logging.error(f'Error in files_check: {str(e)}')
        raise


def getting_schema(table):
    try:
        project, dataset, tbl = table.split('.')
        client = bigquery.Client(project)
        table_ref = client.dataset(dataset).table(tbl)
        table_obj = client.get_table(table_ref)
        schema_fields = {field.name: field.field_type for field in table_obj.schema}
        return schema_fields

    except Exception as e:
        logging.error(f'Error in getting_schema: {str(e)}')
        raise


def data_typecast(element, schema, delimiter):
    try:
        split_data = element.split(delimiter)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if len(split_data) == len(schema) - 1:
            split_data.append(timestamp)
        elif len(split_data) == len(schema):
            split_data[-1] = timestamp
        else:
            split_data += [''] * (len(schema) - len(split_data))
            split_data[-1] = timestamp

        json_data = {}
        for i, (col, dtype) in enumerate(schema.items()):
            val = split_data[i].strip()

            if dtype == 'INTEGER':
                json_data[col] = int(re.sub(r'[^\d\.-]', '', val)) if val else None
            elif dtype == 'STRING':
                json_data[col] = val
            elif dtype in ['NUMERIC', 'FLOAT']:
                prec = 4 if 'EXPOSURE' in col.upper() else 2
                json_data[col] = round(float(re.sub(r'[^\d\.-]', '', val)), prec) if val else None
            elif dtype == 'TIMESTAMP':
                json_data[col] = datetime.strptime(val, "%Y-%m-%d %H:%M:%S") if val else None
            elif dtype == 'DATE':
                json_data[col] = parser.parse(val).date() if val else None
            else:
                json_data[col] = val

        return json_data

    except Exception as e:
        logging.error(f"Error in data_typecast: {str(e)}")
        raise


def data_processing(pipeline, input_file, bigquery_table, delimiter, header):
    schema = getting_schema(bigquery_table)

    (
        pipeline
        | f"Read_{input_file}" >> beam.io.ReadFromText(input_file, skip_header_lines=header)
        | f"Process_{input_file}" >> beam.Map(data_typecast, schema, delimiter)
        | f"Write_{input_file}" >> WriteToBigQuery(
            bigquery_table,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            custom_gcs_temp_location='gs://acg-claims-fin-dev-raw-data/DFtemp/Temp/'
        )
    )


def run(argv=None):
    try:
        pipelineoptions = PipelineOptions(argv)
        customoptions = pipelineoptions.view_as(CustomOptions)

        env_details = get_job_details(customoptions.env, customoptions.env_file)
        dataset = env_details['dataset-bq']
        project_id = env_details['project-id']
        raw_files_bucket = env_details['raw-bucket']
        raw_files_folder = env_details['raw-folder']
        tables_details_file = env_details['tables-file-name']

        header, delimiter, available_files, unavailable_files, job_name = files_check(
            raw_files_bucket, raw_files_folder, tables_details_file, customoptions.table
        )

        if not available_files:
            raise Exception(f"No valid files found. Missing: {unavailable_files}")

        options = PipelineOptions(
            project=project_id,
            runner='DataflowRunner',
            staging_location=env_details['staging-location'],
            temp_location=env_details['temp-location'],
            region=env_details['region'],
            subnetwork=env_details['subnetwork'],
            job_name=job_name
        )

        google_cloud_options = options.view_as(GoogleCloudOptions)
        google_cloud_options.service_account_email = env_details['impersonated-service-account']
        google_cloud_options.template_location = f"{env_details['template-folder']}/{job_name}"

        bigquery_table = f"{project_id}.{dataset}.{customoptions.table}"

        with beam.Pipeline(options=options) as pipeline:
            for blob in available_files:
                file_name = blob.name.split('/')[-1]
                input_file = f'gs://{raw_files_bucket}/{raw_files_folder}{file_name}'
                data_processing(pipeline, input_file, bigquery_table, delimiter, header)

    except Exception as e:
        logging.error(f'Pipeline failed: {str(e)}')
        raise


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
