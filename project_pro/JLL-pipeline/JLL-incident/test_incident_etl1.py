from dataclasses import dataclass
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging, json, configparser, os
from datetime import datetime
from google.cloud import storage, bigquery
from io import BytesIO
import pandas as pd
from apache_beam.io.gcp.internal.clients import bigquery as bq_beam
from io import StringIO
import csv
import re
import traceback

logging.getLogger().setLevel(logging.INFO)
from apache_beam.options.value_provider import StaticValueProvider

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/build/workspace/hsbc-11359979-dbsrefinery-dev/TemplateCreation-dev/dbsr-pdtest-dev-svc-account.json"

class StrUtf8Coder(beam.coders.coders.Coder):
    def encode(self, value):
        return value.encode('utf-8', 'ignore')
    def decode(self, value):
        return value.decode('utf-8', 'ignore')
    def is_deterministic(self):
        return True

class read_from_csv(beam.DoFn):
    def process(self, element, file_name, csv_headers):
        print(file_name.get())
        df = beam.io.ReadFromCsv(file_name.get(), encoding='utf-8', encoding_errors='ignore', header=0, names=csv_headers)
        print('=====================================')
        print(df)
        print('=====================================')
        yield beam.io.ReadFromCsv(file_name.get(), encoding='utf-8', encoding_errors='ignore', header=0, names=csv_headers, converters='dict')

class return_raw_structure(beam.DoFn):
    def process(self, element, report_date):
        try:
            if element == "":
                return False
            reader = csv.reader([element])
            x = next(reader)
            logging.getLogger().setLevel('INFO')

            down_time = None
            try:
                if x[16]:
                    down_time = int(x[16])
            except ValueError:
                logging.warning(f"Invalid downtime value '{x[16]}'; expected an integer.")

            incident_data = {
                'HSBC_REGION': x[0],
                'COUNTRY_ISO': x[1],
                'HORIZON_ID': x[2],
                'BUILDING_NAME': x[3],
                'PROPERTY_TYPE': x[4],
                'PROPERTY_STATUS': x[5],
                'PROPERTY_CRITICALITY': x[6],
                'HSBC_BUSINESS_LINE': x[7],
                'JLL_RESPONSIBILITY': x[8],
                'INCIDENT_NUMBER': x[9],
                'INCIDENT_CREATED_DATE': x[10],
                'INCIDENT_OCCURRENCE_DATE': x[11],
                'INCIDENT_CATEGORY': x[12],
                'INCIDENT_TYPE': x[13],
                'INCIDENT_SUMMARY': x[14],
                'INCIDENT_STATUS': x[15],
                'SYSTEM_DOWNTIME': down_time,
                'BUSINESS_IMPACT': x[17],
                'SEVERITY': x[18],
                'PREVENTABLE': x[19],
                'IMPACT_TO_SITE_AVAILABILITY': x[20],
                'RESPONSIBILITY_FOR_CAUSING_INCIDENT': x[21],
                'ROOT_CAUSE': x[22],
                'REPORTABLE_DOWNTIME_INCIDENT': x[23],
                'REPORT_DATE': report_date.get()
            }

            logging.info('Incident No - ' + x[9])
            yield incident_data

        except Exception as e:
            error_message = f"Error processing element: {element}\n"
            error_message += f"Exception Type: {type(e).__name__}\n"
            error_message += f"Exception Message: {e}\n"
            error_message += f"Traceback: {traceback.format_exc()}"
            logging.error(error_message)

class filter_records(beam.DoFn):
    def process(self, element, filter_condition):
        if element['IS_MAPPED'] == filter_condition:
            yield element
        else:
            return False

class MyPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--logging_mode', default='INFO')
        parser.add_value_provider_argument('--file_name_with_path', type=str)
        parser.add_argument('--config_bucket')
        parser.add_argument('--config_file_name')
        parser.add_value_provider_argument('--process_date', type=str)

if __name__ == '__main__':

    logging.getLogger().setLevel(logging.INFO)
    my_pipeline_options = PipelineOptions().view_as(MyPipelineOptions)
    report_date_time = datetime.now()

    dt_YYYY = datetime.today().strftime('%Y')
    dt_MM = datetime.today().strftime('%m')
    dt_DD = datetime.today().strftime('%d')
    p = beam.Pipeline(options=PipelineOptions())
    report_date = my_pipeline_options.process_date

    # Reading config file from GCS bucket
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(my_pipeline_options.config_bucket)
    blob = bucket.blob(my_pipeline_options.config_file_name)
    blob = blob.download_as_string().decode('utf-8')

    def func_read_config(config_file_name):
        config = configparser.ConfigParser()
        config.read_string(config_file_name)
        global project, region, temp_location, temp_dataset, staging_location
        global bq_raw_table_name, bq_da_table_name, bq_da_error_table_name
        global bq_raw_table_schema, bq_da_table_schema, bq_property_table_name

        project = config['Default']['project']
        region = config['Default']['region']
        temp_dataset = config['Default']['temp_dataset']
        temp_location = config['Default']['temp_location']
        staging_location = config['Default']['staging_location']
        bq_raw_table_name = config['Default']['bq_raw_table_name']
        bq_da_table_name = config['Default']['bq_da_table_name']
        bq_da_error_table_name = config['Default']['bq_da_error_table_name']
        bq_raw_table_schema = config['Default']['bq_raw_table_schema']
        bq_da_table_schema = config['Default']['bq_da_table_schema']
        bq_property_table_name = config['Default']['bq_property_table_name']

    func_read_config(blob)

    bq_query_raw = ''
    bq_da_table_write_method = 'WRITE_TRUNCATE'
    csv_headers = ['HSBC_REGION', 'COUNTRY_ISO', 'HORIZON_ID', 'BUILDING_NAME', 'PROPERTY_TYPE', 'PROPERTY_STATUS',
                   'PROPERTY_CRITICALITY', 'HSBC_BUSINESS_LINE', 'JLL_RESPONSIBILITY', 'INCIDENT_NUMBER',
                   'INCIDENT_CREATED_DATE', 'INCIDENT_OCCURRENCE_DATE', 'INCIDENT_CATEGORY', 'INCIDENT_TYPE',
                   'INCIDENT_SUMMARY', 'INCIDENT_STATUS', 'SYSTEM_DOWNTIME', 'BUSINESS_IMPACT', 'SEVERITY',
                   'PREVENTABLE', 'IMPACT_TO_SITE_AVAILABILITY', 'RESPONSIBILITY_FOR_CAUSING_INCIDENT', 'ROOT_CAUSE',
                   'REPORTABLE_DOWNTIME_INCIDENT']

    file_name_with_path = my_pipeline_options.file_name_with_path
    logging.info('Records deleted from Raw Table for current report date')
    logging.info('Raw Table Name - ' + bq_raw_table_name)
    logging.info('DA Table Name - ' + bq_da_table_name)
    if not bq_raw_table_name:
        logging.error('bq_raw_table_name is not set. Please check the configuration file.')
        raise ValueError('bq_raw_table_name is not set.')

    source_data = (
        p
        | "Read From GCS" >> beam.io.ReadFromText(my_pipeline_options.file_name_with_path, skip_header_lines=1,
                                                  coder=StrUtf8Coder(), escapechar=b'\"')
        | "Converting to JSON(RAW)" >> beam.ParDo(return_raw_structure(), my_pipeline_options.process_date)
    )

    result_bq_raw_save = (
        source_data
        | "Saving To BQ RAW Table" >> beam.io.WriteToBigQuery(bq_raw_table_name,
                                                              schema=bq_raw_table_schema,
                                                              write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    )

    p.run().wait_until_finish()
