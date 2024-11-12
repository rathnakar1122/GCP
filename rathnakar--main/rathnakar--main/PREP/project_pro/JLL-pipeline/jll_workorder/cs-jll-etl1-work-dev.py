
from dataclasses import dataclass
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging, json, configparser, os
from datetime import datetime
from google.cloud import storage,bigquery
#from options import MyPipelineOptions
from io import BytesIO
import pandas as pd
#import re
from apache_beam.io.gcp.internal.clients import bigquery as bq_beam
from io import StringIO
logging.getLogger().setLevel(logging.INFO)
from apache_beam.options.value_provider import StaticValueProvider
import traceback
import datetime 
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/build/workspace/hsbc-11359979-dbsrefinery-dev/TemplateCreation-dev/dbsr-pdtest-dev-svc-account.json"

class StrUtf8Coder(beam.coders.coders.Coder):
    """A coder used for reading and writing strings as UTF-8."""
    def encode(self, value):
        return value.encode('utf-8','ignore')
    def decode(self, value):
        return value.decode('utf-8','ignore')
    def is_deterministic(self):
        return True

class read_from_csv(beam.DoFn):
    def process(self,element,file_name,csv_headers):
        #from datetime import datetime
        #import re
        print(file_name.get())
        df =  beam.io.ReadFromCsv(file_name.get(),encoding='utf-8',encoding_errors='ignore',header=0,names=csv_headers)
        print('=====================================')
        print(df)
        print('=====================================')
        yield beam.io.ReadFromCsv(file_name.get(),encoding='utf-8',encoding_errors='ignore',header=0,names=csv_headers,converters='dict')

class ReturnRawStructure(beam.DoFn):
    def process(self, element, report_date):
        from datetime import datetime
        import re, csv
        try:
            if element == "":
                return
            
            reader = csv.reader([element])
            x = next(reader)

            logging.getLogger().setLevel(logging.INFO)
            down_time = None
            
            if x[16] is not None and x[16] != '':            
                down_time = int(x[16])
            logging.warning(f"Invalid downtime '{x[16]}'; expected an integer")

            data_dict = {
                'HSBC_REGION': x[0],
                'COUNTRY_ISO': x[1],
                'HORIZON_ID': x[2],
                'BUILDING_NAME': x[3],
                'PROPERTY_TYPE': x[4],
                'PROPERTY_CRITICALITY': x[5],
                'HSBC_BUSINESS_LINE': x[6],
                'KPI_CATEGORY_GROUP': x[7],
                'PRIORITY': x[8],
                'WO_NUMBER': x[9],
                'SPECIALITY': x[10],
                'WORK_ORDER_DESCRIPTION': x[11],
                'CREATED_DATE': x[12],
                'CREATED_TIME_24': x[13],
                'COMPLETED_LAST_DATE': x[14],
                'COMPLETED_LAST_TIME_24': x[15],
                'SLA_COMPLETION_DATE': x[16],
                'SLA_COMPLETED_TIME_24': x[17],
                'COMPLETE_LAST_WITHIN_SLA': x[18],
                'WO_STATUS': x[19],
                'REASON': x[20],
                'REPORT_DATE': report_date.get()
            }
            logging.info('WO No - ' + x[9])
            yield data_dict

        except Exception as e:
            error_message = (
                f"Error processing element: {element}\n"
                f"Exception Type: {type(e).__name__}\n"
                f"Exception Message: {e}\n"
                f"Traceback: {traceback.format_exc()}"
            )
            logging.error(error_message)
            print(error_message)


class filter_records(beam.DoFn):
    def process(self,element,filter_condition):
        if (element['IS_MAPPED']==filter_condition):
            yield element
        else:
            return False
class MyPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--logging_mode',default='INFO')
        parser.add_value_provider_argument('--file_name_with_path',type=str)
        parser.add_argument('--config_bucket')
        parser.add_argument('--config_file_name')
        parser.add_value_provider_argument('--process_date',type=str)
if __name__ == '__main__':

    logging.getLogger().setLevel(logging.INFO)
    my_pipeline_options = PipelineOptions().view_as(MyPipelineOptions)
    
    report_date_time = datetime.now()
    dt_YYYY =  datetime.today().strftime('%Y')
    dt_MM =  datetime.today().strftime('%m')
    dt_DD =  datetime.today().strftime('%d')
    p=beam.Pipeline(options=PipelineOptions())
    report_date = my_pipeline_options.process_date
    

    #Reading config file form GCS bucket
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(my_pipeline_options.config_bucket)
    blob = bucket.blob(my_pipeline_options.config_file_name)
    blob = blob.download_as_string()
    blob = blob.decode('utf-8')
    #blob = StringIO(blob)
    
    #print(blob)
    #exit(1)
    def func_read_config(config_file_name):
        config = configparser.ConfigParser()
        config.read_string(config_file_name)
        global project , region
        global temp_location ,temp_dataset , staging_location

        global bq_raw_table_name ,bq_da_table_name,bq_da_error_table_name
        global bq_raw_table_schema, bq_da_table_schema



        project                 =   config['Default']['project']
        region                  =   config['Default']['region']
        
        temp_dataset           =   config['Default']['temp_dataset']
        temp_location           =   config['Default']['temp_location']
        staging_location        =   config['Default']['staging_location']
        bq_raw_table_name        =   config['Default']['bq_raw_table_name']
        bq_da_table_name          =   config['Default']['bq_da_table_name']
        bq_da_error_table_name        =   config['Default']['bq_da_error_table_name']
        bq_raw_table_schema     =   config['Default']['bq_raw_table_schema']
        bq_da_table_schema      =   config['Default']['bq_da_table_schema']
        

    func_read_config(blob)
    #print(df_ref.info())
    
    valid_option=False
    
    csv_headers = ''
    
    my_pipeline_options.process_name =='workorder'
    bq_query_raw = ''   
    bq_da_table_write_method='WRITE_TRUNCATE'
    csv_headers = ['HSBC_REGION','COUNTRY_ISO','HORIZON_ID','BUILDING_NAME','PROPERTY_TYPE','PROPERTY_CRITICALITY','HSBC_BUSINESS_LINE','KPI_CATEGORY_GROUP','PRIORITY','WO_NUMBER','SPECIALITY','WORK_ORDER_DESCRIPTION','CREATED_DATE','CREATED_TIME_24','COMPLETED_LAST_DATE','COMPLETED_LAST_TIME_24','SLA_COMPLETION_DATE','SLA_COMPLETED_TIME_24','COMPLETE_LAST_WITHIN_SLA','WO_STATUS','REASON']
    logging.info('Records deleted from Raw Table for current report date')
  
    logging.info('Raw Table Name - ' + bq_raw_table_name )
    logging.info('DA Table Name - ' + bq_da_table_name )
    #Pipeline 1st started
    source_data = (
        p
        #|beam.Create([None])
        |"Read From GCS" >> beam.io.ReadFromText(my_pipeline_options.file_name_with_path,skip_header_lines=1,coder=StrUtf8Coder(),escapechar=b'\"')
        |"Converting to JSON(RAW)" >> beam.ParDo(ReturnRawStructure(),my_pipeline_options.process_name,my_pipeline_options.process_date)
        #|beam.Map(print)
    )

    result_bq_raw_save = (
        source_data
        |"Saving To BQ RAW Table" >> beam.io.WriteToBigQuery(bq_raw_table_name
                                                    ,schema=bq_raw_table_schema
                                                    ,write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
        #|beam.Create([{'DataSaved':'True'}])
        #|"Print Raw">>beam.Map(print)
    )
    
    #for Raw insert
    p.run().wait_until_finish()