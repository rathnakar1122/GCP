from dataclasses import dataclass
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging, json, configparser, os
from datetime import datetime
from google.cloud import storage,bigquery
#from options import MyPipelineOptions
from io import BytesIO
import pandas as pd
import re
from apache_beam.io.gcp.internal.clients import bigquery as bq_beam
from io import StringIO
logging.getLogger().setLevel(logging.INFO)


#os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"C:\Users\45263685\Documents\Vikas\CustomScripts\dbsr-pdtest-dev-svc-account.json"
#os.environ['http_proxy'] = "http://googleapis-dev.gcp.cloud.uk.hsbc:3128"
#os.environ['https_proxy'] = "http://googleapis-dev.gcp.cloud.uk.hsbc:3128"


class return_da_structure(beam.DoFn):
    def process(self,element,report_date):
        from datetime import datetime
        import re
        #print(datetime.now())
        if element == "":
            return False
        
        down_time = 0
        if element['SYSTEM_DOWNTIME'] != '':
            down_time = int(element['SYSTEM_DOWNTIME'])

            dict = {
             'REAL_ESTATE_PROPERTY_ID': element['HORIZON_ID']
            ,'REAL_ESTATE_PROPERTY_JLL_RESPONSIBILITY_IND': element['JLL_RESPONSIBILITY']
            ,'REAL_ESTATE_PROPERTY_INCIDENT_ID': element['INCIDENT_NUMBER']
            ,'REAL_ESTATE_PROPERTY_INCIDENT_CREATED_DATE': datetime.strptime(element['INCIDENT_CREATED_DATE'], '%m/%d/%Y').strftime('%Y-%m-%d')
            ,'REAL_ESTATE_PROPERTY_INCIDENT_OCCURENCE_DATE': datetime.strptime(element['INCIDENT_OCCURENCE_DATE'], '%m/%d/%Y').strftime('%Y-%m-%d')
            ,'REAL_ESTATE_PROPERTY_INCIDENT_CATEGORY_TEXT': element['INCIDENT_CATEGORY']
            ,'REAL_ESTATE_PROPERTY_INCIDENT_TYPE_NAME': element['INCIDENT_TYPE']
            ,'REAL_ESTATE_PROPERTY_INCIDENT_SUMMARY_TEXT': element['INCIDENT_SUMMARY'] 
            ,'REAL_ESTATE_PROPERTY_INCIDENT_STATUS_NAME': element['INCIDENT_STATUS']
            ,'REAL_ESTATE_PROPERTY_INCIDENT_SYSTEM_DOWNTIME_TIME': down_time
            ,'REAL_ESTATE_PROPERTY_INCIDENT_BUSINESS_IMPACT_IND': element['BUSINESS_IMPACT']
            ,'REAL_ESTATE_PROPERTY_INCIDENT_SEVERITY_CODE': element['SEVERITY']
            ,'REAL_ESTATE_PROPERTY_INCIDENT_PREVENTABLE_IND': element['PREVENTABLE'] 
            ,'REAL_ESTATE_PROPERTY_INCIDENT_SITE_AVAILABILITY_IMPACT_IND': element['IMPACT_TO_SITE_AVAILABILITY']
            ,'REAL_ESTATE_PROPERTY_INCIDENT_RESPONSIBLE_ORG_TEXT': element['RESPONSIBILITY_FOR_CAUSING_INCIDENT']
            ,'REAL_ESTATE_PROPERTY_INCIDENT_ROOT_CAUSE_TEXT': element['ROOT_CAUSE']
            ,'REAL_ESTATE_PROPERTY_INCIDENT_REPORTABLE_DOWNTIME_IND': element['REPORTABLE_DOWNTIME_INCIDENT']
            ,'REPORT_DATE': report_date
            #,'PROPERTY_ID_MAPPED': p_found
            }

        yield dict

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
        parser.add_argument('--file_name_with_path')
        parser.add_argument('--bq_table_name')
        parser.add_argument('--bq_raw_table_name')
        parser.add_argument('--bad_records_location')
        parser.add_argument('--jll_process_name',help='incident')
        parser.add_argument('--config_bucket')
        parser.add_argument('--config_file_name')
        parser.add_argument('--process_date')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    my_pipeline_options = PipelineOptions().view_as(MyPipelineOptions)
    
    report_date = datetime.today().strftime('%Y-%m-%d')
    p=beam.Pipeline(options=PipelineOptions())
    p1=beam.Pipeline(options=PipelineOptions())

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

        global bq_raw_table_name 
        global bq_da_table_name 
        
        global bq_da_error_table_name 
        global bq_raw_table_schema
        global bq_da_table_schema 
        global bq_property_table_name


        project                 =   config['Default']['project']
        region                  =   config['Default']['region']
        
        temp_dataset           =   config['Default']['temp_dataset']
        temp_location           =   config['Default']['temp_location']
        staging_location        =   config['Default']['staging_location']

        bq_raw_table_name         =   config['Default']['bq_raw_table_name']

        bq_da_table_name         =   config['Default']['bq_da_table_name']

        bq_da_error_table_name          =   config['Default']['bq_da_error_table_name']

        bq_raw_table_schema       =   config['Default']['bq_raw_table_schema']

        bq_da_table_schema      =   config['Default']['bq_da_table_schema']
        
        bq_property_table_name              =   config['Default']['bq_property_table_name']

    func_read_config(blob)

    bq_query_incident_raw = """ SELECT
        HORIZON_ID,JLL_RESPONSIBILITY,INCIDENT_NUMBER,
        INCIDENT_CREATED_DATE,INCIDENT_OCCURENCE_DATE,INCIDENT_CATEGORY,
        INCIDENT_TYPE,INCIDENT_SUMMARY,INCIDENT_STATUS,
        SYSTEM_DOWNTIME,BUSINESS_IMPACT,SEVERITY,
        PREVENTABLE,IMPACT_TO_SITE_AVAILABILITY,RESPONSIBILITY_FOR_CAUSING_INCIDENT,
        ROOT_CAUSE,REPORTABLE_DOWNTIME_INCIDENT,REPORT_DATE,
        CASE WHEN P.PROPERTY_DETAIL_ID IS NOT NULL THEN TRUE ELSE FALSE END AS IS_MAPPED
        FROM
        (SELECT *,ROW_NUMBER() OVER(PARTITION BY INCIDENT_NUMBER ORDER BY REPORT_DATE DESC) RN 
            FROM `""" + bq_raw_table_name +"""`) R
        LEFT JOIN (SELECT DISTINCT PROPERTY_DETAIL_ID FROM `"""+ bq_property_table_name +"""`) P ON P.PROPERTY_DETAIL_ID = R.HORIZON_ID
        WHERE REPORT_DATE = '"""+ report_date + """' AND R.RN=1; """
    valid_option=False


    if(valid_option==False):
        logging.info('jll_process_name paramenter must be incident/workorder/asset only-exiting process')
        exit(1)

    bq_query_raw = bq_query_incident_raw
    bq_da_table_write_method='WRITE_TRUNCATE'

    logging.info('Raw Table Name - ' + bq_raw_table_name )
    logging.info('DA Table Name - ' + bq_da_table_name )
    
    result_bq_raw_pull = (
        p
        |"Pulling From BQ RAW Table" >> beam.io.ReadFromBigQuery(
            query=bq_query_raw
            , use_standard_sql=True
            , method='EXPORT'
            , temp_dataset=bq_beam.DatasetReference(projectId=project
                                            ,datasetId=temp_dataset)
        )
        #|"Print Raw">>beam.Map(print)
    )
    
    result_bq = (
        result_bq_raw_pull
        |"FilteringRecordsMatchedDQRules" >> beam.ParDo(filter_records(),True)
        |"ConvertingTo_DA_Format" >> beam.ParDo(return_da_structure(),my_pipeline_options.jll_process_name,report_date)
|"Saving To BQ Data Asset" >> beam.io.WriteToBigQuery(bq_da_table_name
                                                    ,schema=bq_da_table_schema
                                                    ,write_disposition=bq_da_table_write_method)
        #|"Print matched">>beam.Map(print)
    )

    result_bq_error = (
        result_bq_raw_pull
        |"FilteringRecordsUnMatchedDQRules" >> beam.ParDo(filter_records(),False)
        |"ConvertingTo_DA_ERROR_Format" >> beam.ParDo(return_da_structure(),my_pipeline_options.jll_process_name,report_date) 
        |"Saving To BQ Data Asset ERROR" >> beam.io.WriteToBigQuery(bq_da_error_table_name
                                                    ,schema=bq_da_table_schema
                                                    ,write_disposition=bq_da_table_write_method)
        #|"Print matched">>beam.Map(print)
    )
    #for Raw insert
    p.run().wait_until_finish()