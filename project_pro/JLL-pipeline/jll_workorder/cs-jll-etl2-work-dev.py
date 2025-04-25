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
from apache_beam.options.value_provider import StaticValueProvider

# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/45375853/Documents/cs_data_plotform/service_account/dbsr-pdtest-dev-svc-account.json' 
# os.environ['http_proxy'] = "http://googleapis-dev.gcp.cloud.uk.hsbc:3128" 
# os.environ['https_proxy'] = "http://googleapis-dev.gcp.cloud.uk.hsbc:3128" 


class read_from_bq_table(beam.DoFn):
    def process(self,element,bq_query,report_date):
        from datetime import datetime
        import re
        print(bq_query)
    
        qry = bq_query.replace('$$report_date$$',str(report_date.get()) )


        from apache_beam.io import ReadFromBigQueryRequest
        yield ReadFromBigQueryRequest(query=qry  , use_standard_sql=True  )

class return_da_structure(beam.DoFn):
    def process(self,element,report_date,table_type):
            from datetime import datetime
            import re        
            if element == "":
                return False
            down_time = 0
            if element['SYSTEM_DOWNTIME'] != '':
                down_time = int(element['SYSTEM_DOWNTIME']) if element['SYSTEM_DOWNTIME'] != None else None 
                try:
                    down_time = int(element['SYSTEM_DOWNTIME'])
                except ValueError as e:
                    logging.error(f"Error converting SYSTEM_DOWNTIME: {e}")               
   
            dict = {
                'REAL_ESTATE_PROPERTY_ID': element['HORIZON_ID']             
                ,'REAL_ESTATE_PROPERTY_WORKORDER_KPI_CATEGORY_CODE': element['KPI_CATEGORY_GROUP'] 
                ,'REAL_ESTATE_PROPERTY_WORKORDER_PRIORITY_CODE': element['PRIORITY']
                ,'REAL_ESTATE_PROPERTY_WORKORDER_ID': element['WO_NUMBER']
                ,'REAL_ESTATE_PROPERTY_WORKORDER_SPECIALITY_TYPE_NAME': element['SPECIALITY'] 
                ,'REAL_ESTATE_PROPERTY_WORKORDER_DESC': element['WORK_ORDER_DESCRIPTION']
                ,'REAL_ESTATE_PROPERTY_WORKORDER_CREATED_DATE': datetime.strptime(element['CREATED_DATE'], '%m/%d/%Y').strftime('%Y-%m-%d') if element['CREATED_DATE'] != None and element['CREATED_DATE'] !='' else None
                ,'REAL_ESTATE_PROPERTY_WORKORDER_CREATED_TIME': element['CREATED_TIME_24']
                ,'REAL_ESTATE_PROPERTY_WORKORDER_COMPLETED_DATE': datetime.strptime(element['COMPLETED_LAST_DATE'], '%m/%d/%Y').strftime('%Y-%m-%d') if element['COMPLETED_LAST_DATE'] != None and element['COMPLETED_LAST_DATE'] != '' else None
                ,'REAL_ESTATE_PROPERTY_WORKORDER_COMPLETED_TIME': element['COMPLETED_LAST_TIME_24']            
                ,'REAL_ESTATE_PROPERTY_WORKORDER_SLA_COMPLETED_DATE': datetime.strptime(element['SLA_COMPLETION_DATE'], '%m/%d/%Y').strftime('%Y-%m-%d') if element['SLA_COMPLETION_DATE'] != None and element['SLA_COMPLETION_DATE'] !='' else None
                ,'REAL_ESTATE_PROPERTY_WORKORDER_SLA_COMPLETED_TIME': element['SLA_COMPLETED_TIME_24']             
                ,'REAL_ESTATE_PROPERTY_WORKORDER_SLA_IND': element['COMPLETE_LAST_WITHIN_SLA'] 
                ,'REAL_ESTATE_PROPERTY_WORKORDER_STATUS_NAME': element['WO_STATUS'] 
                ,'REAL_ESTATE_PROPERTY_WORKORDER_REASON_DESC': element['REASON']
                ,'REPORT_DATE': (report_date.get())
            }
            
            if (table_type =='error'):
                dict['ERROR_REASON'] = element['ERROR_REASON']          

            yield dict

            # except Exception as e:
            #     error_message = f"Error processing element {element}. SYSTEM_DOWNTIME: {element.get('SYSTEM_DOWNTIME', 'Missing')}, Error: {str(e)}"
            #     logging.error(error_message)

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
        parser.add_argument('--process_name',help=' workorder ')
        parser.add_argument('--config_bucket')
        parser.add_argument('--config_file_name')
        parser.add_value_provider_argument('--process_date',type=str)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    my_pipeline_options = PipelineOptions().view_as(MyPipelineOptions)    
    p=beam.Pipeline(options=PipelineOptions())    
    report_date = my_pipeline_options.process_date
    
    #Reading config file form GCS bucket
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(my_pipeline_options.config_bucket)
    blob = bucket.blob(my_pipeline_options.config_file_name)
    blob = blob.download_as_string()
    blob = blob.decode('utf-8')
    
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
        global bq_da_error_table_schema
        global bq_property_table_name


        project                 =   config['Default']['project']
        region                  =   config['Default']['region']
        
        temp_dataset           =   config['Default']['temp_dataset']
        temp_location           =   config['Default']['temp_location']
        staging_location        =   config['Default']['staging_location']
        bq_raw_table_name        =   config['Default']['bq_raw_table_name']
        bq_da_table_name        =   config['Default']['bq_da_table_name']
        bq_da_error_table_name         =   config['Default']['bq_da_error_table_name']
        bq_raw_table_schema     =   config['Default']['bq_raw_table_schema']
        bq_da_table_schema    =   config['Default']['bq_da_table_schema']
        bq_da_error_table_schema      =   config['Default']['bq_da_error_table_schema']        
        bq_property_table_name              =   config['Default']['bq_property_table_name']
    func_read_config(blob)
 
    bq_query_workorder_raw = """ SELECT
        HSBC_REGION,COUNTRY_ISO,HORIZON_ID
        ,BUILDING_NAME,PROPERTY_TYPE,PROPERTY_CRITICALITY
        ,HSBC_BUSINESS_LINE,KPI_CATEGORY_GROUP,PRIORITY
        ,WO_NUMBER,SPECIALITY,WORK_ORDER_DESCRIPTION
        ,CREATED_DATE,CREATED_TIME_24,COMPLETED_LAST_DATE,COMPLETED_LAST_TIME_24
        ,SLA_COMPLETION_DATE,SLA_COMPLETED_TIME_24
        ,COMPLETE_LAST_WITHIN_SLA,WO_STATUS,REASON
        ,CASE WHEN P.PROPERTY_DETAIL_ID IS NULL THEN FALSE
             WHEN R.KPI_CATEGORY_GROUP IS NULL THEN FALSE
             WHEN R.PRIORITY IS NULL THEN FALSE
             WHEN R.WO_NUMBER IS NULL THEN FALSE
             WHEN R.SPECIALITY IS NULL THEN FALSE
             WHEN R.WORK_ORDER_DESCRIPTION IS NULL THEN FALSE
             WHEN R.CREATED_DATE IS NULL THEN FALSE
             WHEN R.CREATED_TIME_24 IS NULL THEN FALSE
 FALSE
             WHEN R.SLA_COMPLETION_DATE IS NULL THEN FALSE
             WHEN R.SLA_COMPLETED_TIME_24 IS NULL THEN FALSE
             WHEN R.WO_STATUS IS NULL THEN FALSE
             ELSE TRUE END AS IS_MAPPED
         ,CONCAT('ERROR '
    ,CASE WHEN P.PROPERTY_DETAIL_ID IS NULL THEN 'Rule-1.Property not found ' ELSE '' END
        ,CASE WHEN R.KPI_CATEGORY_GROUP IS NULL THEN 'Rule-2.KPI_CATEGORY_GROUP COLUMN IS NULL ' ELSE '' END
        ,CASE WHEN R.PRIORITY IS NULL THEN  'Rule-3.PRIORITY COLUMN IS NULL ' ELSE '' END
        ,CASE WHEN R.WO_NUMBER IS NULL THEN  'Rule-4.WO_NUMBER COLUMN IS NULL ' ELSE '' END
        ,CASE WHEN R.SPECIALITY IS NULL THEN  'Rule-5.SPECIALITY COLUMN IS NULL ' ELSE '' END
        ,CASE WHEN R.WORK_ORDER_DESCRIPTION IS NULL THEN  'Rule-6.WORK_ORDER_DESCRIPTION IS NULL ' ELSE '' END
        ,CASE WHEN R.CREATED_DATE IS NULL THEN  'Rule-7.CREATED_DATE COLUMN IS NULL ' ELSE '' END
        ,CASE WHEN R.CREATED_TIME_24 IS NULL THEN  'Rule-8.CREATED_TIME_24 COLUMN IS NULL ' ELSE '' END
        ,CASE WHEN R.SLA_COMPLETION_DATE IS NULL THEN  'Rule-9.SLA_COMPLETION_DATE COLUMN IS NULL ' ELSE '' END
        ,CASE WHEN R.SLA_COMPLETED_TIME_24 IS NULL THEN  'Rule-10.SLA_COMPLETED_TIME_24 COLUMN IS NULL ' ELSE '' END
        ,CASE WHEN R.WO_STATUS IS NULL THEN  'Rule-11.WO_STATUS COLUMN IS NULL' ELSE '' END )  ERROR_REASON
        FROM   
        (SELECT *,ROW_NUMBER() OVER(PARTITION BY WO_NUMBER ORDER BY REPORT_DATE DESC) RN 
            FROM `""" + bq_raw_table_name +"""`) R
        LEFT JOIN (SELECT DISTINCT PROPERTY_DETAIL_ID FROM `"""+ bq_property_table_name +"""`) P ON P.PROPERTY_DETAIL_ID = R.HORIZON_ID
        WHERE R.RN=1; """

    valid_option=False
    
    bq_query_raw = bq_query_workorder_raw
    bq_da_table_write_method='WRITE_TRUNCATE'  

    print("##############################################bq query raw")
    print(bq_query_raw)

    logging.info('Raw Table Name - ' + bq_raw_table_name )
    logging.info('DA Table Name - ' + bq_da_table_name )
    logging.info('PULL Query - ' + bq_query_raw )
    print(bq_query_raw)

    logging.info(report_date )
    result_bq_raw_pull = (
        p
        |beam.Create([None])
        |"ReadFromBQRawPARDO" >> beam.ParDo(read_from_bq_table(),bq_query_raw,my_pipeline_options.process_date)
        |"ReadAllFromRequest" >> beam.io.ReadAllFromBigQuery(temp_dataset=bq_beam.DatasetReference(projectId=project
                                            ,datasetId=temp_dataset))
       #|"Print Raw">>beam.Map(print)
    )

    result_bq = (
        result_bq_raw_pull
        |"FilteringRecordsMatchedDQRules" >> beam.ParDo(filter_records(),True)
        |"ConvertingTo_DA_Format" >> beam.ParDo(return_da_structure(),my_pipeline_options.process_date,'da')
        #|"Print matched">>beam.Map(print)
        |"Saving To BQ Data Asset" >> beam.io.WriteToBigQuery(bq_da_table_name
                                                    ,schema=bq_da_table_schema
                                                    ,write_disposition=bq_da_table_write_method)
        #|"Print matched">>beam.Map(print)
    )

    result_bq_error = (
        result_bq_raw_pull
        |"FilteringRecordsUnMatchedDQRules" >> beam.ParDo(filter_records(),False)
        |"ConvertingTo_DA_ERROR_Format" >> beam.ParDo(return_da_structure(),my_pipeline_options.process_date,'error')
        |"Saving To BQ Data Asset ERROR" >> beam.io.WriteToBigQuery(bq_da_error_table_name
                                                    ,schema=bq_da_error_table_schema
                                                    ,write_disposition=bq_da_table_write_method)
        #|"Print matched">>beam.Map(print)
    )
    #for Raw insert
    p.run().wait_until_finish()