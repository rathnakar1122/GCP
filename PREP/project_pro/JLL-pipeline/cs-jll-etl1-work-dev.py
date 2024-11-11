
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

class return_raw_structure(beam.DoFn):
    def process(self,element,process_name,report_date):
        from datetime import datetime
        import re ,csv
        #print(datetime.now())
        if element == "":
            return False
        #x = re.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)",element)
        #x = element
        reader = csv.reader([element])
        x = next(reader)
        #print(fields)

        logging.getLogger().setLevel('INFO')
        #logging.info(type(x[16]))
        if (process_name =='incident'):
            
            down_time = None

            if x[16] != None and x[16] != '':
                down_time = int(x[16])   
            dict = {
            'HSBC_REGION': x[0]
            ,'COUNTRY_ISO': x[1] 
            ,'HORIZON_ID': x[2] 
            ,'BUILDING_NAME': x[3]
            ,'PROPERTY_TYPE': x[4]
            ,'PROPERTY_STATUS': x[5] 
            ,'PROPERTY_CRITICALITY': x[6]
            ,'HSBC_BUSINESS_LINE': x[7] 
            ,'JLL_RESPONSIBILITY': x[8]
            ,'INCIDENT_NUMBER': x[9] 
            ,'INCIDENT_CREATED_DATE': x[10] 
            ,'INCIDENT_OCCURENCE_DATE': x[11] 
            ,'INCIDENT_CATEGORY': x[12] 
            ,'INCIDENT_TYPE': x[13] 
            ,'INCIDENT_SUMMARY':x[14]
            ,'INCIDENT_STATUS':x[15]
            ,'SYSTEM_DOWNTIME': down_time
            ,'BUSINESS_IMPACT':x[17]
            ,'SEVERITY':x[18]
            ,'PREVENTABLE':x[19]
            ,'IMPACT_TO_SITE_AVAILABILITY':x[20]
            ,'RESPONSIBILITY_FOR_CAUSING_INCIDENT':x[21]
            ,'ROOT_CAUSE':x[22]
            ,'REPORTABLE_DOWNTIME_INCIDENT':x[23]
            ,'REPORT_DATE': report_date.get()
            }
            logging.info('Incident No - '+ x[9])
        elif(process_name=='workorder'):
           
            dict = {
             'HSBC_REGION': x[0]
            ,'COUNTRY_ISO': x[1] 
            ,'HORIZON_ID': x[2] 
            ,'BUILDING_NAME': x[3]
            ,'PROPERTY_TYPE': x[4]
            ,'PROPERTY_CRITICALITY': x[5] 
            ,'HSBC_BUSINESS_LINE': x[6]
            ,'KPI_CATEGORY_GROUP': x[7] 
            ,'PRIORITY': x[8]
            ,'WO_NUMBER': x[9]
            ,'SPECIALITY': x[10]
            ,'WORK_ORDER_DESCRIPTION': x[11] 
            ,'CREATED_DATE': x[12]
            ,'CREATED_TIME_24': x[13] 
            ,'COMPLETED_LAST_DATE': x[14] 
            ,'COMPLETED_LAST_TIME_24': x[15]
            ,'SLA_COMPLETION_DATE': x[16]
            ,'SLA_COMPLETED_TIME_24': x[17]
            ,'COMPLETE_LAST_WITHIN_SLA': x[18]
            ,'WO_STATUS': x[19]
            ,'REASON': x[20]
            ,'REPORT_DATE': report_date.get()
            }
            logging.info('WO No - '+ x[9])
        elif(process_name=='asset'):
            dict = {
             'ASSET_CONDITION': x[0]
            ,'ASSET_CONDITION_RISK_FACTOR': x[1] 
            ,'ASSET_FAILURE_BUSINESS_IMPACT_RISK': x[2] 
            ,'ASSET_ID': x[3] 
            ,'ASSET_LIFE_EXPECTANCY': int(x[4]) if x[4] != None and x[4] !='' else None 
            ,'ASSET_MODEL': x[5]
            ,'ASSET_NAME': x[6]
            ,'HORIZON_ID': x[7]
            ,'INSTALLATION_DATE': x[8]
            ,'LEGACY_ASSET_RANK': x[9]
            ,'MANUFACTURER': x[10]
            ,'MODEL': x[11]
            ,'SERIAL_NUMBER': x[12]
            ,'CAPEX_REFERENCE_TRACKER': x[13]
            ,'ESTIMATED_LIFE_YEARS': int(x[14]) if x[14] != None and x[14] !='' else None
            ,'REPORT_DATE': report_date.get()
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
        parser.add_value_provider_argument('--file_name_with_path',type=str)
        parser.add_value_provider_argument('--process_name',help='incident, workorder, assets')
        parser.add_argument('--config_bucket')
        parser.add_argument('--config_file_name')
        parser.add_value_provider_argument('--process_date',type=str)
if __name__ == '__main__':

    logging.getLogger().setLevel(logging.INFO)
    my_pipeline_options = PipelineOptions().view_as(MyPipelineOptions)
    
    #report_date = datetime.today().strftime('%Y-%m-%d')
    report_date_time = datetime.now()
    dt_YYYY =  datetime.today().strftime('%Y')
    dt_MM =  datetime.today().strftime('%m')
    dt_DD =  datetime.today().strftime('%d')
    p=beam.Pipeline(options=PipelineOptions())
    report_date = my_pipeline_options.process_date
    project = ""
    region = ""
    temp_dataset = ""
    temp_location = ""
    staging_location = ""

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

        global bq_raw_table_name_incident , bq_raw_table_name_workorder , bq_raw_table_name_asset 
        global bq_da_table_name_incident , bq_da_table_name_workorder, bq_da_table_name_asset 
        
        global bq_da_error_table_name_incident , bq_da_error_table_name_workorder, bq_da_error_table_name_asset 
        global bq_raw_table_schema_incident ,bq_raw_table_schema_workorder,bq_raw_table_schema_asset 
        global bq_da_table_schema_incident ,bq_da_table_schema_workorder,bq_da_table_schema_asset 
        global bq_property_table_name
        global file_name_with_path_incident , file_name_with_path_workorder , file_name_with_path_asset


        project                 =   config['Default']['project']
        region                  =   config['Default']['region']
        
        temp_dataset           =   config['Default']['temp_dataset']
        temp_location           =   config['Default']['temp_location']
        staging_location        =   config['Default']['staging_location']
        bq_raw_table_name_incident          =   config['Default']['bq_raw_table_name_incident']
        bq_raw_table_name_workorder         =   config['Default']['bq_raw_table_name_workorder']
        bq_raw_table_name_asset             =   config['Default']['bq_raw_table_name_asset']
        bq_da_table_name_incident           =   config['Default']['bq_da_table_name_incident']
        bq_da_table_name_workorder          =   config['Default']['bq_da_table_name_workorder']
        bq_da_table_name_asset              =   config['Default']['bq_da_table_name_asset']

        bq_da_error_table_name_incident           =   config['Default']['bq_da_error_table_name_incident']
        bq_da_error_table_name_workorder          =   config['Default']['bq_da_error_table_name_workorder']
        bq_da_error_table_name_asset              =   config['Default']['bq_da_error_table_name_asset']
        bq_raw_table_schema_incident        =   config['Default']['bq_raw_table_schema_incident']
        bq_raw_table_schema_workorder       =   config['Default']['bq_raw_table_schema_workorder']
        bq_raw_table_schema_asset           =   config['Default']['bq_raw_table_schema_asset']
        bq_da_table_schema_incident        =   config['Default']['bq_da_table_schema_incident']
        bq_da_table_schema_workorder       =   config['Default']['bq_da_table_schema_workorder']
        bq_da_table_schema_asset           =   config['Default']['bq_da_table_schema_asset']
        
        bq_property_table_name              =   config['Default']['bq_property_table_name']

        file_name_with_path_incident              =   config['Default']['file_name_with_path_incident']
        file_name_with_path_workorder              =   config['Default']['file_name_with_path_workorder']
        file_name_with_path_asset              =   config['Default']['file_name_with_path_asset']
    func_read_config(blob)
    #print(df_ref.info())
    
    valid_option=False
    if (my_pipeline_options.process_name =='incident' 
        or my_pipeline_options.process_name=='workorder' 
        or my_pipeline_options.process_name=='asset'):
        valid_option=True
    if(valid_option==False):
        logging.info('process_name paramenter must be incident/workorder/asset only-exiting process')
        #exit(1)

    bq_query_raw = ''
    bq_raw_table_schema = ''
    bq_da_table_schema = ''
    bq_raw_table_name=''
    bq_da_table_name=''
    bq_da_error_table_name=''
    bq_da_table_write_method=''
    file_name_with_path=''
    csv_headers = ''
    if my_pipeline_options.process_name =='incident':
        bq_raw_table_name = bq_raw_table_name_incident
        bq_da_table_name = bq_da_table_name_incident
        bq_da_error_table_name = bq_da_error_table_name_incident
        bq_query_raw = ''

        bq_raw_table_schema = bq_raw_table_schema_incident
        bq_da_table_schema = bq_da_table_schema_incident
        bq_da_table_write_method='WRITE_TRUNCATE'
        file_name_with_path = file_name_with_path_incident
        csv_headers = ['HSBC_REGION','COUNTRY_ISO','HORIZON_ID','BUILDING_NAME','PROPERTY_TYPE','PROPERTY_STATUS','PROPERTY_CRITICALITY','HSBC_BUSINESS_LINE','JLL_RESPONSIBILITY','INCIDENT_NUMBER','INCIDENT_CREATED_DATE','INCIDENT_OCCURENCE_DATE','INCIDENT_CATEGORY','INCIDENT_TYPE','INCIDENT_SUMMARY','INCIDENT_STATUS','SYSTEM_DOWNTIME','BUSINESS_IMPACT','SEVERITY','PREVENTABLE','IMPACT_TO_SITE_AVAILABILITY','RESPONSIBILITY_FOR_CAUSING_INCIDENT','ROOT_CAUSE','REPORTABLE_DOWNTIME_INCIDENT']

    elif my_pipeline_options.process_name =='workorder':
        bq_raw_table_name = bq_raw_table_name_workorder
        bq_da_table_name = bq_da_table_name_workorder
        bq_da_error_table_name = bq_da_error_table_name_workorder
        bq_query_raw = ''
        bq_raw_table_schema = bq_raw_table_schema_workorder
        bq_da_table_schema = bq_da_table_schema_workorder
        bq_da_table_write_method='WRITE_TRUNCATE'
        file_name_with_path = file_name_with_path_workorder
        csv_headers = ['HSBC_REGION','COUNTRY_ISO','HORIZON_ID','BUILDING_NAME','PROPERTY_TYPE','PROPERTY_CRITICALITY','HSBC_BUSINESS_LINE','KPI_CATEGORY_GROUP','PRIORITY','WO_NUMBER','SPECIALITY','WORK_ORDER_DESCRIPTION','CREATED_DATE','CREATED_TIME_24','COMPLETED_LAST_DATE','COMPLETED_LAST_TIME_24','SLA_COMPLETION_DATE','SLA_COMPLETED_TIME_24','COMPLETE_LAST_WITHIN_SLA','WO_STATUS','REASON']

    elif my_pipeline_options.process_name =='asset':
        bq_raw_table_name = bq_raw_table_name_asset
        bq_da_table_name = bq_da_table_name_asset
        bq_da_error_table_name = bq_da_error_table_name_asset
        bq_query_raw = ''
        bq_raw_table_schema = bq_raw_table_schema_asset
        bq_da_table_schema = bq_da_table_schema_asset
        bq_da_table_write_method='WRITE_APPEND'
        file_name_with_path = file_name_with_path_asset
        csv_headers = ['AssetCondition','AssetConditionRiskFactor','AssetFailureBusinessImpactRisk','AssetID','AssetLifeExpectancy','AssetModel','AssetName','HorizonID','InstallationDate','AssetRank','Manufacturer','Model','SerialNumber','CapexReferenceTracker','EstimatedLifeyears']
    #file_name_with_path = file_name_with_path.replace('YYYY',dt_YYYY).replace('MM',dt_MM).replace('DD',dt_DD)
    file_name_with_path = my_pipeline_options.file_name_with_path
    #Delete FROM Raw Table for current date
    #DEL_QUERY = "DELETE FROM `"+ bq_raw_table_name +"` WHERE REPORT_DATE='"+ report_date  +"' "
    #client = bigquery.Client()
    #client.query_and_wait(DEL_QUERY)
    #print('Records deleted from Raw Table')
    logging.info('Records deleted from Raw Table for current report date')
    #Read From Bigquery reference data
    #print("table schema")
    #print(bq_raw_table_schema)
    #print("bq query raw")
    #print(bq_query_raw)
    #print(bq_raw_table_name)
    #print("bq DA-----------------------------------------")
    #print(bq_da_table_name)
    #print(bq_da_table_schema)
    logging.info('Raw Table Name - ' + bq_raw_table_name )
    logging.info('DA Table Name - ' + bq_da_table_name )
    #Pipeline 1st started
    source_data = (
        p
        #|beam.Create([None])
        |"Read From GCS" >> beam.io.ReadFromText(my_pipeline_options.file_name_with_path,skip_header_lines=1,coder=StrUtf8Coder(),escapechar=b'\"')
        #|"Read From GCS" >> beam.io.ReadFromCsv(my_pipeline_options.file_name_with_path,encoding='utf-8',encoding_errors='ignore',header=0,names=csv_headers)
        #|"Read From GCS" >> beam.ParDo(read_from_csv(),my_pipeline_options.file_name_with_path,csv_headers)
        #|"DecodingUTF-8" >> beam.Map(lambda x : x.decode('utf-8','ignore'))
        |"Converting to JSON(RAW)" >> beam.ParDo(return_raw_structure(),my_pipeline_options.process_name,my_pipeline_options.process_date)
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