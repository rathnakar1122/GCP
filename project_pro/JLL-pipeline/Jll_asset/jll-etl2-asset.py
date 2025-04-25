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
    def process(self,element,jll_process_name,report_date):
        from datetime import datetime
        import re
        #print(datetime.now())
        if element == "":
            return False
        
        if (jll_process_name =='incident'):
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

        elif(jll_process_name=='workorder'):
            
            dict = {
            'REAL_ESTATE_PROPERTY_ID': element['HORIZON_ID'] 
            
            ,'REAL_ESTATE_PROPERTY_WORKORDER_KPI_CATEGORY_CODE': element['KPI_CATEGORY_GROUP'] 
            ,'REAL_ESTATE_PROPERTY_WORKORDER_PRIORITY_CODE': element['PRIORITY']
            ,'REAL_ESTATE_PROPERTY_WORKORDER_ID': element['WO_NUMBER']

            ,'REAL_ESTATE_PROPERTY_WORKORDER_SPECIALITY_TYPE_NAME': element['SPECIALITY'] 
            ,'REAL_ESTATE_PROPERTY_WORKORDER_DESC': element['WORK_ORDER_DESCRIPTION']

            ,'REAL_ESTATE_PROPERTY_WORKORDER_CREATED_DATE': datetime.strptime(element['CREATED_DATE'], '%m/%d/%Y').strftime('%Y-%m-%d') 
            ,'REAL_ESTATE_PROPERTY_WORKORDER_CREATED_TIME': element['CREATED_TIME_24'] 

            ,'REAL_ESTATE_PROPERTY_WORKORDER_COMPLETED_DATE': datetime.strptime(element['COMPLETED_LAST_DATE'], '%m/%d/%Y').strftime('%Y-%m-%d') 
            ,'REAL_ESTATE_PROPERTY_WORKORDER_COMPLETED_TIME': element['COMPLETED_LAST_TIME_24']
            
            ,'REAL_ESTATE_PROPERTY_WORKORDER_SLA_COMPLETED_DATE': datetime.strptime(element['SLA_COMPLETION_DATE'], '%m/%d/%Y').strftime('%Y-%m-%d') 
            ,'REAL_ESTATE_PROPERTY_WORKORDER_SLA_COMPLETED_TIME': element['SLA_COMPLETED_TIME_24']
            
            ,'REAL_ESTATE_PROPERTY_WORKORDER_SLA_IND': element['COMPLETE_LAST_WITHIN_SLA'] 
            ,'REAL_ESTATE_PROPERTY_WORKORDER_STATUS_NAME': element['WO_STATUS'] 

            ,'REAL_ESTATE_PROPERTY_WORKORDER_REASON_DESC': element['REASON']

            ,'REPORT_DATE': report_date
            }
        elif(jll_process_name=='asset'):
            
            dict = {
             'REAL_ESTATE_COMPONENT_ASSET_ID': element['ASSET_ID']
            ,'REAL_ESTATE_PROPERTY_ID': element['HORIZON_ID'] 
            ,'REAL_ESTATE_COMPONENT_ASSET_CONDITION_TEXT': element['ASSET_CONDITION'] 
            ,'REAL_ESTATE_COMPONENT_EXPECTED_LIFE_YEAR_NUM': element['ESTIMATED_LIFE_YEARS']
            ,'REAL_ESTATE_COMPONENT_ASSET_INSTALLATION_DATE': datetime.strptime(element['INSTALLATION_DATE'], '%m/%d/%Y').strftime('%Y-%m-%d') 
            ,'REAL_ESTATE_COMPONENT_ASSET_MODEL_DESC': element['ASSET_MODEL']
            ,'REAL_ESTATE_COMPONENT_ASSET_RANK_DESC': element['LEGACY_ASSET_RANK']
            ,'REAL_ESTATE_COMPONENT_ASSET_BATTERY_RACK_LIFE_YEAR_NUM': element['ASSET_LIFE_EXPECTANCY']
            ,'REAL_ESTATE_COMPONENT_ASSET_FAILURE_BUSINESS_IMPACT_CODE': element['ASSET_FAILURE_BUSINESS_IMPACT_RISK']
            ,'REPORT_DATE': report_date
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
        parser.add_argument('--jll_process_name',help='incident, workorder, assets')
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
            FROM `""" + bq_raw_table_name_incident +"""`) R
        LEFT JOIN (SELECT DISTINCT PROPERTY_DETAIL_ID FROM `"""+ bq_property_table_name +"""`) P ON P.PROPERTY_DETAIL_ID = R.HORIZON_ID
        WHERE REPORT_DATE = '"""+ report_date + """' AND R.RN=1; """

    bq_query_workorder_raw = """ SELECT
        HSBC_REGION,COUNTRY_ISO,HORIZON_ID
        ,BUILDING_NAME,PROPERTY_TYPE,PROPERTY_CRITICALITY
        ,HSBC_BUSINESS_LINE,KPI_CATEGORY_GROUP,PRIORITY
        ,WO_NUMBER,SPECIALITY,WORK_ORDER_DESCRIPTION
        ,CREATED_DATE,CREATED_TIME_24,COMPLETED_LAST_DATE,COMPLETED_LAST_TIME_24
        ,SLA_COMPLETION_DATE,SLA_COMPLETED_TIME_24
        ,COMPLETE_LAST_WITHIN_SLA,WO_STATUS,REASON
        ,CASE WHEN P.PROPERTY_DETAIL_ID IS NOT NULL THEN TRUE ELSE FALSE END AS IS_MAPPED
        FROM   
        (SELECT *,ROW_NUMBER() OVER(PARTITION BY WO_NUMBER ORDER BY REPORT_DATE DESC) RN 
            FROM `""" + bq_raw_table_name_workorder +"""`) R
        LEFT JOIN (SELECT DISTINCT PROPERTY_DETAIL_ID FROM `"""+ bq_property_table_name +"""`) P ON P.PROPERTY_DETAIL_ID = R.HORIZON_ID
        WHERE REPORT_DATE = '"""+ report_date + """' AND R.RN=1; """

    bq_query_asset_raw = """ SELECT
        ASSET_CONDITION,ASSET_CONDITION_RISK_FACTOR,ASSET_FAILURE_BUSINESS_IMPACT_RISK  
        ,ASSET_ID,ASSET_LIFE_EXPECTANCY,ASSET_MODEL 
        ,ASSET_NAME ,HORIZON_ID,INSTALLATION_DATE   
        ,LEGACY_ASSET_RANK,MANUFACTURER,MODEL   
        ,SERIAL_NUMBER,CAPEX_REFERENCE_TRACKER,ESTIMATED_LIFE_YEARS
        ,CASE WHEN P.PROPERTY_DETAIL_ID IS NOT NULL THEN TRUE ELSE FALSE END AS IS_MAPPED
        FROM
        `""" + bq_raw_table_name_asset +"""` R
        LEFT JOIN (SELECT DISTINCT PROPERTY_DETAIL_ID FROM `"""+ bq_property_table_name +"""`) P ON P.PROPERTY_DETAIL_ID = R.HORIZON_ID
        WHERE REPORT_DATE = '"""+ report_date + """' ; """
    #print(df_ref.info())

    valid_option=False

    if (my_pipeline_options.jll_process_name =='incident' 
        or my_pipeline_options.jll_process_name=='workorder' 
        or my_pipeline_options.jll_process_name=='asset'):
        valid_option=True

    if(valid_option==False):
        logging.info('jll_process_name paramenter must be incident/workorder/asset only-exiting process')
        exit(1)

    bq_query_raw = ''

    bq_raw_table_schema = ''
    bq_da_table_schema = ''

    bq_raw_table_name=''
    bq_da_table_name=''
    bq_da_error_table_name=''

    bq_da_table_write_method=''
    if my_pipeline_options.jll_process_name =='incident':
        bq_raw_table_name = bq_raw_table_name_incident
        bq_da_table_name = bq_da_table_name_incident
        bq_da_error_table_name = bq_da_error_table_name_incident

        bq_query_raw = bq_query_incident_raw

        bq_raw_table_schema = bq_raw_table_schema_incident
        bq_da_table_schema = bq_da_table_schema_incident

        bq_da_table_write_method='WRITE_TRUNCATE'
    elif my_pipeline_options.jll_process_name =='workorder':
        bq_raw_table_name = bq_raw_table_name_workorder
        bq_da_table_name = bq_da_table_name_workorder
        bq_da_error_table_name = bq_da_error_table_name_workorder

        bq_query_raw = bq_query_workorder_raw

        bq_raw_table_schema = bq_raw_table_schema_workorder
        bq_da_table_schema = bq_da_table_schema_workorder

        bq_da_table_write_method='WRITE_TRUNCATE'
    elif my_pipeline_options.jll_process_name =='asset':
        bq_raw_table_name = bq_raw_table_name_asset
        bq_da_table_name = bq_da_table_name_asset
        bq_da_error_table_name = bq_da_error_table_name_asset

        bq_query_raw = bq_query_asset_raw

        bq_raw_table_schema = bq_raw_table_schema_asset
        bq_da_table_schema = bq_da_table_schema_asset

        bq_da_table_write_method='WRITE_APPEND'

    #deleting records from DA assets
    if my_pipeline_options.jll_process_name =='asset':
        DEL_QUERY_DA = "DELETE FROM `"+ bq_da_table_name +"` WHERE REPORT_DATE='"+ report_date  +"' "
        client = bigquery.Client()
        client.query_and_wait(DEL_QUERY_DA)
        logging.info('Records deleted from DA Table for current report date')

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