from dataclasses import dataclass
import dataclass
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging, json, configparser, os
from datetime import datetime
from google.cloud import storage
#from options import MyPipelineOptionsBqRead
from apache_beam.io.gcp.internal.clients import bigquery as bq_beam
logging.getLogger().setLevel(logging.INFO)
from apache_beam.io.textio import WriteToCsv
from apache_beam.io import fileio
#from apache_beam.dataframe.io import read_csv


#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/build/workspace/hsbc-11359979-dbsrefinery-dev/TemplateCreation-dev/dbsr-pdtest-dev-svc-account.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/45375853/Documents/cs_data_plotform/service_account/dbsr-pdtest-dev-svc-account.json' 
os.environ['http_proxy'] = "http://googleapis-dev.gcp.cloud.uk.hsbc:3128"
os.environ['https_proxy'] = "http://googleapis-dev.gcp.cloud.uk.hsbc:3128"

class read_from_bq_table(beam.DoFn):
    def process(self,element,bq_query,process_name,report_date):
        from datetime import datetime
        qry = bq_query.replace('$$report_date$$',str(report_date.get()) )
        logging.getLogger().setLevel(logging.INFO)
        logging.info(qry)
        from apache_beam.io import ReadFromBigQueryRequest
        yield ReadFromBigQueryRequest(query=qry
            , use_standard_sql=True
        )
class MyPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--logging_mode',default='INFO')
        parser.add_argument('--process_name',help='floor, floor_space, employee')
        parser.add_argument('--config_bucket')
        parser.add_argument('--config_file_name')
        parser.add_value_provider_argument('--out_file_name_with_path')
        parser.add_value_provider_argument('--process_date')

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    my_pipeline_options = PipelineOptions().view_as(MyPipelineOptions)
    report_date = my_pipeline_options.process_date #datetime.today().strftime('%Y-%m-%d')
    p=beam.Pipeline(options=PipelineOptions())
    valid_option=False
    if (my_pipeline_options.process_name =='floor'
        or my_pipeline_options.process_name=='floor-space'
        or my_pipeline_options.process_name=='employee'):
        valid_option=True
    if(valid_option==False):
        logging.info('process_name paramenter must be floor/floor-space/employee only - exiting process')
        exit(1)

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

        global bq_table_name_floor , bq_query_floor , csv_header_floor
        global bq_table_name_floor_space , bq_query_floor_space , csv_header_floor_space
        global bq_table_name_employee , bq_query_employee , csv_header_employee

        project                 =   config['Default']['project']
        region                  =   config['Default']['region']
        temp_dataset           =   config['Default']['temp_dataset']
        temp_location           =   config['Default']['temp_location']
        staging_location        =   config['Default']['staging_location']
        bq_table_name_floor             =   config['Default']['bq_table_name_floor']
        bq_query_floor              =   config['Default']['bq_query_floor']
        csv_header_floor                  =   config['Default']['csv_header_floor']
        bq_table_name_floor_space       =   config['Default']['bq_table_name_floor_space']
        bq_query_floor_space            =   config['Default']['bq_query_floor_space']
        csv_header_floor_space          =   config['Default']['csv_header_floor_space']

        bq_table_name_employee          =   config['Default']['bq_table_name_employee']
        bq_query_employee               =   config['Default']['bq_query_employee']
        csv_header_employee             =   config['Default']['csv_header_employee']

    func_read_config(blob)
    bq_table_name=''
    bq_query=''
    csv_header=''
    if my_pipeline_options.process_name =='floor':
        bq_table_name = bq_table_name_floor
        bq_query = bq_query_floor
        csv_header = csv_header_floor
    elif  my_pipeline_options.process_name =='floor-space':
        bq_table_name = bq_table_name_floor_space
        bq_query = bq_query_floor_space
        csv_header = csv_header_floor_space
    elif  my_pipeline_options.process_name =='employee':
        bq_table_name = bq_table_name_employee
        bq_query = bq_query_employee
        csv_header = csv_header_employee
    logging.info('bq table name ' + bq_table_name)
    logging.info('bq query ' + bq_query)

    res_bq = (
        p
        |beam.Create([None])
        |"ReadFromBQRawPARDO" >> beam.ParDo(read_from_bq_table(),bq_query,my_pipeline_options.process_name,my_pipeline_options.process_date)
        |"ReadAllFromRequest" >> beam.io.ReadAllFromBigQuery(temp_dataset=bq_beam.DatasetReference(projectId=project
                                            ,datasetId=temp_dataset))
        #|beam.Map(print)
            )

    def write_to_csv():
        res_gcs = (
            res_bq
            |"ConvertingToValues" >> beam.Map(lambda x: list(x.values()))           
            |"JoiningColumns" >> beam.Map(lambda row: ','.join([''+ str(column) +'' for column in row]))
            #|beam.Map(print)
 
            |"SavingToGCS" >> beam.io.textio.WriteToText(my_pipeline_options.out_file_name_with_path
            ,file_name_suffix='.csv'
            ,header=csv_header,num_shards=0, shard_name_template='')
            )

    write_to_csv()

    #for Raw insert
    p.run().wait_until_finish()


