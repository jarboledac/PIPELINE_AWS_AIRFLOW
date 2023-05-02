from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator


#from crawler_operator import GlueTriggerCrawlerOperator



from datetime import datetime
import requests
import os

AWS_CONN_ID = "AWSConnection"
PATH = './data/'
BUCKET = 'airflow-pipeline-raw'
JOB_NAME = 'elt_aws_airflow'
CRAWLER_NAME = 'etl-airflow-crawler'
REGION = 'us-east-1'
FILES = ['prueba_data.csv', 'prueba_data2.csv']

def uploadData(path, bucket):
    conn = S3Hook(aws_conn_id=AWS_CONN_ID)
    client = conn.get_conn()
    files = os.listdir(path)
    for file in files:
        if file.rsplit('.',1)[1]=='csv':
            client.upload_file(
                Filename=path+file,
                Bucket=bucket,
                Key=f'{file}'
                )

def startJob(job_name):
    session = AwsGenericHook(aws_conn_id=AWS_CONN_ID)
    boto3_session = session.get_session(
        region_name='us-east-1'
        )
    client = boto3_session.client('glue')
    client.start_job_run(
        JobName=job_name,
    )



default_args = {
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 1),
}

glue_crawler_config = {
        "Name": CRAWLER_NAME
    }

with DAG(dag_id="etl_aws",
         description="etl using aws and airflow",
         schedule_interval="@once",
         tags=['ingest'],
         default_args=default_args) as dag:
    
    #t1 = PythonOperator(task_id="upload_s3",
    #                    python_callable=uploadData,
    #                    op_kwargs={
    #                        'path': PATH ,
    #                        'bucket': BUCKET,
    #                        }
    #                    )
    #s3_sensor = S3KeySensor(
    #    task_id='s3_file_check',
    #    poke_interval=10,
    #    timeout=180,
    #    soft_fail=False,
    #    retries=2,
    #    bucket_key=FILES,
    #    bucket_name=BUCKET,
    #    aws_conn_id=AWS_CONN_ID
    #    )

    #job_task = PythonOperator(task_id="run_job_a",
    #                    python_callable=startJob,
    #                    trigger_rule=TriggerRule.ALL_SUCCESS,
    #                    op_kwargs={
    #                        'job_name': JOB_NAME
    #                        }
    #                        )
    job_task1 = GlueJobOperator(
        task_id="run_job_a",
        job_name=JOB_NAME,
        script_location='s3://aws-glue-assets-834399531927-us-east-1/scripts/elt_aws_airflow.py',
        s3_bucket='aws-glue-assets-834399531927-us-east-1',
        iam_role_name='prueba-GlueRol',
        create_job_kwargs={"GlueVersion": "3.0", "NumberOfWorkers": 2, "WorkerType": "G.1X"},
        #run_job_kwargs={'--additional-python-modules':'awswrangler==2.17.0'},
        aws_conn_id=AWS_CONN_ID
    )
    job_task1
    #job_task2 = GlueJobSensor(
    #                task_id="wait_job_a",
    #                job_name = JOB_NAME,
    #                run_id=job_task.output,
    #               # region_name = REGION
#
    #)
    #run_crawler = GlueCrawlerOperator(
    #                task_id = 'run_crawler',
    #                aws_conn_id = AWS_CONN_ID,
    #                config = glue_crawler_config,
    #                region_name = REGION,
    #                poll_interval = 5,
    #)
    #t1 >> s3_sensor >>job_task  >> run_crawler



