from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import BaseOperator
#from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.amazon.aws.operators.s3 import S3Hook #S3ListOperator 
from main_etl import airpol_etl
#from botocore.client import Config
import credentials


default_args={
        "depends_on_past": False,
        "start_date": datetime(2021, 1, 1),
        "email": ["zyrvsuarez07@gmail.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'trigger_rule': 'all_success',
        #  description="A simple tutorial DAG",
        #  schedule=timedelta(days=1),
        #  start_date=datetime(2021, 1, 1),
        #  catchup=False,
        #  tags=["example"]
    }

"""
def get_latest_file(ds, **kwargs):

    bucket = f'{credentials.bucket_name}'

    s3_list_operator = S3ListOperator(
        task_id='s3_list_operator',
        bucket=bucket,
        aws_conn_id='aws_default',
        config=Config(s3={'addressing_style': 'path'}),
        dag=dag,
    )

    file_list = s3_list_operator.execute(context=kwargs)
    
    # Sort the file list by modified timestamp in descending order
    sorted_files = sorted(file_list, key=lambda x: x['LastModified'], reverse=True)

    # Return the key of the latest file
    if sorted_files:
        return sorted_files[0]['Key']
    else:
        return None
"""


# Create Dummy operator to get latest file
class s3NewestFileOperator(BaseOperator):
    def __init__(self, s3_bucket, s3_prefix, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id='aws_default') #change aws_conn_id

        # List objects in the S3 bucket
        objects = s3_hook.list_keys(bucket_name=self.s3_bucket, prefix=self.s3_prefix)

        # Sort the objects by their last modified timestamp in descending order
        objects_sorted = sorted(objects, key=lambda x: x['LastModified'], reverse=True)

        # Get the newest file
        if objects_sorted:
            context['ti'].xcom_push(key='newest', value=objects_sorted[0]['Key'])
        else:
            return None


def process_latest_file(ds, **kwargs):
    # The task to perform on the latest file
    # Use the file_key variable to access the latest file key
    file_key = kwargs['ti'].xcom_pull(task_ids='get_latest_file_task')
    # Add your logic to process the latest file



with DAG('airflow-zvsuarez', default_args=default_args) as dag:
    
    # ETL task
    run_etl = PythonOperator(
        task_id='run_etl',
        description="ETL from API to Amazon S3",
        schedule_interval='@hourly', # 0 * * * *
        python_callable=airpol_etl,
    )
    
    # DummyOperator to get latest file task
    #start = DummyOperator(task_id='start')
    get_latest_file_task = s3NewestFileOperator(
        task_id='get_latest_file_task',
        description="Get latest file in Amazon S3",
        retries=2,
        s3_bucket=f'{credentials.bucket_name}',
        s3_prefix=f'{credentials.bucket_name}/data/'
    )
    #end = DummyOperator(task_id='end')

    process_latest_file_task = PythonOperator(
        task_id='process_latest_file_task',
        provide_context=True,
        python_callable=process_latest_file
        #depends_on_past= True
)

    # Load to Redshift task


    #run_etl >> get_latest_file_task >> process_latest_file_task
