from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.s3_operator import S3CreateBucketOperator, S3KeySensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.redshift_operator import RedshiftDataOperator
from main_etl import air_pol_etl


default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
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
        # 'trigger_rule': 'all_success'
    }

dag = DAG(
    "create_s3_bucket",
    default_args={},
    description="Create S3 bucket for data ingestion",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
)

run_s3_create = S3CreateBucketOperator(
    #task_id="sleep",
    #depends_on_past=False,
    #bash_command="sleep 5",
    #retries=3,
)

run_etl = PythonOperator(
    #task_id="sleep",
    #depends_on_past=False,
    #bash_command="sleep 5",
    #retries=3,
)

run_redshift = PythonOperator(
    #task_id="sleep",
    #depends_on_past=False,
    #bash_command="sleep 5",
    #retries=3,
)


#run_s3_create >> run_etl
