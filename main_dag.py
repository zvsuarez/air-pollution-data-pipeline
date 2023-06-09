from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
#from airflow.providers.amazon.aws.operators.s3 import S3Hook
from main_etl import airpol_etl
import boto3
import credentials as crd


default_args={
        "depends_on_past": False,
        "start_date": datetime(2023, 6, 22, 12, 0, 0),
        "email": ["zyrvsuarez07@gmail.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1)
        #"schedule":'0 8 * * *',
        #"catchup":True
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        #  start_date=datetime(2021, 1, 1),
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
        #  tags=["example"]
    }

def load_to_redshift():
    
    # Get latest object in S3
    s3_client = boto3.client('s3')
    s3_bucket = crd.BUCKET
    s3_access_point= crd.BUCKET_AP
    s3_prefix = 'data/'
    response= s3_client.list_objects_v2(Bucket=s3_access_point, Prefix=s3_prefix)
    if 'Contents' in response:
        sorted_keys = sorted(response['Contents'], key=lambda obj: obj['LastModified'], reverse=True)
    latest_key = sorted_keys[0]['Key']
    s3_key_path = f's3://{s3_bucket}/{latest_key}'

    # Alternative get latest key in S3 bucket
    """s3_bucket = crd.BUCKET
    #s3_bucket_ARN= crd.BUCKET_ARN
    s3_prefix = 'data/'
    s3_hook = S3Hook(aws_conn_id=crd.AWS_CONN)
    object_keys = s3_hook.list_keys(bucket_name=s3_bucket, prefix=s3_prefix)
    #sorted_keys = sorted(object_keys, key=lambda x: x['LastModified'], reverse=True)
    sorted_keys = sorted(object_keys, key=lambda x: s3_hook.get_key(bucket_name=s3_bucket, key=x).last_modified, reverse=True)
    latest_key = sorted_keys[0] if sorted_keys else None
    s3_key_path = f's3://{s3_bucket}/{latest_key}' if latest_key else None"""
    
    # Establish connection to redshift serverless
    client = boto3.client('redshift-data', region_name=crd.REGION, aws_access_key_id=crd.AWS_ACCESS_KEY,aws_secret_access_key=crd.AWS_SECRET_KEY)
    copy_command = f"""COPY {crd.DBNAME}.{crd.SCHEMA}.{crd.TBNAME} FROM '{s3_key_path}' IAM_ROLE '{crd.REDSHIFT_ROLE}' FORMAT AS CSV DELIMITER ',' QUOTE '"' IGNOREHEADER 1 BLANKSASNULL EMPTYASNULL REGION AS '{crd.REGION}'"""

    response = client.execute_statement(
        Database=crd.DBNAME,
        Sql=copy_command,
        WithEvent=False,
        WorkgroupName=crd.WORKGROUP
    )
    return response

    # Use this instead if redshift has provisioned clusters
    """
    df = pd.read_csv(s3_hook.read_key(s3_key_path))
    columns = ', '.join(df.columns)
    values = [tuple(row) for row in df.to_numpy()]
    placeholders = ', '.join(['%s'] * len(df.columns))
    query = <query statement>
    conn = psycopg2.connect(host={crd.HOST}, port={crd.PORT}, dbname={crd.DBNAME},
                            user={crd.USER}, password={crd.PASS})
    cursor = conn.cursor()
    schema = "schema-name"
    cursor.executemany(query, values)
    conn.commit()
    cursor.close()
    conn.close()
    """
    
with DAG('airflow-zvsuarez', default_args=default_args) as dag:
    # ETL task
    run_etl = PythonOperator(
        task_id='run_etl',
        python_callable=airpol_etl,
    )
    # Load to Redshift task
    run_load = PythonOperator(
        task_id='run_load',
        python_callable=load_to_redshift
    )

    run_etl>>run_load
