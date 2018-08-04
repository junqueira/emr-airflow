from airflow import DAG

from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta

default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'start_date' : datetime(2018, 8, 3),
    'email' : ['daniel@dittenhafersolutions.com'],
    'email_on_failure' : True,
    'email_on_retry' : True,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5)
}

# Prep the DAG container
dag = DAG('s3tohdfs', default_args=default_args, schedule_interval=timedelta(1))

# First task, get files from S3
s3fetch = 'echo s3 fetch placeholder'
t1 = BashOperator(
    task_id = 'fetch_from_s3',
    bash_command=s3fetch,
    dag=dag
)


# Second task - transfer into HDFS
pushtohdfs = 'echo push to hdfs placeholder'
t2 = BashOperator(
    task_id = 'local_to_hdfs',
    bash_command=pushtohdfs,
    dag=dag
)

t2.set_upstream(t1)