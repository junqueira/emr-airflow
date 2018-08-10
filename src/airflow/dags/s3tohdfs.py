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

# First and only task thanks to s3-dist-cp from Amazon
# s3fetch = 's3-dist-cp --src s3://dwdii/putevt --dest /user/hadoop/temp --srcPattern .*\.ZIP'
s3fetch_tmpl = """
    echo "s3_bucket: {{ params.s3_bucket }}"
    echo "hdfs_dest: {{ params.hdfs_destination }}"
    echo " src_ptrn: {{ params.src_pattern }}"
    s3-dist-cp --src {{ params.s3_bucket }} --dest {{ params.hdfs_destination }} --srcPattern {{ params.src_pattern }}
"""

s3fetch_params = {
    's3_bucket' : 's3://dwdii/airflow',
    'hdfs_destination' : '/user/hadoop/temp',
    'src_pattern' : r'.*\.ZIP'
}

t1 = BashOperator(
    task_id = 'fetch_from_s3',
    bash_command=s3fetch_tmpl,
    params=s3fetch_params,
    dag=dag
)


# Not needed = Second task - transfer into HDFS
#pushtohdfs = 'echo push to hdfs placeholder'
#t2 = BashOperator(
    #task_id = 'local_to_hdfs',
    #bash_command=pushtohdfs,
    #dag=dag
#)

#t2.set_upstream(t1)
