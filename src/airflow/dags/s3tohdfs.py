from airflow import DAG

from airflow.operators.bash_operator import BashOperator
from airflow.operators import HiveOperator

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

# Some params related to this dag... but not generic enough to make it into the default_args
s3fetch_params = {
    's3_bucket' : 's3://dwdii/airflow_raw',
    'hdfs_destination' : '/user/hadoop/temp',
    'src_pattern' : r'.*\.ZIP'
}

# Pre-step - clear destination (but ignore errors)
cleanHdfs_tmpl = """
    hdfs dfs -rm -skipTrash {{ params.hdfs_destination }}/*
    echo "hdfs -rm returned: $?"
    exit 0
"""
cleanHdfs = BashOperator(
    task_id = 'clean_hdfs',
    bash_command=cleanHdfs_tmpl,
    params=s3fetch_params,
    dag=dag
)

# Task - s3-dist-cp from Amazon
# s3fetch = 's3-dist-cp --src s3://dwdii/putevt --dest /user/hadoop/temp --srcPattern .*\.ZIP'
s3fetch_tmpl = """
    echo "s3_bucket: {{ params.s3_bucket }}"
    echo "hdfs_dest: {{ params.hdfs_destination }}"
    echo " src_ptrn: {{ params.src_pattern }}"
    s3-dist-cp --src {{ params.s3_bucket }} --dest {{ params.hdfs_destination }} --srcPattern {{ params.src_pattern }}
"""

t1 = BashOperator(
    task_id = 'fetch_from_s3',
    bash_command=s3fetch_tmpl,
    params=s3fetch_params,
    dag=dag
)

# HiveOperator
hql = """
    CREATE EXTERNAL TABLE IF NOT EXISTS dwdii2.noaa_temps 
    (
        station string,
        year int,
        jan string,
        feb string,
        mar string,
        apr string,
        may string,
        jun string,
        jul string,
        aug string,
        sep string,
        oct string,
        nov string,
        dec string
    ) 
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '
    STORED AS TEXTFILE
    LOCATION '/user/hadoop/temp/';
"""

createTable = HiveOperator(
    task_id='hive_create_ext_table',
    hql=hql,
    params=s3fetch_params,
    dag=dag
)


t1.set_upstream(cleanHdfs)
createTable.set_upstream(t1)

