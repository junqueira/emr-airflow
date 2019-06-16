# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from builtins import range
from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

args = {
    'owner': 'loadShark',
    'start_date': airflow.utils.dates.days_ago(12),
    'depends_on_past': False,
    #'start_date': datetime(2018, 8, 22),
    'email': ['admin@loadshark.io'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
}

dag = DAG(
    dag_id='order-to-trucks',
    default_args=args,
    schedule_interval='5 0 * * *',
    dagrun_timeout=timedelta(minutes=50),
)

run_this_last = DummyOperator(
    task_id='run_this_last',
    dag=dag,
)

# [START howto_operator_bash]

truck = """
       cd /home/hadoop
       . trucks-transport.sh
  """

run_this = BashOperator(
    task_id='truck_history',
    bash_command= truck,
    dag=dag,
)
# [END howto_operator_bash]

run_this >> run_this_last

for i in range(3):
    task = BashOperator(
        task_id='runme_' + str(i),
        bash_command='echo "{{ task_instance_key_str }}" && sleep 1',
        dag=dag,
    )
    task >> run_this


order = """
       echo "run_id={{ run_id }} | dag_run={{ dag_run }}"
       cd /home/hadoop
       . orders-consolidate.sh
  """

# [START howto_operator_bash_template]
also_run_this = BashOperator(
    task_id='order_consolidade',
    bash_command= order,
    dag=dag,
)
# [END howto_operator_bash_template]
also_run_this >> run_this_last

if __name__ == "__main__":
    dag.cli()
