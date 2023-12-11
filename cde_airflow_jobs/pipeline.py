# (C) Cloudera, Inc. 2020-2023
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
#  Author(s): Maximilian Engelhardt

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from datetime import timedelta
from airflow import DAG
import pendulum

username = "user123"

default_args = {
    "owner": username,
    "retry_delay": timedelta(seconds=5),
    "depends_on_past": False,
    "start_date": pendulum.datetime(2020, 1, 1, tz="Europe/Amsterdam")
}

dag_name = f"{username}-07-airflow-code-dag"

airflow_tour_dag = DAG(
    dag_name,
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    is_paused_upon_creation=False
)

start = DummyOperator(
    task_id="start",
    dag=airflow_tour_dag
)

api_call_1 = SimpleHttpOperator(
    task_id="random_joke_api_1",
    method="GET",
    http_conn_id=f"{username}_random_joke_connection",
    endpoint="/jokes/programming/random",
    headers={"Content-Type":"application/json"},
    response_check=lambda response: True if response.status_code == 200 else False,
    dag=airflow_tour_dag,
    do_xcom_push=True
)

api_call_2 = SimpleHttpOperator(
    task_id="random_joke_api_2",
    method="GET",
    http_conn_id=f"{username}_random_joke_connection",
    endpoint="/jokes/programming/random",
    headers={"Content-Type":"application/json"},
    response_check=lambda response: True if response.status_code == 200 else False,
    dag=airflow_tour_dag,
    do_xcom_push=True
)

def _print_random_joke(**context):
    return context["ti"].xcom_pull(task_ids=["random_joke_api_1", "random_joke_api_2"])

api_response = PythonOperator(
    task_id="print_random_joke",
    python_callable=_print_random_joke,
    dag=airflow_tour_dag
)

# note that api_response only executed after successful api_call tasks
start >> [api_call_1, api_call_2] >> api_response
