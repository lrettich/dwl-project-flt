import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import functions_stack_exchange
import job_request

dag = DAG('dag_skills_data_lake', start_date=datetime.datetime(2022, 4, 1), schedule_interval='@daily')


greet_task = PythonOperator(
   task_id="greet_task",
   python_callable=functions_stack_exchange.greet,
   dag=dag
)

dev_job_collection = PythonOperator(
   task_id="dev_job_collection",
   python_callable=job_request.job_request,
   dag=dag
)

greet_task >> dev_job_collection
