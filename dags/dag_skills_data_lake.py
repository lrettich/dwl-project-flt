import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import functions_stack_exchange
import job_request
import stack_exchange_handler
import top_technologies
import google_trends

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

stack_exchange_collection = PythonOperator(
   task_id="stack_exchange_collection",
   python_callable=stack_exchange_handler.collect_stack_exchange_data,
   dag=dag
)

import_top_technologies = PythonOperator(
   task_id="import_top_technologies",
   python_callable=top_technologies.import_top_technologies,
   dag=dag
)

google_trends_collection = PythonOperator(
   task_id="google_trends_collection",
   python_callable=google_trends.google_trends,
   dag=dag
)

greet_task >> dev_job_collection
dev_job_collection >> stack_exchange_collection
dev_job_collection >> import_top_technologies
import_top_technologies >> google_trends_collection
