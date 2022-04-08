import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

dag = DAG('dag_skills_data_lake', start_date=datetime.datetime(2022, 4, 1), schedule_interval='@daily')


def call_job_request():
    import job_request
    job_request.job_request()

def call_collect_stack_exchange_data(**kwargs):
    import stack_exchange_handler
    stack_exchange_handler.collect_stack_exchange_data(kwargs)

def call_clean_stack_exchange_data():
    import stack_exchange_handler
    stack_exchange_handler.clean_stack_exchange_data()

def call_import_top_technologies():
    import top_technologies
    top_technologies.import_top_technologies()

def call_google_trends():
    import google_trends
    google_trends.google_trends


dev_job_collection = PythonOperator(
   task_id="dev_job_collection",
   python_callable=call_job_request,
   dag=dag
)

stack_exchange_collection = PythonOperator(
   task_id="stack_exchange_collection",
   python_callable=call_collect_stack_exchange_data,
   provide_context=True,
   dag=dag
)

stack_exchange_cleaning = PythonOperator(
   task_id="stack_exchange_cleaning",
   python_callable=call_clean_stack_exchange_data,
   dag=dag
)

import_top_technologies = PythonOperator(
   task_id="import_top_technologies",
   python_callable=call_import_top_technologies,
   dag=dag
)

google_trends_collection = PythonOperator(
   task_id="google_trends_collection",
   python_callable=call_google_trends,
   dag=dag
)

dev_job_collection >> import_top_technologies
import_top_technologies >> stack_exchange_collection
import_top_technologies >> google_trends_collection
stack_exchange_collection >> stack_exchange_cleaning
