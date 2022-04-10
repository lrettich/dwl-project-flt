import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

dag = DAG('dag_skills_data_lake', start_date=datetime.datetime(2022, 4, 1), schedule_interval='@daily')


def call_job_request():
    import job_request
    job_request.job_request()

def call_collect_stack_exchange_question_data(**kwargs):
    import stack_exchange_handler
    stack_exchange_handler.collect_stack_exchange_question_data(**kwargs)

def call_collect_stack_exchange_answer_data(**kwargs):
    import stack_exchange_handler
    stack_exchange_handler.collect_stack_exchange_answer_data(**kwargs)

def call_clean_stack_exchange_question_data():
    import stack_exchange_handler
    stack_exchange_handler.clean_stack_exchange_question_data()

def call_clean_stack_exchange_answer_data():
    import stack_exchange_handler
    stack_exchange_handler.clean_stack_exchange_answer_data()

def call_clean_stack_exchange_tags():
    import stack_exchange_handler
    stack_exchange_handler.clean_stack_exchange_tags()

def call_collect_stack_exchange_missing_questions():
    import stack_exchange_handler
    stack_exchange_handler.collect_stack_exchange_missing_questions()

def call_import_top_technologies():
    import top_technologies
    top_technologies.update_table()

def call_google_trends():
    import google_trends
    google_trends.google_trends()


dev_job_collection = PythonOperator(
   task_id="dev_job_collection",
   python_callable=call_job_request,
   dag=dag
)

stack_exchange_question_collection = PythonOperator(
   task_id="stack_exchange_question_collection",
   python_callable=call_collect_stack_exchange_question_data,
   provide_context=True,
   dag=dag
)

stack_exchange_answer_collection = PythonOperator(
   task_id="stack_exchange_answer_collection",
   python_callable=call_collect_stack_exchange_answer_data,
   provide_context=True,
   dag=dag
)

stack_exchange_question_cleaning = PythonOperator(
   task_id="stack_exchange_question_cleaning",
   python_callable=call_clean_stack_exchange_question_data,
   dag=dag
)

stack_exchange_answer_cleaning = PythonOperator(
   task_id="stack_exchange_answer_cleaning",
   python_callable=call_clean_stack_exchange_answer_data,
   dag=dag
)

stack_exchange_tags_cleaning = PythonOperator(
   task_id="stack_exchange_tags_cleaning",
   python_callable=call_clean_stack_exchange_tags,
   dag=dag
)

stack_exchange_missing_questions_collection = PythonOperator(
   task_id="stack_exchange_missing_questions_collection",
   python_callable=call_collect_stack_exchange_missing_questions,
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
import_top_technologies >> google_trends_collection
import_top_technologies >> stack_exchange_question_collection
import_top_technologies >> stack_exchange_answer_collection
stack_exchange_question_collection >> stack_exchange_question_cleaning
stack_exchange_answer_collection >> stack_exchange_answer_cleaning
stack_exchange_question_cleaning >> stack_exchange_missing_questions_collection
stack_exchange_answer_cleaning >> stack_exchange_missing_questions_collection
stack_exchange_missing_questions_collection >> stack_exchange_tags_cleaning
