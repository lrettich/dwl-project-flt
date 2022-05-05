import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

dag = DAG('dag_skills_data_lake', start_date=datetime.datetime(2022, 4, 14), schedule_interval='@daily')


# The following code section may look like bad coding practise, having functions with no logic besides calling
# another function and having import statements iside of functions instead of in the beginneing of the file.
# However, for Airflow, it is important to keep the complexity of the top level code of a DAG low.
# As the imports below can use some time to be loaded, it is much better to only have them executed during the
# runtime of the DAG instead of every few seconds, as it happens with imports on top level code.
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

def call_google_trends_staging():
    import google_trends_simple
    google_trends_simple.get_google_trends_simple()

def call_job_staging():
    import job_staging
    job_staging.get_job_staging()

def call_stack_exchange_staging_1():
    import stack_exchange_staging
    stack_exchange_staging.get_stack_exchange_staging_2()

def call_stack_exchange_staging_2():
    import stack_exchange_staging
    stack_exchange_staging.get_stack_exchange_staging_3()

def call_dwh_merge():
    import merging_for_dwh
    merging_for_dwh.merge_data_for_dwh()


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

dev_job_staging = PythonOperator(
   task_id="dev_job_staging",
   python_callable=call_job_staging,
   dag=dag
)

stack_exchange_staging_1 = PythonOperator(
   task_id="stack_exchange_staging_1",
   python_callable=call_stack_exchange_staging_1,
   dag=dag
)

stack_exchange_staging_2 = PythonOperator(
   task_id="stack_exchange_staging_2",
   python_callable=call_stack_exchange_staging_2,
   dag=dag
)

google_trends_staging = PythonOperator(
   task_id="google_trends_staging",
   python_callable=call_google_trends_staging,
   dag=dag
)

dwh_merge = PythonOperator(
   task_id="dwh_merge",
   python_callable=call_dwh_merge,
   dag=dag
)

dev_job_collection >> import_top_technologies
import_top_technologies >> dev_job_staging
import_top_technologies >> google_trends_collection
google_trends_collection >> google_trends_staging
import_top_technologies >> stack_exchange_question_collection
import_top_technologies >> stack_exchange_answer_collection
stack_exchange_question_collection >> stack_exchange_question_cleaning
stack_exchange_answer_collection >> stack_exchange_answer_cleaning
stack_exchange_question_cleaning >> stack_exchange_missing_questions_collection
stack_exchange_answer_cleaning >> stack_exchange_missing_questions_collection
stack_exchange_missing_questions_collection >> stack_exchange_tags_cleaning
stack_exchange_tags_cleaning >> stack_exchange_staging_1
stack_exchange_staging_1 >> stack_exchange_staging_2
dev_job_staging >> dwh_merge
google_trends_staging >> dwh_merge
stack_exchange_staging_2 >> dwh_merge
