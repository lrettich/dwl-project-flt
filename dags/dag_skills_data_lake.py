import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import functions_stack_exchange

dag = DAG('dag_skills_data_lake', start_date=datetime.datetime.now())


greet_task = PythonOperator(
   task_id="greet_task",
   python_callable=greet,
   dag=dag
)
