"""Handler for all the functions in connection with the StackExchange data fetching process.

This script initializes and utilizes the classes StackExchangeDataCollector and DBConnection.
It servers as an interface between the DAG in Apache Airflow and the rest of the Python code.
This script is necessary beacuse the rest of the Python code was designed in a reusable matter,
that is meant to be independent from the exact usage in the DAG.

This file can be imported as a module and contains the following
functions:

    * collect_stack_exchange_question_data - fetch and store of questions from StackExchange
    * collect_stack_exchange_answer_data - fetch and store of answers from StackExchange
    * clean_stack_exchange_question_data - clean the questions data fetched from StackExchange
    * clean_stack_exchange_answer_data - clean the answers data fetched from StackExchange
    * collect_stack_exchange_missing_questions - fetch and store missing questions for already fetched answers
    * clean_stack_exchange_tags - delete data that does not correspond to the required tags from the DevJobs database
"""

import os
from airflow.models import Variable

import stack_exchange_cleaning
from DBConnection import DBConnection
from StackExchangeDataCollector import StackExchangeDataCollector

# load database credentials from Airflow variables
DJ_ENDPOINT = Variable.get('DJ_ENDPOINT')
DJ_DB_NAME = Variable.get('DJ_DATABASENAME')
DJ_USERNAME = Variable.get('DJ_USER')
DJ_PASSWORD = Variable.get('DJ_PASSWORD')

SE_ENDPOINT = Variable.get('SE_ENDPOINT')
SE_DB_NAME = Variable.get('SE_DATABASENAME')
SE_USERNAME = Variable.get('SE_USER')
SE_PASSWORD = Variable.get('SE_PASSWORD')

# StackExchange Developer token
# (allowed to be included directly in the code, as is allowed to be used by other to replicate the code)
SE_API_KEY = '8SD3PD5C6fVJBH1h0kpk3w(('


def initiate_collector(**kwargs):
    """Internal function for code parts that are used for several DAG tasks.
    Function is instantiating the connection to the databases and the StackExchangeDataCollector itself.
    As argument, the kwargs from airflow are passed. This may contain the fields data_interval_start and data_interval_end.
    Returns the prepared StackExchangeDataCollector object.
    """
    # Instantiate Connection to Dev-Jobs-DB
    dj_conn = DBConnection(user=DJ_USERNAME, password=DJ_PASSWORD, endpoint=DJ_ENDPOINT, db_name=DJ_DB_NAME)
    dj_conn.init_psycopg2_connection()

    # Instantiate Connection to StackExchange-DB
    se_conn = DBConnection(user=SE_USERNAME, password=SE_PASSWORD, endpoint=SE_ENDPOINT, db_name=SE_DB_NAME)
    se_conn.init_psycopg2_connection()
    se_conn.init_sqlalchemy_connection()

    # Initiate and trigger StackExchangeDataCollector
    collector = StackExchangeDataCollector(se_db_connection=se_conn, dj_db_connection=dj_conn, stack_api_key=SE_API_KEY,
                                           start_datetime=kwargs.get('data_interval_start', None),
                                           end_datetime=kwargs.get('data_interval_end', None))

    # Reads the tags from DevJobs database
    collector.read_tags()
    
    return collector


def collect_stack_exchange_question_data(**kwargs):
    """Fetch and store of questions from StackExchange
    To be called from airflow task
    As argument, the kwargs from airflow are passed. This may contain the fields data_interval_start and data_interval_end.
    """
    collector = initiate_collector(**kwargs)
    collector.collect_question_data()


def collect_stack_exchange_answer_data(**kwargs):
    """Fetch and store of answers from StackExchange
    To be called from airflow task
    As argument, the kwargs from airflow are passed. This may contain the fields data_interval_start and data_interval_end.
    """
    collector = initiate_collector(**kwargs)
    collector.collect_answer_data()


def clean_stack_exchange_question_data():
    """Clean the questions data fetched from StackExchange
    To be called from airflow task
    """
    # Instantiate Connection to StackExchange-DB
    se_conn = DBConnection(user=SE_USERNAME, password=SE_PASSWORD, endpoint=SE_ENDPOINT, db_name=SE_DB_NAME)
    se_conn.init_sqlalchemy_connection()
    stack_exchange_cleaning.delete_duplicates(conn=se_conn, table='questions', ident='question_id')


def clean_stack_exchange_answer_data():
    """Clean the answers data fetched from StackExchange
    To be called from airflow task
    """
    # Instantiate Connection to StackExchange-DB
    se_conn = DBConnection(user=SE_USERNAME, password=SE_PASSWORD, endpoint=SE_ENDPOINT, db_name=SE_DB_NAME)
    se_conn.init_sqlalchemy_connection()
    stack_exchange_cleaning.delete_duplicates(conn=se_conn, table='answers', ident='answer_id')


def collect_stack_exchange_missing_questions():
    """Fetch and store missing questions for already fetched answers
    To be called from airflow task
    """
    collector = initiate_collector()
    collector.collect_missing_questions()


def clean_stack_exchange_tags():
    """Delete data that does not correspond to the required tags from the DevJobs database
    To be called from airflow task
    """
    collector = initiate_collector()
    collector.clean_tags()
