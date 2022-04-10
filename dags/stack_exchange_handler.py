import os
from airflow.models import Variable

import stack_exchange_cleaning
from DBConnection import DBConnection
from StackExchangeDataCollector import StackExchangeDataCollector


DJ_ENDPOINT = Variable.get('DJ_ENDPOINT')
DJ_DB_NAME = Variable.get('DJ_DATABASENAME')
DJ_USERNAME = Variable.get('DJ_USER')
DJ_PASSWORD = Variable.get('DJ_PASSWORD')

SE_ENDPOINT = Variable.get('SE_ENDPOINT')
SE_DB_NAME = Variable.get('SE_DATABASENAME')
SE_USERNAME = Variable.get('SE_USER')
SE_PASSWORD = Variable.get('SE_PASSWORD')

SE_API_KEY = '8SD3PD5C6fVJBH1h0kpk3w(('


def initiate_collector(**kwargs):
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
    collector.read_tags()
    return collector


def collect_stack_exchange_question_data(**kwargs):
    collector = initiate_collector(**kwargs)
    collector.collect_question_data()


def collect_stack_exchange_answer_data(**kwargs):
    collector = initiate_collector(**kwargs)
    collector.collect_answer_data()


def clean_stack_exchange_question_data():
    # Instantiate Connection to StackExchange-DB
    se_conn = DBConnection(user=SE_USERNAME, password=SE_PASSWORD, endpoint=SE_ENDPOINT, db_name=SE_DB_NAME)
    se_conn.init_sqlalchemy_connection()
    stack_exchange_cleaning.delete_duplicates(conn=se_conn, table='questions', ident='question_id')


def clean_stack_exchange_answer_data():
    # Instantiate Connection to StackExchange-DB
    se_conn = DBConnection(user=SE_USERNAME, password=SE_PASSWORD, endpoint=SE_ENDPOINT, db_name=SE_DB_NAME)
    se_conn.init_sqlalchemy_connection()
    stack_exchange_cleaning.delete_duplicates(conn=se_conn, table='answers', ident='answer_id')


def collect_stack_exchange_missing_questions():
    collector = initiate_collector()
    collector.collect_missing_questions()


def clean_stack_exchange_tags():
    collector = initiate_collector()
    collector.clean_tags()
