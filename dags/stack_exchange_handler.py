import os
from DBConnection import DBConnection
from StackExchangeDataCollector import StackExchangeDataCollector
from airflow.models import Variable


DJ_ENDPOINT = Variable.get('DJ_ENDPOINT')
DJ_DB_NAME = Variable.get('DJ_DATABASENAME')
DJ_USERNAME = Variable.get('DJ_USER')
DJ_PASSWORD = Variable.get('DJ_PASSWORD')

SE_ENDPOINT = Variable.get('SE_ENDPOINT')
SE_DB_NAME = Variable.get('SE_DATABASENAME')
SE_USERNAME = Variable.get('SE_USER')
SE_PASSWORD = Variable.get('SE_PASSWORD')

SE_API_KEY = '8SD3PD5C6fVJBH1h0kpk3w(('
NO_OF_TAGS = 10


def collect_stack_exchange_data(**kwargs):
    # Instantiate Connection to Dev-Jobs-DB
    dj_conn = DBConnection(user=DJ_USERNAME, password=DJ_PASSWORD, endpoint=DJ_ENDPOINT, db_name=DJ_DB_NAME)
    dj_conn.init_psycopg2_connection()

    # Instantiate Connection to StackExchange-DB
    se_conn = DBConnection(user=SE_USERNAME, password=SE_PASSWORD, endpoint=SE_ENDPOINT, db_name=SE_DB_NAME)
    se_conn.init_psycopg2_connection()
    se_conn.init_sqlalchemy_connection()

    # Initiate and trigger StackExchangeDataCollector
    collector = StackExchangeDataCollector(se_db_connection=se_conn, dj_db_connection=dj_conn, stack_api_key=SE_API_KEY,
                                           start_datetime=kwargs.get('data_interval_start'),
                                           end_datetime=kwargs.get('data_interval_end'))
    collector.read_tags(no_of_tags=NO_OF_TAGS)
    collector.collect_data()
