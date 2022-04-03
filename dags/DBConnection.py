# Class to store the connection to the database

import psycopg2
import sqlalchemy


class DBConnection:
    def __init__(self, user, password, endpoint, db_name):
        self.sqlalchemy_connection = None
        self.psycopg2_connection = None
        self.user = user
        self.password = password
        self.endpoint = endpoint
        self.db_name = db_name

    def init_psycopg2_connection(self):
        conn_string = f'host={self.endpoint} dbname={self.db_name} user={self.user} password={self.password}'
        self.psycopg2_connection = psycopg2.connect(conn_string)
        self.psycopg2_connection.autocommit = True

    def init_sqlalchemy_connection(self):
        conn_string = f'postgresql://{self.user}:{self.password}@{self.endpoint}/{self.db_name}'
        self.sqlalchemy_connection = sqlalchemy.create_engine(conn_string)
