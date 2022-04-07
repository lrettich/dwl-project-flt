# Class that collects the Data from StackExchange (StackOverflow), storing it to the DataLake-Database.

import pandas as pd
from datetime import datetime, timedelta
import logging
from sqlalchemy import exc
from stackapi import StackAPI


class StackExchangeDataCollector:
    def __init__(self, se_db_connection, dj_db_connection,
                 stack_api_key=None, stack_api_page_size=100, stack_api_max_pages=1000,
                 start_datetime=datetime.now() - timedelta(days=1), end_datetime=datetime.now()):
        self.se_db_connection = se_db_connection
        self.dj_db_connection = dj_db_connection
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime

        # Initialize StackAPI and needed variables
        self.stack_site = StackAPI('stackoverflow', key=stack_api_key)
        self.stack_site.page_size = stack_api_page_size
        self.stack_site.max_pages = stack_api_max_pages

        self.tags = None

    def read_tags(self, no_of_tags):
        # Get cursor from psycopg2-Connection to DevJobs-Database
        cur = self.dj_db_connection.psycopg2_connection.cursor()

        # Get top keywords from DevJobs and loop though them
        cur.execute(f"SELECT technology FROM dev_jobs_1 GROUP BY technology ORDER BY COUNT(*) DESC LIMIT {no_of_tags};")
        self.tags = cur.fetchall()

    def collect_data(self):
        for tag in self.tags:
            logging.info(f'=== {tag} ===')
            self.__se_fetch_and_store("questions", tag)
            # self.__se_fetch_and_store("answers", tag)
            logging.info('\n')

    def __se_fetch_and_store(self, object_type, tag):
        fetched_data = self.stack_site.fetch(object_type,
                                             fromdate=self.start_datetime,
                                             todate=self.end_datetime,
                                             tagged=tag)
        df = pd.json_normalize(fetched_data["items"])
        # Drop data about migration history, because this is not stringently structured.
        df = df.loc[:, ~df.columns.str.startswith('migrated')]
        try:
            df.to_sql(name=object_type,
                      con=self.se_db_connection.sqlalchemy_connection,
                      index=False,
                      if_exists='append')
            # if there is a new column, the above statement will fail.
            # The Exception-Handler will try to add the missing columns.
        except exc.SQLAlchemyError:
            tmp_table_name = 'tmp_' + object_type
            # Store data in temporary table
            df.to_sql(name=tmp_table_name, con=self.se_db_connection.sqlalchemy_connection, index=False)
            # Add missing columns
            self.__adapt_columns(from_table=tmp_table_name, to_table=object_type)
            # Try to store the data again
            df.to_sql(name=object_type,
                      con=self.se_db_connection.sqlalchemy_connection,
                      index=False,
                      if_exists='append')
            # Delete the temporary table
            cur = self.se_db_connection.psycopg2_connection.cursor()
            cur.execute(f"DROP TABLE {tmp_table_name};")

        logging.info(f"Number of fetched {object_type}: {len(df)}")
        logging.info('quota_remaining: ' + str(fetched_data['quota_remaining']))
        if fetched_data['has_more']:
            logging.warning('Not all available records fetched from StackExchange')

    def __adapt_columns(self, from_table, to_table):
        cur = self.se_db_connection.psycopg2_connection.cursor()
        cur.execute(f"SELECT column_name, data_type "
                    f"FROM information_schema.columns "
                    f"WHERE table_name = '{from_table}' "
                    f"ORDER BY ordinal_position;")
        columns = cur.fetchall()
        for column in columns:
            cur.execute(f'ALTER TABLE {to_table} ADD COLUMN IF NOT EXISTS "{column[0]}" {column[1]};')
