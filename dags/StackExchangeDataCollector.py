# Class that collects the Data from StackExchange (StackOverflow), storing it to the DataLake-Database.

import pandas as pd
from datetime import datetime, timedelta
import logging
import sys
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
        self.quota_remaining = sys.maxsize

    def read_tags(self):
        # Get cursor from psycopg2-Connection to DevJobs-Database
        cur = self.dj_db_connection.psycopg2_connection.cursor()

        # Get top keywords from DevJobs
        cur.execute("SELECT technology FROM top_technologies;")
        tags = cur.fetchall()
        self.tags = [i[0].lower() for i in tags]

    def clean_tags(self):
        query_start = "SELECT question_id FROM questions WHERE tags NOT LIKE '%"
        query_end = "%';"
        query_center = "%' AND tags NOT LIKE '%".join(self.tags)
        cur = self.se_db_connection.psycopg2_connection.cursor()
        cur.execute(query_start + query_center + query_end)
        unnecessary_question_ids = cur.fetchall()
        unnecessary_question_ids = [str(i[0]) for i in unnecessary_question_ids]
        logging.info(f"Deleting {len(unnecessary_question_ids)} questions and corresponding answers.")
        chunk_size = 100
        for i in range(0, len(unnecessary_question_ids), chunk_size):
            delete_string = ",".join(unnecessary_question_ids[i:i+chunk_size])
            cur.execute(f"DELETE FROM questions WHERE question_id IN ({delete_string});")
            cur.execute(f"DELETE FROM answers WHERE question_id IN ({delete_string});")

    def collect_question_data(self):
        for tag in self.tags:
            logging.info(f'=== {tag} ===')
            self.__se_fetch_and_store("questions", tag)

    def collect_answer_data(self):
        logging.info('=== all answers ===')
        self.__se_fetch_and_store("answers")

    def collect_missing_questions(self):
        # Get cursor from psycopg2-Connection to StackExchange-Database
        cur = self.se_db_connection.psycopg2_connection.cursor()

        # Get top keywords from DevJobs and loop though them
        cur.execute("SELECT DISTINCT a.question_id FROM answers a "
                    "LEFT JOIN questions q ON a.question_id = q.question_id "
                    "WHERE q.question_id IS NULL;")
        question_ids_to_fetch = cur.fetchall()
        question_ids_to_fetch = [i[0] for i in question_ids_to_fetch]
        logging.info(f'=== Missing questions ({len(question_ids_to_fetch)}) ===')
        chunk_size = 100
        for i in range(0, len(question_ids_to_fetch), chunk_size):
            self.__se_fetch_and_store("questions", ids=question_ids_to_fetch[i:i+chunk_size])

    def __se_fetch_and_store(self, object_type, tag=None, ids=None):
        if self.quota_remaining < 100:
            raise RuntimeError("Remaining API quota is below 100, no further requests should be made in order to avoid "
                               "getting locked from the API.")
        if ids is None:
            fetched_data = self.stack_site.fetch(object_type,
                                                 fromdate=self.start_datetime,
                                                 todate=self.end_datetime,
                                                 tagged=tag)
        elif tag is None:
            fetched_data = self.stack_site.fetch(object_type,
                                                 ids=ids)
        else:
            raise ValueError('Data can only be fetched either by ids or by tag.')

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
        self.quota_remaining = fetched_data['quota_remaining']
        logging.info('quota_remaining: ' + str(self.quota_remaining))
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
