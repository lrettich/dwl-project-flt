import pandas as pd
from datetime import datetime, timedelta
import logging
import sys
from sqlalchemy import exc
from stackapi import StackAPI


class StackExchangeDataCollector:
    """
    Class that collects the Data from StackExchange (StackOverflow) and stores it to the data lake database.

    ...

    Attributes
    ----------
    se_db_connection : DBConnection
        DBConnection object with connection to the StackExchange database
    dj_db_connection : DBConnection
        DBConnection with connection to the DecJobs database
    start_datetime : datetime.datetime
        Timestamp from when on the data should be fetched
    end_datetime : datetime.datetime
        Timestamp until when the data should be fetched
    stack_site : stackapi.stackapi.StackAPI
        StackAPI object with settings for the API
    tags : list of str
        List of tags that should be fetched from StackExchange
    quota_remaining : int
        Remaining quota of requests that can be made to the API

    Methods
    -------
    read_tags():
        Read the required tags from the DevJobs database
    clean_tags():
        Deletes fetched data from the StackExchange database that is not matching the required tags
        from the DevJobs database.
    collect_question_data():
        Fetching question data from the StackExchange API
    collect_answer_data():
        Fetching answer data from the StackExchange API
    collect_missing_questions():
        Fetching question data from the StackExchange API in order to receive related the question
        for each answer in the database.
    """
    def __init__(self, se_db_connection, dj_db_connection,
                 stack_api_key=None, stack_api_page_size=100, stack_api_max_pages=1000,
                 start_datetime=datetime.now() - timedelta(days=1), end_datetime=datetime.now()):
        """
        Parameters
        ----------
        se_db_connection : DBConnection
            DBConnection object with connection to the StackExchange database
        dj_db_connection : DBConnection
            DBConnection with connection to the DecJobs database
        stack_api_key : str, optional
            Developer token for calling the StackExchange API
            (if not provided, no token is used and the access is limitted more strictly)
        stack_api_page_size : int, optional
            Setting for the StackAPI library. Defines how many objects should be fetched with one request.
            (default is 100)
        stack_api_max_pages : int, optional
            Setting for the StackAPI library. Defines how many requests can be made in one batch.
            (default is 1000)
        start_datetime : datetime.datetime, optional
            Timestamp from when on the data should be fetched (default is now minus one day)
        end_datetime : datetime.datetime, optional
            Timestamp until when the data should be fetched (default is now)
        """
        self.se_db_connection = se_db_connection
        self.dj_db_connection = dj_db_connection
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime

        # Initialize StackAPI and needed variables
        self.stack_site = StackAPI('stackoverflow', key=stack_api_key)
        self.stack_site.page_size = stack_api_page_size
        self.stack_site.max_pages = stack_api_max_pages

        # Attributes that will be set later
        self.tags = None
        self.quota_remaining = sys.maxsize

    def read_tags(self):
        """Read the required tags from the DevJobs database"""
        # Get cursor from psycopg2-Connection to DevJobs-Database
        cur = self.dj_db_connection.psycopg2_connection.cursor()

        # Get top keywords from DevJobs
        cur.execute("SELECT technology FROM top_technologies;")
        tags = cur.fetchall()
        # the following list comprehension is needed because...
        # - the query returns tuples, from where only the first element is relevant.
        # - the tags have to be transformed to lowercase, as StackExchange only works with lowercase tags.
        self.tags = [i[0].lower() for i in tags]

    def clean_tags(self):
        """Deletes fetched data from the StackExchange database that is not matching the required tags
        from the DevJobs database."""
        # Because not only questions have to be deleted, but as well the corresponding answers, the ids to delete
        # are first queried from the database and then deleted later.

        # The query is generated dynamically, including all the tags.
        query_start = "SELECT question_id FROM questions WHERE tags NOT LIKE '%"
        query_end = "%';"
        query_center = "%' AND tags NOT LIKE '%".join(self.tags)
        cur = self.se_db_connection.psycopg2_connection.cursor()
        cur.execute(query_start + query_center + query_end)
        unnecessary_question_ids = cur.fetchall()
        unnecessary_question_ids = [str(i[0]) for i in unnecessary_question_ids]
        logging.info(f"Deleting {len(unnecessary_question_ids)} questions and corresponding answers.")

        # as the sql delete query gets a problem, if too many ids are sepcified, the deletion is made in chunks.
        chunk_size = 100
        for i in range(0, len(unnecessary_question_ids), chunk_size):
            delete_string = ",".join(unnecessary_question_ids[i:i+chunk_size])
            cur.execute(f"DELETE FROM questions WHERE question_id IN ({delete_string});")
            cur.execute(f"DELETE FROM answers WHERE question_id IN ({delete_string});")

    def collect_question_data(self):
        """Fetching question data from the StackExchange API"""
        for tag in self.tags:
            logging.info(f'=== {tag} ===')
            self.__se_fetch_and_store("questions", tag)

    def collect_answer_data(self):
        """Fetching answer data from the StackExchange API"""
        logging.info('=== all answers ===')
        self.__se_fetch_and_store("answers")

    def collect_missing_questions(self):
        """Fetching question data from the StackExchange API in order to receive related the question
        for each answer in the database."""
        # Get cursor from psycopg2-Connection to StackExchange-Database
        cur = self.se_db_connection.psycopg2_connection.cursor()

        # Select questions that are referenced in the answers table but are not existing in the questions table.
        cur.execute("SELECT DISTINCT a.question_id FROM answers a "
                    "LEFT JOIN questions q ON a.question_id = q.question_id "
                    "WHERE q.question_id IS NULL;")
        question_ids_to_fetch = cur.fetchall()
        question_ids_to_fetch = [i[0] for i in question_ids_to_fetch]
        logging.info(f'=== Missing questions ({len(question_ids_to_fetch)}) ===')
        # Fetch the missing questions by ids in chunks of hunderds
        chunk_size = 100
        for i in range(0, len(question_ids_to_fetch), chunk_size):
            self.__se_fetch_and_store("questions", ids=question_ids_to_fetch[i:i+chunk_size])

    def __se_fetch_and_store(self, object_type, tag=None, ids=None):
        """Class-internal function that fetches the data using StackAPI and stores it into the database.

        The data can be filtered by tag or by id.

        Parameters
        ----------
        object_type : str
            Type of object that should be fetched from StackExchange (for example 'questions' or 'answers')
        tag : str, optional
            StackExchange tag, for which data should be fetched
            (if not provided, the data will not be filtered by tag)
        ids : list, optional
            list of ids, for which data should be fetched
            (if not provided, the data will not be filtered by id)

        Raises
        ------
        RuntimeError
            If two many requests are made on the same day, this Exception should avoid getting locked from the API.
        ValueError
            Raised when a combination of filters is applied that is not supported by the API
        """
        # check API quota
        if self.quota_remaining < 100:
            raise RuntimeError("Remaining API quota is below 100, no further requests should be made in order to avoid "
                               "getting locked from the API.")

        # Fetch data depending on parameters
        if ids is None:
            fetched_data = self.stack_site.fetch(object_type,
                                                 fromdate=self.start_datetime,
                                                 todate=self.end_datetime,
                                                 tagged=tag)
        elif tag is None:
            fetched_data = self.stack_site.fetch(object_type,
                                                 ids=ids)
        else:
            # The API does not allow a filter by tag AND id.
            raise ValueError('Data can only be fetched either by ids or by tag.')

        # the result of the API fetching is in josin format.
        # This can be transformed to a datafram using the following command.
        df = pd.json_normalize(fetched_data["items"])
        # Drop data about migration history, because this is not stringently structured.
        df = df.loc[:, ~df.columns.str.startswith('migrated')]
        try:
            df.to_sql(name=object_type,
                      con=self.se_db_connection.sqlalchemy_connection,
                      index=False,
                      if_exists='append')
            # if there is a new column in the dataframe, that is not present in the database, the above statement will fail.
            # (sqlalchemy 'append' can only be used if the columns stay the same)
            # The Exception-Handler will try to add the missing columns.
        except exc.SQLAlchemyError:
            tmp_table_name = 'tmp_' + object_type
            # Store data in a temporary table
            df.to_sql(name=tmp_table_name, con=self.se_db_connection.sqlalchemy_connection, index=False)
            # Add the missing columns to the table, according to the tables that were generated in the temporary table.
            self.__adapt_columns(from_table=tmp_table_name, to_table=object_type)
            # Hopefully, the data can be stored now.
            df.to_sql(name=object_type,
                      con=self.se_db_connection.sqlalchemy_connection,
                      index=False,
                      if_exists='append')
            # Delete the temporary table again.
            cur = self.se_db_connection.psycopg2_connection.cursor()
            cur.execute(f"DROP TABLE {tmp_table_name};")

        logging.info(f"Number of fetched {object_type}: {len(df)}")
        self.quota_remaining = fetched_data['quota_remaining']
        logging.info('quota_remaining: ' + str(self.quota_remaining))
        # the has_more value in the response would indicate, that not all the available data was collected.
        # This would mean that the parameters page_size and max_pages are set to low.
        if fetched_data['has_more']:
            logging.warning('Not all available records fetched from StackExchange')

    def __adapt_columns(self, from_table, to_table):
        """Class-internal helper function that adapts the columns of one table to the columns of another table.

        Parameters
        ----------
        from_table : str
            Name of the table from where the columns should be copied
        to_table : str
            Name of the table to where the columns should be copied
        """
        cur = self.se_db_connection.psycopg2_connection.cursor()
        # Read all column name and types from table 'from_table'
        cur.execute(f"SELECT column_name, data_type "
                    f"FROM information_schema.columns "
                    f"WHERE table_name = '{from_table}' "
                    f"ORDER BY ordinal_position;")
        columns = cur.fetchall()
        # Add all the columns to the table 'to_table' (if they are not already there)
        for column in columns:
            cur.execute(f'ALTER TABLE {to_table} ADD COLUMN IF NOT EXISTS "{column[0]}" {column[1]};')
