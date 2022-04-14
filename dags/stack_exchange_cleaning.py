"""Deletion of duplicates in the StackExchange database

According to the fetching process in StackExchangeDataCollector.py, it is likely, that the same data will be
collected more than once.
This script provedes the functionality to delete the duplicates from the StackExchange database.
Duplicates are recoginzed by id. If an entry is present multiple times, the one with the latest activity date
will be kept, as it can be assumed, that this entry contains the most actual infomration.

This file can be imported as a module and contains the following
functions:

    * delete_duplicates - performs the cleaning of the duplicates
"""

import pandas as pd
import logging


def delete_duplicates(conn, table, ident):
    """Performs the deletion of duplicate entries in the StackExchange database.

    Parameters
    ----------
    conn : DBConnection
        DBConnection object with connection to the StackExchange database
    table : str
        Name of the table where the duplicated should be dropped
    ident : str
        Name of the column that is used as an identifier to identify duplicates.

    """
    # read all the data from the database
    questions = pd.read_sql(f'SELECT * FROM {table}', conn.sqlalchemy_connection)
    initial_length = len(questions)

    # order by activity date
    questions.sort_values(by='last_activity_date', ascending=False, inplace=True)

    # drop the duplicates. The default value of the "keep" perameter is "first", so because of the ordering,
    # the one with tho most recent activity date is kept.
    questions.drop_duplicates(subset=ident, inplace=True)

    # store the data again to the database
    questions.to_sql(name=table, con=conn.sqlalchemy_connection, index=False, if_exists='replace')
    
    logging.info(f"Dropped {initial_length - len(questions)} duplicates from table {table}.")
