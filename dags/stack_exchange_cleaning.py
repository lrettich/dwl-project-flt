import pandas as pd
import logging


def delete_duplicates(conn, table, ident):
    questions = pd.read_sql(f'SELECT * FROM {table}', conn.sqlalchemy_connection)
    questions.sort_values(by='last_activity_date', ascending=False, inplace=True)
    initial_length = len(questions)
    questions.drop_duplicates(subset=ident, inplace=True)
    questions.to_sql(name=table, con=conn.sqlalchemy_connection, index=False, if_exists='replace')
    logging.info(f"Dropped {initial_length - len(questions)} duplicates from table {table}.")
