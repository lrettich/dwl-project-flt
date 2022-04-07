import pandas as pd


def delete_duplicates(conn):
    questions = pd.read_sql(f'SELECT * FROM questions', conn.sqlalchemy_connection)
    questions.sort_values(by='last_activity_date', ascending=False, inplace=True)
    questions.drop_duplicates(subset='question_id', inplace=True)
    questions.to_sql(name='questions', con=conn.sqlalchemy_connection, index=False, if_exists='replace')
