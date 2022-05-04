import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import column, create_engine
from statistics import mean
from airflow.models import Variable


# get credentials for staged df upload
staged_user = Variable.get("STAGED_USER")
staged_password = Variable.get("STAGED_PASSWORD")
staged_endpoint = Variable.get("STAGED_ENDPOINT")
staged_port = Variable.get("STAGED_PORT")
staged_database = Variable.get("STAGED_DATABASE")

# get credentials for google trends db
ggT_user = Variable.get("GT_USER")
ggT_password = Variable.get("GT_PASSWORD")
ggT_endpoint = Variable.get("GT_ENDPOINT")
ggT_port = Variable.get("GT_PORT")
ggT_database = Variable.get("GT_DATABASENAME")


def merge_data_for_dwh():

    # get staged job data as singel dfs
    STAGED_DATABASE_URL = f"postgresql+psycopg2://{staged_user}:{staged_password}@{staged_endpoint}:{staged_port}/{staged_database}"
    staged_engine = create_engine(STAGED_DATABASE_URL)
    job_query = f"SELECT * FROM swissdevjobs_staged_2;"
    staged_swissdevjobs = pd.read_sql_query(sql=job_query, con=staged_engine)

    # get staged stackexchange data
    stackex_query = f"SELECT * FROM stackex_staged_3;"
    staged_stackex = pd.read_sql_query(sql=stackex_query, con=staged_engine)

    # connect to db for google trends data
    GGT_DATABASE_URL = f"postgresql+psycopg2://{ggT_user}:{ggT_password}@{ggT_endpoint}:{ggT_port}/{ggT_database}"
    ggT_engine = create_engine(GGT_DATABASE_URL)

    # get google trend data
    google_trend_query = f"SELECT * FROM google_trends;"
    staged_google_trend = pd.read_sql_query(sql=google_trend_query, con=ggT_engine)

    # merge all data in two steps (take google as base)
    df_gg_jobs = pd.merge(staged_google_trend, staged_swissdevjobs, left_on=["date", "tag"], right_on=["date_swissdevjobs", "tag_swissdevjobs"], how="left")
    df_gg_jobs_stack = pd.merge(df_gg_jobs, staged_stackex, left_on=["date", "tag"], right_on=["date_stackex", "tag_stackex"], how="left")

    # select relevant columns
    df_gg_jobs_stack = df_gg_jobs_stack[["date", "tag", "gg_trend_value", "indexed_google_trends", "count_swissdevjobs", "indexed_swissdevjobs", "salary_avg", "count_stackex", "indexed_stackex"]]

    # fill up missings by using previous values in df
    df_gg_jobs_stack = df_gg_jobs_stack.fillna(method="ffill")

    # ad avg indexed column
    df_gg_jobs_stack["avg_indexed"] = df_gg_jobs_stack.apply(lambda x: mean([x.indexed_google_trends, x.indexed_swissdevjobs, x.indexed_stackex]), axis=1)

    # upload new created df to staged data warehouse
    df_gg_jobs_stack.to_sql("staged_dwh", con=staged_engine, index=False, if_exists="replace")
