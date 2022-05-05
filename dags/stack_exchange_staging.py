from datetime import timedelta, datetime, timezone, date as dt_date
import pandas as pd
import psycopg2
import os
from sqlalchemy import column, create_engine
import time
from airflow.models import Variable

# get credentials for stackex query
stack_user = Variable.get('SE_USER')
stack_password = Variable.get('SE_PASSWORD')
stack_endpoint = Variable.get('SE_ENDPOINT')
stack_port = Variable.get("SE_PORT")
stack_database = Variable.get('SE_DATABASENAME')

# get credentials for staged df upload
staged_user = Variable.get("STAGED_USER")
staged_password = Variable.get("STAGED_PASSWORD")
staged_endpoint = Variable.get("STAGED_ENDPOINT")
staged_port = Variable.get("STAGED_PORT")
staged_database = Variable.get("STAGED_DATABASE")

# get credentials for job db
job_user = Variable.get("DJ_USER")
job_password = Variable.get("DJ_PASSWORD")
job_endpoint = Variable.get("DJ_ENDPOINT")
job_port = Variable.get("DJ_PORT")
job_database = Variable.get("DJ_DATABASENAME")



# ---------------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------- stage 2 --------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------

def get_stack_exchange_staging_2():

    # ---------------------------------------- define date range for query ------------------------------------------------
    # ---------------------------------------------------------------------------------------------------------------------
    START = Variable.get("START")
    # get yesterday date as end date
    yesterday_date = (datetime.now() - timedelta(days=1)).date()
    END = yesterday_date.strftime("%Y-%m-%d")

    # --------------------------- define functions for clean up, enrichment and upload ------------------------------------
    # ---------------------------------------------------------------------------------------------------------------------

    # define query tranches as unix timestamps (day increments)
    # e.g. for START = "2022-04-25" // END = "2022-04-27":
    # [(datetime.date(2022, 4, 25), 1650844800, 1650931200), (datetime.date(2022, 4, 26), 1650931200, 1651017600), (datetime.date(2022, 4, 27), 1651017600, 1651104000)]
    def get_query_tranches(abs_start:str, abs_end:str) -> list:
        query_tranches = []
        # create date range
        query_date_range = pd.date_range(start=abs_start, end=abs_end).tolist()
        # create list of tuples of unix timestamps (start, end) for each day in date range
        for date in query_date_range:
            start_unix = int(date.replace(tzinfo=timezone.utc).timestamp())
            end_unix = int((date + timedelta(days=1)).replace(tzinfo=timezone.utc).timestamp())
            query_tranches.append((date.date(), start_unix, end_unix))

        return query_tranches


    # get stackex tranche for day increment as df
    # not selected:
    # is_answered, view_count, answer_count, score, last_activity_date, last_edit_date, question_id, content_license, link, title, "owner.reputation", "owner.user_id", "owner.user_type", "owner.profile_image", "owner.display_name", "owner.link", closed_date, closed_reason, "owner.accept_rate", accepted_answer_id, protected_date, bounty_amount, bounty_closes_date, locked_date, community_owned_date
    def get_tranche_data(start_unix: int, end_unix: int) -> pd.DataFrame:
        query = f"""SELECT tags, creation_date
                FROM public.questions
                WHERE creation_date >= {start_unix} AND
                creation_date < {end_unix};"""

        return pd.read_sql_query(sql=query, con=stack_engine)


    # get top 10 technologies from job postings
    JOB_DATABASE_URL = f"postgresql+psycopg2://{job_user}:{job_password}@{job_endpoint}:{job_port}/{job_database}"
    job_engine = create_engine(JOB_DATABASE_URL)
    top_tech_df = pd.read_sql_query(sql="SELECT technology FROM top_technologies", con=job_engine)

    # match tag given with top 10 reference tags from job db
    def get_main_tag(tag: str):
        if "java" in tag:
            if "javascript" in tag:
                return "javascript"
            else:
                return "java"
        else:
            for job_tag in top_tech_df["technology"].str.lower():
                if job_tag in tag:
                    return job_tag


    # clean up and enrich stackex df for stage 2
    def stackex_staging2(df: pd.DataFrame, date: datetime.date) -> pd.DataFrame:
        # turn string of tags to list of tags
        df.loc[:, "tags"] = df.loc[:, "tags"].apply(lambda x: x.replace("{", "").replace("}", ""))
        df.loc[:, "tags"] = df.loc[:, "tags"].apply(lambda x: x.split(","))
        # explode df according to number of tags in tags column
        df = df.explode("tags")
        # group by tags and count
        df = df.groupby(["tags"], as_index=False).size()
        # add human readable date
        df.loc[:, "date_stackex"] = date
        # add source info
        df.loc[:, "source"] = "stackexchange"
        # assign to main tech tag
        df.loc[:, "tag_stackex"] = df.loc[:, "tags"].apply(lambda x: get_main_tag(x))
        # fill nan/none
        df.loc[:, "tag_stackex"] = df.loc[:, "tag_stackex"].apply(lambda x: "not_in_top_10" if pd.isnull(x) else x)
        return df

    # --------------------------------- set up db connection and apply functions ---------------------------------------------
    # ---------------------------------------------------------------------------------------------------------------------

    # set up engine for stackex query
    STACK_DATABASE_URL = f"postgresql+psycopg2://{stack_user}:{stack_password}@{stack_endpoint}:{stack_port}/{stack_database}"
    stack_engine = create_engine(STACK_DATABASE_URL)

    # set up engine for staged 2 data upload
    STAGED_DATABASE_URL = f"postgresql+psycopg2://{staged_user}:{staged_password}@{staged_endpoint}:{staged_port}/{staged_database}"
    staged_engine = create_engine(STAGED_DATABASE_URL)

    # initial table clean to enable adding data only for one run (without having data from other runs in table)
    staged_engine.execute("DROP TABLE IF EXISTS stackex_staged_2")

    # proceed data query, clean up and upload
    for tranch in get_query_tranches(START, END):
        raw_tranche_data = get_tranche_data(tranch[1], tranch[2])
        staged_data = stackex_staging2(raw_tranche_data, tranch[0])

        staged_data.to_sql("stackex_staged_2", con=staged_engine, index=False, if_exists="append")

    # extra time to not risk bad behavoir cause by latency
    time.sleep(5)



# ---------------------------------------------------------------------------------------------------------------------
# --------------------------------------------- stage 3 ---------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------

def get_stack_exchange_staging_3():

    # IF FOLLOWING IS EXECUTED SEPARATED FROM FIRST STAGING OF STACKEXCHANGE RAW DATA, UNCOMMENT THIS PART TO ENSURE DB CONNECTIONS AND LOAD OF TOP 10 TAGS
    # # get only data with relevant top 10 tags. If needed, set up db connection:
    # JOB_DATABASE_URL = f"postgresql+psycopg2://{job_user}:{job_password}@{job_endpoint}:{job_port}/{job_database}"
    # job_engine = create_engine(JOB_DATABASE_URL)
    # top_tech_df = pd.read_sql_query(sql="SELECT technology FROM top_technologies", con=job_engine)

    # # get credentials for staged df upload
    # staged_user=os.getenv("STAGED_USER")
    # staged_password=os.getenv("STAGED_PASSWORD")
    # staged_endpoint=os.getenv("STAGED_ENDPOINT")
    # staged_port=os.getenv("STAGED_PORT")
    # staged_database=os.getenv("STAGED_DATABASE")

    # # set up engine for staged 2 data upload
    # STAGED_DATABASE_URL = f"postgresql+psycopg2://{staged_user}:{staged_password}@{staged_endpoint}:{staged_port}/{staged_database}"
    # staged_engine = create_engine(STAGED_DATABASE_URL)

    # get top 10 technologies from job postings
    JOB_DATABASE_URL = f"postgresql+psycopg2://{job_user}:{job_password}@{job_endpoint}:{job_port}/{job_database}"
    job_engine = create_engine(JOB_DATABASE_URL)
    top_tech_df = pd.read_sql_query(sql="SELECT technology FROM top_technologies", con=job_engine)

    top_10_tags = tuple(top_tech_df["technology"].str.lower())
    top_10_query = f"SELECT * FROM stackex_staged_2 WHERE tag_stackex in {top_10_tags};"
    top_10_stackex_df = pd.read_sql_query(sql=top_10_query, con=staged_engine)

    # goup by date_stackex and tag_stackex. Sum up size
    top_10_stackex_df = top_10_stackex_df.groupby(["date_stackex", "tag_stackex"], as_index=False).agg({
        "size": "sum",
        "source": "min"
    })

    # get standardized values for every single tag separate (i.e., like google trends calculation)
    # tags: list of top 10 tags
    def create_standardized_value_df(df, tag_column, value_column, tags):
        standardized_df_list = []
        for tag in tags:
            single_tag_df = df[df[tag_column] == tag]
            max_value = single_tag_df[value_column].max()
            single_tag_df["indexed_stackex"] = df[value_column].apply(lambda x: (x/max_value)*100)
            standardized_df_list.append(single_tag_df)
        return pd.concat(standardized_df_list, ignore_index=True)

    # apply function, rename size and select relevant columns in right order
    standardized_value_df = create_standardized_value_df(top_10_stackex_df, "tag_stackex", "size", top_10_tags)
    standardized_value_df.rename(columns={"size": "count_stackex"}, inplace=True)
    standardized_value_df = standardized_value_df[["date_stackex", "tag_stackex", "count_stackex", "indexed_stackex", "source"]]

    STAGED_DATABASE_URL = f"postgresql+psycopg2://{staged_user}:{staged_password}@{staged_endpoint}:{staged_port}/{staged_database}"
    staged_engine = create_engine(STAGED_DATABASE_URL)

    # upload new created df to staged 3 db
    standardized_value_df.to_sql("stackex_staged_3", con=staged_engine, index=False, if_exists="replace")
