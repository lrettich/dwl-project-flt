import pandas as pd
import psycopg2
from sqlalchemy import column, create_engine
import time
from datetime import datetime, timedelta
from airflow.models import Variable


# get credentials for job db
job_user = Variable.get("DJ_USER")
job_password = Variable.get("DJ_PASSWORD")
job_endpoint = Variable.get("DJ_ENDPOINT")
job_port = Variable.get("DJ_PORT")
job_database = Variable.get("DJ_DATABASENAME")

# get credentials for staged df upload
staged_user = Variable.get("STAGED_USER")
staged_password = Variable.get("STAGED_PASSWORD")
staged_endpoint = Variable.get("STAGED_ENDPOINT")
staged_port = Variable.get("STAGED_PORT")
staged_database = Variable.get("STAGED_DATABASE")


def get_job_staging():
    # ---------------------------------------- define date range for query ------------------------------------------------
    # ---------------------------------------------------------------------------------------------------------------------
    START = Variable.get("START")
    # get yesterday date
    yesterday_date = (datetime.now() - timedelta(days=1)).date()
    END = yesterday_date.strftime("%Y-%m-%d")

    # get job data as a whole
    JOB_DATABASE_URL = f"postgresql+psycopg2://{job_user}:{job_password}@{job_endpoint}:{job_port}/{job_database}"
    job_engine = create_engine(JOB_DATABASE_URL)

    # get topo 10 tags
    top_tech_df = pd.read_sql_query(sql="SELECT technology FROM top_technologies", con=job_engine)
    top_10_tags = tuple(top_tech_df["technology"])
    # get raw job data
    job_query = f"""SELECT technology, salary_avg, request_date
                    FROM dev_jobs_1
                    WHERE technology in {top_10_tags} AND
                    request_date::date >= date '{START}' AND
                    request_date::date <= date '{END}';"""
    raw_job_df = pd.read_sql_query(sql=job_query, con=job_engine)

    # drop duplicates if any
    raw_job_df = raw_job_df.drop_duplicates(subset=["job_id", "technology"])

    # group by date and technology (count technology, avg salary_avg)
    grouped_job_df = raw_job_df.groupby(["request_date", "technology"], as_index=False).agg(
        technology_count = ("technology", "count"),
        salary_avg = ("salary_avg", "mean")
    )

    # lower case technology i.e., tags
    grouped_job_df["technology"] = grouped_job_df["technology"].apply(lambda x: x.lower())
    # convert datetime column to date format
    grouped_job_df["request_date"] = grouped_job_df["request_date"].apply(lambda x: x.date())
    # add column source
    grouped_job_df["source"] = "swissdevjobs"

    # get standardized values for every single tag separate (i.e., like google trends calculation)
    # tags: list of top 10 tags
    def create_standardized_value_df(df, tag_column, value_column, tags):
        standardized_df_list = []
        for tag in tags:
            single_tag_df = df[df[tag_column] == tag]
            max_value = single_tag_df[value_column].max()
            single_tag_df["indexed_swissdevjobs"] = df[value_column].apply(lambda x: (x/max_value)*100)
            standardized_df_list.append(single_tag_df)
        return pd.concat(standardized_df_list, ignore_index=True)

    standardized_value_df = create_standardized_value_df(grouped_job_df, "technology", "technology_count", top_tech_df["technology"].str.lower())

    # rename columns
    standardized_value_df.rename(columns={"request_date": "date_swissdevjobs", "technology": "tag_swissdevjobs", "technology_count": "count_swissdevjobs"}, inplace=True)
    # select relevant columns and right order
    standardized_value_df = standardized_value_df[["date_swissdevjobs", "tag_swissdevjobs", "count_swissdevjobs", "indexed_swissdevjobs", "salary_avg", "source"]]

    # set up db connection
    STAGED_DATABASE_URL = f"postgresql+psycopg2://{staged_user}:{staged_password}@{staged_endpoint}:{staged_port}/{staged_database}"
    staged_engine = create_engine(STAGED_DATABASE_URL)
    # upload new created df to staged 2 db
    standardized_value_df.to_sql("swissdevjobs_staged_2", con=staged_engine, index=False, if_exists="replace")
