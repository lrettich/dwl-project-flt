from keyword import kwlist
from pytrends.request import TrendReq
from sqlalchemy import column, create_engine
import pandas as pd
import time
from datetime import datetime, timedelta
from airflow.models import Variable


# get credentials for job db
job_user = Variable.get("DJ_USER")
job_password = Variable.get("DJ_PASSWORD")
job_endpoint = Variable.get("DJ_ENDPOINT")
job_port = Variable.get("DJ_PORT")
job_database = Variable.get("DJ_DATABASENAME")

# get credentials for google trends db
ggT_user = Variable.get("GT_USER")
ggT_password = Variable.get("GT_PASSWORD")
ggT_endpoint = Variable.get("GT_ENDPOINT")
ggT_port = Variable.get("GT_PORT")
ggT_database = Variable.get("GT_DATABASENAME")


def get_google_trends_simple():

    # connect to job data
    JOB_DATABASE_URL = f"postgresql+psycopg2://{job_user}:{job_password}@{job_endpoint}:{job_port}/{job_database}"
    job_engine = create_engine(JOB_DATABASE_URL)

    # get top 10 tags
    top_tech_df = pd.read_sql_query(sql="SELECT technology FROM top_technologies", con=job_engine)
    top_tags = top_tech_df["technology"].str.lower()

    START = Variable.get("START")
    # get yesterday date
    yesterday_date = (datetime.now() - timedelta(days=1)).date()
    END = yesterday_date.strftime("%Y-%m-%d")

    pytrends = TrendReq(hl='en-US', tz=360)

    year_start = int(START[:4])
    month_start = int(START[5:7])
    day_start = int(START[8:10])
    year_end = int(END[:4])
    month_end = int(END[5:7])
    day_end = int(END[8:10])

    # get standardized values for every single tag separate (i.e., like google trends calculation)
    def create_standardized_value_df(df, raw_value_column):
        indexed_values = []
        for values in df[raw_value_column]:
            indexed_values.append((values/(df[raw_value_column].max()))*100)
        df["indexed_google_trends"] = indexed_values
        return df

    # proceed google trends request
    def get_gg_trend_data(tag_ls: list) -> pd.DataFrame:
        gg_trend_df_ls = []
        for tag in tag_ls:
            tag_df = pytrends.get_historical_interest(
                        [tag],
                        year_start=year_start,
                        month_start=month_start,
                        day_start=day_start,
                        hour_start=0,
                        year_end=year_end,
                        month_end=month_end,
                        day_end=day_end,
                        hour_end=23,
                        cat=0,
                        geo='CH',
                        gprop='',
                        sleep=10
                    )

            # reset index
            tag_df = tag_df.reset_index()
            # only consider dates (no hours needed)
            tag_df["date"] = tag_df["date"].apply(lambda x: x.date())
            # rename value column given
            tag_df.rename(columns={tag: "gg_trend_value"}, inplace=True)
            # select only relevant columns
            tag_df = tag_df[["date", "gg_trend_value"]]
            # group by date and calculate mean of values
            grouped_tag_df = tag_df.groupby(["date"], as_index=False).mean()
            # calculate indexed trend values per queried time period
            grouped_tag_df = create_standardized_value_df(grouped_tag_df, "gg_trend_value")
            # add current tag as separate column
            grouped_tag_df["tag"] = tag
            # append tag df to df list
            gg_trend_df_ls.append(grouped_tag_df)
            time.sleep(5)
        return gg_trend_df_ls

    # apply function to get list of df for each technology
    df_ls = get_gg_trend_data(top_tags)

    # concat these list of df to final df
    df = pd.concat(df_ls, ignore_index=True)

    # connect to db for staged data
    GGT_DATABASE_URL = f"postgresql+psycopg2://{ggT_user}:{ggT_password}@{ggT_endpoint}:{ggT_port}/{ggT_database}"
    ggT_engine = create_engine(GGT_DATABASE_URL)
    # upload new created df to google trends db
    df.to_sql("google_trends", con=ggT_engine, index=False, if_exists="replace")
