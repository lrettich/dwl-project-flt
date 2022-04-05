import pandas as pd
from pytrends.request import TrendReq
from itertools import combinations
import time
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import top_technologies
from airflow.models import Variable

## Load credentials
GT_ENDPOINT = Variable.get('GT_ENDPOINT')
GT_DB_NAME = Variable.get('GT_DB_NAME')
GT_USERNAME = Variable.get('GT_USERNAME')
GT_PASSWORD = Variable.get('GT_PASSWORD')

## Define database connection
url = f"postgresql://{GT_USERNAME}:{GT_PASSWORD}@{GT_ENDPOINT}/{GT_DB_NAME}"
engine = create_engine(url)

def google_trends():
    """
    Requests data from the Google Trends API for a given set of keywords and time period.
    """

    def check_db_date(table):
      """
      Check whether the data for the selected date is already in the database.
      """

      sql = f"""
      SELECT DISTINCT
        DATE_TRUNC('day', date) date
      FROM {table}
      """

      db_dates = []

      with engine.connect() as conn:
        with conn.begin():
          try:
            result = conn.execute(sql)
            for row in result:
              db_dates.append(row[0])
            return 1, db_dates
          except:
            return 0, f"Table '{table}' does not exist. Starting initial load ..."

    def gt_historical_interest(kw_list, initial_load):
      """
      Retrieving and Loading the data from the Google Trends API regaring the historical interest of defined keywords.
      """

      ## Google Trends API type/kpi
      gt_type = "gt_historical_interest"

      ## Check whether the date is already in the database
      db_table = check_db_date(gt_type)

      ## Set delay days (1: Google Trends results from yesterday, 2: Google Trends results from the day before yesterday etc.)
      DELAY_DAYS = 1
      delay_date = datetime.today() - timedelta(days=1)
      delay_date_trunc = datetime(delay_date.year, delay_date.month, delay_date.day)

      ## Define time period to be loaded
      if db_table[0] == 1:
        if delay_date_trunc in db_table[1]:
          print(f"Data for '{datetime.date(delay_date_trunc)}' is already in the database.")
          return None
        else:
          delay_date_start = delay_date
          delay_date_end = delay_date
      elif db_table[0] == 0:
        print(db_table[1])
        delay_date_start = delay_date - timedelta(days=initial_load)
        delay_date_end = delay_date

      ## Define Google Trends object
      pytrends = TrendReq()

      ## DataFrame template
      df_final = pd.DataFrame({"date": [], "technology": [], "value": []})

      ## Unique combinations of keywords
      combos = list(combinations(kw_list,2))

      for combo in combos:
        df = pytrends.get_historical_interest(list(combo),
                                       year_start=delay_date_start.year,
                                       month_start=delay_date_start.month,
                                       day_start=delay_date_start.day,
                                       hour_start=0,
                                       year_end=delay_date_end.year,
                                       month_end=delay_date_end.month,
                                       day_end=delay_date_end.day,
                                       hour_end=23,
                                       cat=0,
                                       geo="CH",
                                       gprop="",
                                       sleep=60)

        ## Time buffer between requests
        time.sleep(0.3)

        ## Adding retrieved data to existing DataFrame
        try:
          df = df[list(combo)]
          df.reset_index(inplace=True)
          df = df.melt(id_vars=["date"], value_vars=combo, var_name="technology", value_name="value")
          df_final = pd.concat([df_final, df])
        except:
          continue

      ## Mean-Score per technology
      df_final = df_final.groupby(by=["date","technology"]).mean().round()

      ## Load data into database
      df_final.reset_index(inplace=True)
      df_final = df_final[["date", "technology", "value"]]
      load_data(df_final, gt_type, "append")


    def gt_interest_over_time_combo(kw_list):
      """
      Retrieving and Loading the data from the Google Trends API regaring the historical interest of defined key words from the Dev-Jobs function.
      """

      ## Google Trends API type/kpi
      gt_type = "gt_interest_over_time_combo"

      ## Define Google Trends object
      pytrends = TrendReq()

      ## DataFrame template
      df_final = pd.DataFrame({"date": [], "technology": [], "value": []})

      ## Unique combinations of keywords
      combos = list(combinations(kw_list,2))

      for combo in combos:
        ## Get results from Google Trends for each keyword combo
        pytrends.build_payload(kw_list = list(combo)
                               ,geo = "CH"
                               ,timeframe="all")

        ## Interest over time
        df = pytrends.interest_over_time()

        ## Time buffer between requests
        time.sleep(0.3)

        ## Adding retrieved data to existing DataFrame
        try:
          df = df[list(combo)]
          df.reset_index(inplace=True)
          df = df.melt(id_vars=["date"], value_vars=combo, var_name="technology", value_name="value")
          df_final = pd.concat([df_final, df])
        except:
          continue

      ## Mean-Score per technology
      df_final = df_final.groupby(by=["date","technology"]).mean().round()

      ## Load data into database
      df_final.reset_index(inplace=True)
      df_final = df_final[["date", "technology", "value"]]
      load_data(df_final, gt_type, "replace")

    def gt_interest_over_time_single(kw_list):
      """
      Retrieving and Loading the data from the Google Trends API regaring the historical interest of defined key words from the Dev-Jobs function.
      """

      ## Google Trends API type/kpi
      gt_type = "gt_interest_over_time_single"

      ## Define Google Trends object
      pytrends = TrendReq()

      ## DataFrame template
      df_final = pd.DataFrame({"date": [], "technology": [], "value": []})

      for keyword in kw_list:
        ## Get results from Google Trends for each individual keyword
        pytrends.build_payload(kw_list = [keyword]
                               ,geo = "CH"
                               ,timeframe="all")

        ## Interest over time
        df = pytrends.interest_over_time()

        ## Time buffer between requests
        time.sleep(0.3)

        ## Adding retrieved data to existing DataFrame
        try:
          df = df[[keyword]]
          df.reset_index(inplace=True)
          df = df.melt(id_vars=["date"], value_vars=[keyword], var_name="technology", value_name="value")
          df_final = pd.concat([df_final, df])
        except:
          continue

      ## Load data into database
      df_final.reset_index(inplace=True)
      df_final = df_final[["date", "technology", "value"]]
      load_data(df_final, gt_type, "replace")


    def load_data(df, table, load):
      """
      Loading the data from a DataFrame to a database table.
      """

      with engine.begin() as connection:
        df.to_sql(table,
                  con=connection,
                  if_exists=load,
                  method="multi",
                  index=False)
      print(f"Loading process '{table}' complete.")

    # -----------------------

    kw_list = retrieve_top_technologies()
    #gt_historical_interest(kw_list, 3)
    gt_interest_over_time_combo(kw_list)
    gt_interest_over_time_single(kw_list)
