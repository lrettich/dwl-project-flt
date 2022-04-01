import requests
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime,timedelta
from sqlalchemy import create_engine
import psycopg2
import os


# load credentials
USER = os.environ["USER"]
PASSWORD = os.environ["PASSWORD"]
ENDPOINT = os.environ["ENDPOINT"]
PORT = os.environ["PORT"]
DATABASENAME = os.environ["DATABASENAME"]

# requst url
URL = "https://swissdevjobs.ch/job_feed.xml"
# set delay days (1: jobs posted yesterday, 2: jobs posted day before yesterday etc.)
DELAY_DAYS = 1


def job_request():
    """
    Requests URL and checks for new job description. If any new jobs existing, store their attributes into DB
    """

    # define request, job attributes extraction and data transformation

    # do reuqest and return xml string
    def xml_request(url):
        try:
            response = requests.get(url)
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)

        return ET.fromstring(response.content)


    # extract desired attributes from xml string and return as key value pairs 
    def get_attributes(xml_root):
        attribute_dict = {}

        # get publication date
        pub_date_ls = []
        for pub_date in xml_root.findall("./job/pubdate"):
            pub_date_ls.append(pub_date.text)
        attribute_dict["pub_date"] = pub_date_ls

        # get all job ids
        job_id_ls = []
        for job in xml_root:
            job_id_ls.append(job.attrib["id"])
        attribute_dict["job_id"] = job_id_ls

        # get all salary declarations
        salary_range_ls = []
        for salary in xml_root.findall("./job/salary"):
            salary_range_ls.append(salary.text)
        attribute_dict["salary_range"] = salary_range_ls

        # get all job descriptions
        job_description = xml_root.findall("./job/description")
        # get list of skills for each job
        technologies_ls = []
        for description in job_description:
            description_html = description.text
            soup = BeautifulSoup(description_html)
            # element of interst is 6th <p> tag in html job description soup
            result = [html_p.text for idx, html_p in enumerate(soup.findAll('p')) if idx == 6]
            technologies_ls.extend(result)
        attribute_dict["technology"] = technologies_ls

        return attribute_dict


    # data wrangling and clean up
    def transform_data(df):
        # ignore first dash and first whitespace in technology string to enable porpper splitting
        df.technology = df.technology.apply(lambda x: x[2:])
        # split technologies items
        df.technology = df.technology.str.split("- ")
        # explode technologies lists 
        df = df.explode("technology").reset_index(drop=True)

        # create lower bound salary column: take first number in salary_range item 
        df["salary_lower_bound"] = df.salary_range.apply(lambda x: int(x.split(" - ")[0].replace("’", "")))
        # create upper bound salary column: take second number in salary_range item 
        df["salary_upper_bound"] = df.salary_range.apply(lambda x: int(x.split(" - ")[1].split(" ")[0].replace("’", "")))
        # create avg salary value
        df["salary_avg"] = (df.salary_upper_bound + df.salary_lower_bound) / 2

        # add date of request as meta data column
        df["request_date"] = datetime.now()

        return df

    # -----------------------

    # do requst and get all job elements as element tree
    root_jobs = xml_request(URL)


    # set job publishing date to check for new job descriptions
    delay_date = datetime.today() - timedelta(days=DELAY_DAYS)
    PUBLISHING_DATE = delay_date.strftime("%d.%m.%Y")


    # check if new jobs were published
    for job in root_jobs.findall("job"):
        pubdate = job.find("pubdate").text
        if pubdate != PUBLISHING_DATE:  # e.g. "18.03.2022"
            root_jobs.remove(job)
        else:
            pass


    # if there are new jobs, apply functions to get job attributes and store data as df in db
    if len(root_jobs) != 0:
        job_attributes = get_attributes(root_jobs)
        df_raw = pd.DataFrame(job_attributes)
        df= transform_data(df_raw)

        SQLALCHEMY_DATABASE_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASENAME}"

        engine = create_engine(SQLALCHEMY_DATABASE_URL)

        df.to_sql("dev_jobs_1", con=engine, index=False, if_exists="append")
