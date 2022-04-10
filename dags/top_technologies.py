from sqlalchemy import create_engine
from airflow.models import Variable

## Load credentials
USER = Variable.get("DJ_USER")
PASSWORD = Variable.get("DJ_PASSWORD")
ENDPOINT = Variable.get("DJ_ENDPOINT")
PORT = Variable.get("DJ_PORT")
DATABASENAME = Variable.get("DJ_DATABASENAME")

## Define database connection
url = f"postgresql://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASENAME}"
engine = create_engine(url)

def update_table():
    """
    Import top x technologies into a table based on tags in job descriptions.
    """

    ## Only use technologies with a certain amount of appearances in job tags (top x technologies)
    top_tech = 10

    sql = f"""
    CREATE TABLE IF NOT EXISTS TOP_TECHNOLOGIES AS
    SELECT
      technology
      ,CURRENT_TIMESTAMP import_ts
      ,COUNT(*) COUNT
    FROM dev_jobs_1
    GROUP BY
      technology
    ORDER BY
      COUNT(*) DESC
    LIMIT {top_tech};

    INSERT INTO TOP_TECHNOLOGIES (
    SELECT
      base.technology
      ,CURRENT_TIMESTAMP import_ts
      ,base.count
    FROM (SELECT
          dev.technology
          ,COUNT(dev.job_id) COUNT
        FROM dev_jobs_1 dev
        GROUP BY
          dev.technology
        ORDER BY
          COUNT(dev.job_id) DESC
        LIMIT {top_tech}) base
      LEFT JOIN top_technologies top
        ON top.technology = base.technology
    WHERE 1=1
      AND top.technology IS NULL)
    """

    with engine.connect() as conn:
      with conn.begin():
        conn.execute(sql)


def get_kw_list():
    """
    Retrieve a list of top technologies.
    """

    sql = """
    SELECT
      technology
    FROM top_technologies
    """

    with engine.connect() as conn:
      with conn.begin():
        result = conn.execute(sql)

    technologies = []
    for row in result:
      technologies.append(row[0])

    return technologies
