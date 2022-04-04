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

def import_top_technologies():
    """
    Import top x technologies into a table based on tags in job descriptions.
    """

    ## Only use technologies with a certain amount of appearances in job tags
    threshold = 120

    sql = f"""
    CREATE TABLE IF NOT EXISTS TOP_TECHNOLOGIES AS
    SELECT
      technology
      ,CURRENT_TIMESTAMP import_ts
      ,COUNT(*) COUNT
    FROM dev_jobs_1
    GROUP BY
      technology
    HAVING
      COUNT(*) >= {threshold}
    ORDER BY
      COUNT(*) DESC;

    INSERT INTO TOP_TECHNOLOGIES (
    SELECT
      dev.technology
      ,CURRENT_TIMESTAMP import_ts
      ,COUNT(dev.job_id) COUNT
    FROM dev_jobs_1 dev
      LEFT JOIN top_technologies tt
        on tt.technology = dev.technology
    WHERE 1=1
      AND tt.technology IS NULL
    GROUP BY
      dev.technology
    HAVING
      COUNT(dev.job_id) >= {threshold}
    ORDER BY
      COUNT(dev.job_id) DESC);
    """

    with engine.connect() as conn:
      with conn.begin():
        conn.execute(sql)


def retrieve_top_technologies():
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
