# Job Skills Analysis
An ETL pipeline including use of Airflow, AWS EC2 VM, AWS RDS and Docker.

## Goal
Extract, transform and load data to find out which IT skills i.e. technologies are most popualar in development community and which are the most demanded on job market.

## Data sources
* Google trends API: Data on which technologies are popular among a broad audience
* Stack Overflow API: Data on which technologies developer community is discussing the most
* SwissDevJobs: Data on which technologies are demanded the most on job market

## Tech stack
![tech stack](/img/tech_stack.png)

## Repository content
* docker-compose.yaml: Docker Compose file to run Apache Airflow within Docker container
* Folder 'additional_code': First steps, exploratory analysis, and AWS lambda function to trigger EC2 instance
* Folder 'dags': Aiflow tasks and Aiflow DAG.
  * google_%.py: Google trends related tasks
  * job_%.py: Develper job market related tasks
  * stack_%.py: Stack exchange related tasks
  * DBConnection.py: Database connection class (supports stack exchange workflow)
  * top_technologies.py: Get top technologies as starting point
  * merging_for_dwh.py: Merging data and store in DWH as analysis base
  * dag_skills_data_lake.py: Construction of DAG
  ![DAG](/img/dag.png)

## Starting ETL Service
1. Use linux environment. Minimal required capacities:
  * 2 CPU kernels
  * 8GB memory
  * 16GB of storage
2. Install git, docker and docker-compose
3. Clone this repository to the linux environment
4. Prepare docker run of airflow container by executing following commands in the project directory: 
``mkdir -p ./dags ./logs ./plugins`` <br>
``echo-e "AIRFLOW_UID=$(id -u)"> .env``
5. Initialize Airflow with docker-compose: ``docker-compose up airflow-init``
6. Start Airflow (in background): ``docker-compose up -d``
7. Airflow is now accessible at port 8080 where the DAG 'dag_skills_data_lake' can be started. 

## Visualization of analysis outcomes
Two Tableau dashboard views are made to get insights regarding popular technologies among developer community and high demanded skills on job market.
https://public.tableau.com/app/profile/filip.maric/viz/Tech_trends_RQ_1_2/RQ_1_2
https://public.tableau.com/app/profile/filip.maric/viz/Tech_trends_RQ_3/RQ_3
