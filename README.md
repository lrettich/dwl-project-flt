# dwl-project-flt
Project for HSLU Module Data Warehouse and Data Lake Systems 1 and 2, Project Team FLT

## Description of the repository
* File 'docker-compose.yaml': Docker Compose file to run Apache Airflow on Docker Compose
* Folder 'dags': Contains all the Python code that is needed in connection to the Aiflow DAG.
  * 'dag_skills_data_lake.py': Definition of the Airflow DAG
  * 'job_request.py' & 'top_technologies.py': Code in relation to DevJobs data
  * 'DBConnection.py', 'StackExchangeDataCollector.py', 'stack_exchange_cleaning.py' & 'stack_exchange_handler.py': Code in relation to StackExchange data
  * 'google_trends.py': Code in relation to GoogleTrends data
* Folder 'additional_code': Other code that was used in the project
  * 'lambda_functions/EC2_start_daily.py': Lambda-Function for regularly starting EC2
  * 'data_analysis/ata_analysis_data_lake_StackExchange.ipynb': Jupyter Notebook for analysis of StackExchange data

## Manual for running the code in Apache Airflow
1. Use a linux environment meeting the following requirements: At least two CPU kernels, at least 8GB memory, at least 16GB of free storage
2. Install git, docker and docker-compose on the linux environment
3. Clone this git repository to the linux environment
4. Prepare docker run of airflow with the execution of the following commands in the project directory: 
``mkdir -p ./dags ./logs ./plugins``, ``echo-e "AIRFLOW_UID=$(id -u)"> .env``
5. Initialize Airflow with docker-compose: ``docker-compose up airflow-init``
6. Start Airflow (in background): ``docker-compose up -d``
7. Airflow is now accessible at port 8080 where the DAG 'dag_skills_data_lake' can be activated/started. 
