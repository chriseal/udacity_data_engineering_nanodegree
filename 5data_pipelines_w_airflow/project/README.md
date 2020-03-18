# Project 5: Data Pipelines with Airflow

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.
They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.
The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Project Overview

This project will introduce you to the core concepts of Apache Airflow. To complete the project, you will need to create your own custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

We have provided you with a project template that takes care of all the imports and provides four empty operators that need to be implemented into functional pieces of a data pipeline. The template also contains a set of tasks that need to be linked to achieve a coherent and sensible data flow within the pipeline.
You'll be provided with a helpers class that contains all the SQL transformations. Thus, you won't need to write the ETL yourselves, but you'll need to execute it with your custom operators.


## Setup virtual environment

```
conda create -n udacity_p5 python=3.7.3 anaconda
conda activate udacity_p5
pip install apache-airflow
pip install boto3
pip install psycopg2-binary
```

## Setup airflow 

- assistance from here: http://michal.karzynski.pl/blog/2017/03/19/developing-workflows-with-apache-airflow/

```
cd ~/Projects/udacity_data_engineering_nanodegree/5data_pipelines_w_airflow/project
export AIRFLOW_HOME=`pwd`
airflow version
airflow initdb
```

- Add connections to Airflow
  - Follow images available in the `./img` folder for AWS and Redshift credentials, respectively

- Add connection to AWS credentials using IAM role
  - Conn Id: aws_credentials
  - Conn Type: Amazon Web Services
  - Login: airflow_redshift_user IAM role, Access Key
  - Password: airflow_redshift_user IAM role, Secret Key
- Add connection to Redshift credentials
  - Conn Id: redshift
  - Conn Type: Postgres
  - Host: (endpoint copied from AWS Redshift)
  - Schema: dev 
  - Login: (redshift DB user)
  - Password: (redshift DB password)
  - Port: 5439

## Running DAG

- In two separate terminals run these shared steps:
```
cd ~/Projects/udacity_data_engineering_nanodegree/5data_pipelines_w_airflow/project
export AIRFLOW_HOME=`pwd`
conda activate udacity_p5
```
- And then, in Terminal 1:
```
airflow webserver
```

- In a new terminal window 
```
airflow scheduler
```

- Access at: http://localhost:8080/