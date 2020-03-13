# Project 5 - Airflow


## Setup virtual environment

```
conda create -n udacity_p5 python=3.7.3 anaconda
conda activate udacity_p5
pip install apache-airflow
```

## Setup airflow 

- assistance from here: http://michal.karzynski.pl/blog/2017/03/19/developing-workflows-with-apache-airflow/

```
cd ~/Projects/udacity_data_engineering_nanodegree/5data_pipelines_w_airflow/project
export AIRFLOW_HOME=`pwd`
airflow version
airflow initdb
airflow webserver
```

- if needed `pip install psycopg2-binary` or `sudo apt-get install build-dep python-psycopg2 && pip install psycopg2`

- Add connections to Airflow
  - Follow images available in the `./img` folder for AWS and Redshift credentials, respectively


## Running DAG

- In a new terminal window 
```
cd ~/Projects/udacity_data_engineering_nanodegree/5data_pipelines_w_airflow/project
export AIRFLOW_HOME=`pwd`
conda activate udacity_p5
airflow scheduler
```

- Access at: http://localhost:8080/