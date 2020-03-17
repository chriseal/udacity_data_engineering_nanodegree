import pandas as pd
import boto3
import psycopg2
import json
import configparser


config = configparser.ConfigParser()
config.read_file(open('/home/chriseal/Documents/airflow.cfg'))

KEY = config.get('AWS','KEY')
SECRET = config.get('AWS','SECRET')

DB_NAME = config.get('REDSHIFT','DB_NAME')
CLUSTER_IDENTIFIER = config.get('REDSHIFT','CLUSTER_IDENTIFIER')
DB_USER = config.get('REDSHIFT','DB_USER')
DB_PASSWORD = config.get('REDSHIFT','DB_PASSWORD')
ENDPOINT = config.get('REDSHIFT','ENDPOINT')
REGION = config.get('REDSHIFT','REGION')
PORT = int(config.get('REDSHIFT','PORT'))

# DB_NAME=dev
# CLUSTER_IDENTIFIER=redshift-cluster-1
# DB_USER=awsuser
# ENDPOINT=redshift-cluster-1.xxxxxxxxxx.xxxxxxxxx.redshift.amazonaws.com
# REGION=us-west-2
# PORT=5439

redshift = boto3.client('redshift', 
    region_name=REGION, 
    aws_access_key_id=KEY, 
    aws_secret_access_key=SECRET 
)
creds = redshift.get_cluster_credentials(
    DbUser=DB_USER,
    ClusterIdentifier=CLUSTER_IDENTIFIER,
    DbName=DB_NAME
)

conn = psycopg2.connect(dbname=DB_NAME, host=ENDPOINT, port=PORT, user=DB_USER, password=DB_PASSWORD)

cur = conn.cursor()
cur.execute("""SELECT * FROM stl_load_errors;""")
errors = cur.fetchall()