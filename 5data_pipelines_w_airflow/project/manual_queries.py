import pandas as pd
import boto3
import json
import configparser


config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')


redshift = boto3.client('redshift',
                       region_name='us-west-2',
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )