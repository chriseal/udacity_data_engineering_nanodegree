#!/usr/bin/env python
# coding: utf-8

# # Spark SQL Examples
# 
# Run the code cells below. This is the same code from the previous screencast.

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc
from pyspark.sql.functions import asc
from pyspark.sql.functions import sum as Fsum

import datetime

import numpy as np
import pandas as pd
get_ipython().run_line_magic('matplotlib', 'inline')
import matplotlib.pyplot as plt


# In[2]:


spark = SparkSession     .builder     .appName("Data wrangling with Spark SQL")     .getOrCreate()


# In[3]:


path = "data/sparkify_log_small.json"
user_log = spark.read.json(path)


# In[4]:


user_log.take(1)


# In[5]:


user_log.printSchema()


# # Create a View And Run Queries
# 
# The code below creates a temporary view against which you can run SQL queries.

# In[6]:


user_log.createOrReplaceTempView("user_log_table")


# In[7]:


spark.sql("SELECT * FROM user_log_table LIMIT 2").show()


# In[8]:


spark.sql('''
          SELECT * 
          FROM user_log_table 
          LIMIT 2
          '''
          ).show()


# In[9]:


spark.sql('''
          SELECT COUNT(*) 
          FROM user_log_table 
          '''
          ).show()


# In[10]:


spark.sql('''
          SELECT userID, firstname, page, song
          FROM user_log_table 
          WHERE userID == '1046'
          '''
          ).collect()


# In[11]:


spark.sql('''
          SELECT DISTINCT page
          FROM user_log_table 
          ORDER BY page ASC
          '''
          ).show()


# # User Defined Functions

# In[12]:


spark.udf.register("get_hour", lambda x: int(datetime.datetime.fromtimestamp(x / 1000.0).hour))


# In[14]:


spark.sql('''
          SELECT *, get_hour(ts) AS hour
          FROM user_log_table 
          LIMIT 1
          '''
          ).collect()


# In[15]:


songs_in_hour = spark.sql('''
          SELECT get_hour(ts) AS hour, COUNT(*) as plays_per_hour
          FROM user_log_table
          WHERE page = "NextSong"
          GROUP BY hour
          ORDER BY cast(hour as int) ASC
          '''
          )


# In[16]:


songs_in_hour.show()


# # Converting Results to Pandas

# In[17]:


songs_in_hour_pd = songs_in_hour.toPandas()


# In[18]:


print(songs_in_hour_pd)

