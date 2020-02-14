#!/usr/bin/env python
# coding: utf-8

# # Data Wrangling with DataFrames Coding Quiz
# 
# Use this Jupyter notebook to find the answers to the quiz in the previous section. There is an answer key in the next part of the lesson.

# In[9]:


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

# TODOS: 
# 1) import any other libraries you might need
# 2) instantiate a Spark session 
# 3) read in the data set located at the path "data/sparkify_log_small.json"
# 4) write code to answer the quiz questions 

spark = SparkSession     .builder     .appName("Wrangling Data")     .getOrCreate()

path = "data/sparkify_log_small.json"
user_log = spark.read.json(path)


# # Question 1
# 
# Which page did user id "" (empty string) NOT visit?

# In[10]:


# TODO: write your code to answer question 1
user_log.select(["userId", "page"]).where(user_log.userId == "").dropDuplicates().collect()


# # Question 2 - Reflect
# 
# What type of user does the empty string user id most likely refer to?
# 

# In[16]:


# TODO: use this space to explore the behavior of the user with an empty string
user_log.filter(user_log.userId == "").groupby(user_log.auth).count().show()


# # Question 3
# 
# How many female users do we have in the data set?

# In[20]:


# TODO: write your code to answer question 3
user_log.select(['userId','gender']).dropDuplicates().groupby("gender").count().show()


# # Question 4
# 
# How many songs were played from the most played artist?

# In[22]:


# TODO: write your code to answer question 4
user_log.filter(user_log.page=='NextSong').groupby("artist").count().orderBy(desc("count")).head()


# # Question 5 (challenge)
# 
# How many songs do users listen to on average between visiting our home page? Please round your answer to the closest integer.
# 
# 

# In[ ]:


# TODO: write your code to answer question 5

