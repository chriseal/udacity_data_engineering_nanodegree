#!/usr/bin/env python
# coding: utf-8

# # Data Wrangling with Spark SQL Quiz
# 
# This quiz uses the same dataset and most of the same questions from the earlier "Quiz - Data Wrangling with Data Frames Jupyter Notebook." For this quiz, however, use Spark SQL instead of Spark Data Frames.

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

spark = SparkSession     .builder     .appName("Data wrangling with Spark SQL")     .getOrCreate()

path = "data/sparkify_log_small.json"
user_log = spark.read.json(path)
# TODOS: 
# 1) import any other libraries you might need
# 2) instantiate a Spark session 
# 3) read in the data set located at the path "data/sparkify_log_small.json"
# 4) create a view to use with your SQL queries
# 5) write code to answer the quiz questions 


# # Question 1
# 
# Which page did user id ""(empty string) NOT visit?

# In[2]:


user_log.createOrReplaceTempView("user_log_table")


# In[4]:


# TODO: write your code to answer question 1
spark.sql("""
    SELECT DISTINCT page
    FROM user_log_table
    WHERE userId=""
""").show()


# # Question 2 - Reflect
# 
# Why might you prefer to use SQL over data frames? Why might you prefer data frames over SQL?

# # Question 3
# 
# How many female users do we have in the data set?

# In[10]:


# TODO: write your code to answer question 3
spark.sql("""
    SELECT gender, COUNT(gender)
    FROM (
        SELECT DISTINCT(userId), gender
        FROM user_log_table
    ) GROUP BY gender
""").show()


# # Question 4
# 
# How many songs were played from the most played artist?

# In[23]:


# TODO: write your code to answer question 4
spark.sql("""
SELECT artist, COUNT(artist) as play_count
FROM user_log_table
WHERE page='NextSong'
GROUP BY artist
ORDER BY play_count DESC
LIMIT 1
""").show()


# # Question 5 (challenge)
# 
# How many songs do users listen to on average between visiting our home page? Please round your answer to the closest integer.

# In[ ]:


# TODO: write your code to answer question 5

