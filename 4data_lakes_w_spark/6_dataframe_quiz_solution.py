#!/usr/bin/env python
# coding: utf-8

# # Answer Key to the Data Wrangling with DataFrames Coding Quiz
# 
# Helpful resources:
# http://spark.apache.org/docs/latest/api/python/pyspark.sql.html

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import isnan, count, when, col, desc, udf, col, sort_array, asc, avg
from pyspark.sql.functions import sum as Fsum
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType


# In[2]:


# 1) import any other libraries you might need
# 2) instantiate a Spark session 
# 3) read in the data set located at the path "data/sparkify_log_small.json"
# 4) write code to answer the quiz questions 

spark = SparkSession     .builder     .appName("Data Frames practice")     .getOrCreate()

df = spark.read.json("data/sparkify_log_small.json")


# # Question 1
# 
# Which page did user id "" (empty string) NOT visit?

# In[3]:


df.printSchema()


# In[4]:


# filter for users with blank user id
blank_pages = df.filter(df.userId == '')     .select(col('page')     .alias('blank_pages'))     .dropDuplicates()

# get a list of possible pages that could be visited
all_pages = df.select('page').dropDuplicates()

# find values in all_pages that are not in blank_pages
# these are the pages that the blank user did not go to
for row in set(all_pages.collect()) - set(blank_pages.collect()):
    print(row.page)


# # Question 2 - Reflect
# 
# What type of user does the empty string user id most likely refer to?
# 

# Perhaps it represents users who have not signed up yet or who are signed out and are about to log in.

# # Question 3
# 
# How many female users do we have in the data set?

# In[5]:


df.filter(df.gender == 'F')     .select('userId', 'gender')     .dropDuplicates()     .count()


# # Question 4
# 
# How many songs were played from the most played artist?

# In[6]:


df.filter(df.page == 'NextSong')     .select('Artist')     .groupBy('Artist')     .agg({'Artist':'count'})     .withColumnRenamed('count(Artist)', 'Artistcount')     .sort(desc('Artistcount'))     .show(1)


# # Question 5 (challenge)
# 
# How many songs do users listen to on average between visiting our home page? Please round your answer to the closest integer.
# 
# 

# In[7]:


# TODO: filter out 0 sum and max sum to get more exact answer

function = udf(lambda ishome : int(ishome == 'Home'), IntegerType())

user_window = Window     .partitionBy('userID')     .orderBy(desc('ts'))     .rangeBetween(Window.unboundedPreceding, 0)

cusum = df.filter((df.page == 'NextSong') | (df.page == 'Home'))     .select('userID', 'page', 'ts')     .withColumn('homevisit', function(col('page')))     .withColumn('period', Fsum('homevisit').over(user_window))

cusum.filter((cusum.page == 'NextSong'))     .groupBy('userID', 'period')     .agg({'period':'count'})     .agg({'count(period)':'avg'}).show()

