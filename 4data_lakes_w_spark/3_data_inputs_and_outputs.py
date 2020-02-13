#!/usr/bin/env python
# coding: utf-8

# # Reading and Writing Data with Spark
# 
# This notebook contains the code from the previous screencast. The only difference is that instead of reading in a dataset from a remote cluster, the data set is read in from a local file. You can see the file by clicking on the "jupyter" icon and opening the folder titled "data".
# 
# Run the code cell to see how everything works. 
# 
# First let's import SparkConf and SparkSession

# In[1]:


import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession


# Since we're using Spark locally we already have both a sparkcontext and a sparksession running. We can update some of the parameters, such our application's name. Let's just call it "Our first Python Spark SQL example"

# In[2]:


spark = SparkSession     .builder     .appName("Our first Python Spark SQL example")     .getOrCreate()


# Let's check if the change went through

# In[3]:


spark.sparkContext.getConf().getAll()


# In[4]:


spark


# As you can see the app name is exactly how we set it
# 
# Let's create our first dataframe from a fairly small sample data set. Througout the course we'll work with a log file data set that describes user interactions with a music streaming service. The records describe events such as logging in to the site, visiting a page, listening to the next song, seeing an ad.

# In[5]:


path = "data/sparkify_log_small.json"
user_log = spark.read.json(path)


# In[6]:


user_log.printSchema()


# In[7]:


user_log.describe()


# In[8]:


user_log.show(n=1)


# In[9]:


user_log.take(5)


# In[10]:


out_path = "data/sparkify_log_small.csv"


# In[11]:


user_log.write.save(out_path, format="csv", header=True)


# In[12]:


user_log_2 = spark.read.csv(out_path, header=True)


# In[13]:


user_log_2.printSchema()


# In[14]:


user_log_2.take(2)


# In[15]:


user_log_2.select("userID").show()


# In[16]:


user_log_2.take(1)

