#!/usr/bin/env python
# coding: utf-8

# # Exercise 1: Schema on Read

# In[1]:


from pyspark.sql import SparkSession
import pandas as pd
import matplotlib


# In[2]:


spark = SparkSession.builder.getOrCreate()


# In[3]:


dfLog = spark.read.text("data/NASA_access_log_Jul95.gz")


# # Load the dataset

# In[4]:


#Data Source: http://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
dfLog = spark.read.text("data/NASA_access_log_Jul95.gz")


# # Quick inspection of  the data set

# In[5]:


# see the schema
dfLog.printSchema()


# In[6]:


# number of lines
dfLog.count()


# In[8]:


#what's in there? 
dfLog.show(5)


# In[7]:


#a better show?
dfLog.show(5, truncate=False)


# In[8]:


#pandas to the rescue
pd.set_option('max_colwidth', 200)
dfLog.limit(5).toPandas()


# # Let' try simple parsing with split

# In[9]:


from pyspark.sql.functions import split
dfArrays = dfLog.withColumn("tokenized", split("value"," "))
dfArrays.limit(10).toPandas()


# # Second attempt, let's build a custom parsing UDF 

# In[10]:


from pyspark.sql.functions import udf

@udf
def parseUDF(line):
    import re
    PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)'
    match = re.search(PATTERN, line)
    if match is None:
        return (line, 0)
    size_field = match.group(9)
    if size_field == '-':
        size = 0
    else:
        size = match.group(9)
    return {
        "host"          : match.group(1), 
        "client_identd" : match.group(2), 
        "user_id"       : match.group(3), 
        "date_time"     : match.group(4), 
        "method"        : match.group(5),
        "endpoint"      : match.group(6),
        "protocol"      : match.group(7),
        "response_code" : int(match.group(8)),
        "content_size"  : size
    }


# In[11]:


#Let's start from the beginning
dfParsed= dfLog.withColumn("parsed", parseUDF("value"))
dfParsed.limit(10).toPandas()


# In[12]:


dfParsed.printSchema()


# # Third attempt, let's fix our UDF

# In[13]:


#from pyspark.sql.functions import udf # already imported
from pyspark.sql.types import MapType, StringType

@udf(MapType(StringType(),StringType()))
def parseUDFbetter(line):
    import re
    PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)'
    match = re.search(PATTERN, line)
    if match is None:
        return (line, 0)
    size_field = match.group(9)
    if size_field == '-':
        size = 0
    else:
        size = match.group(9)
    return {
        "host"          : match.group(1), 
        "client_identd" : match.group(2), 
        "user_id"       : match.group(3), 
        "date_time"     : match.group(4), 
        "method"        : match.group(5),
        "endpoint"      : match.group(6),
        "protocol"      : match.group(7),
        "response_code" : int(match.group(8)),
        "content_size"  : size
    }


# In[14]:


#Let's start from the beginning
dfParsed= dfLog.withColumn("parsed", parseUDFbetter("value"))
dfParsed.limit(10).toPandas()


# In[15]:


#Let's start from the beginning
dfParsed= dfLog.withColumn("parsed", parseUDFbetter("value"))
dfParsed.limit(10).toPandas()


# In[16]:


#Bingo!! we'got a column of type map with the fields parsed
dfParsed.printSchema()


# In[17]:


dfParsed.select("parsed").limit(10).toPandas()


# # Let's build separate columns

# In[18]:


dfParsed.selectExpr("parsed['host'] as host").limit(5).show(5)


# In[19]:


dfParsed.selectExpr(["parsed['host']", "parsed['date_time']"]).show(5)


# In[20]:


fields = ["host", "client_identd","user_id", "date_time", "method", "endpoint", "protocol", "response_code", "content_size"]
exprs = [ "parsed['{}'] as {}".format(field,field) for field in fields]
exprs


# In[21]:


dfClean = dfParsed.selectExpr(*exprs)
dfClean.limit(5).toPandas()


# ## Popular hosts

# In[22]:


from pyspark.sql.functions import desc
dfClean.groupBy("host").count().orderBy(desc("count")).limit(10).toPandas()


# ## Popular content

# In[23]:


from pyspark.sql.functions import desc
dfClean.groupBy("endpoint").count().orderBy(desc("count")).limit(10).toPandas()


# ## Large Files

# In[24]:


dfClean.createOrReplaceTempView("cleanlog")
spark.sql("""
select endpoint, content_size
from cleanlog 
order by content_size desc
""").limit(10).toPandas()


# In[25]:


from pyspark.sql.functions import expr
dfCleanTyped = dfClean.withColumn("content_size_bytes", expr("cast(content_size  as int)"))
dfCleanTyped.limit(5).toPandas()


# In[26]:


dfCleanTyped.createOrReplaceTempView("cleantypedlog")
spark.sql("""
select endpoint, content_size
from cleantypedlog 
order by content_size_bytes desc
""").limit(10).toPandas()


# In[196]:


# Left for you, clean the date column :)
# 1- Create a udf that parses that weird format,
# 2- Create a new column with a data tiem string that spark would understand
# 3- Add a new date-time column properly typed
# 4- Print your schema


# In[28]:


dfCleanTyped.selectExpr("date_time").show(5)


# In[29]:


dfCleanTyped.printSchema()


# In[31]:


from pyspark.sql.functions import to_timestamp
dfDateCleaned = dfClean.withColumn("clean_ts", to_timestamp("date_time", "dd/MMM/yyyy:HH:mm:ss -SSS"))
dfDateCleaned.printSchema()

