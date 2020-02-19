#!/usr/bin/env python
# coding: utf-8

# # Exercise 2: Advanced Analytics NLP

# In[1]:


get_ipython().system('pip install spark-nlp==1.7.3')


# In[2]:


import pandas as pd
pd.set_option('max_colwidth', 800)


# # Create a spark context that includes a 3rd party jar for NLP

# In[3]:


#jarPath = "spark-nlp-assembly-1.7.3.jar"

from pyspark.sql import SparkSession
spark = SparkSession.builder     .config("spark.jars.packages", "JohnSnowLabs:spark-nlp:1.8.2")     .getOrCreate()
spark


# # Read multiple files in a dir as one Dataframe

# In[4]:


dataPath = "./data/reddit/*.json"
df = spark.read.json(dataPath)
print(df.count())
df.printSchema()


# # Deal with Struct type to query subfields 

# In[5]:


title = "data.title"
author = "data.author"
dfAuthorTilte = df.select(title, author)
dfAuthorTilte.limit(5).toPandas()


# # Try to implement the equivalent of flatMap in dataframes

# In[6]:


import pyspark.sql.functions as F

dfWordCount = df.select(
    F.explode(
        F.split(title,"\\s+")).alias("word")
    ).groupBy("word").count().orderBy(F.desc("count"))
dfWordCount.limit(10).toPandas()


# # Use an NLP libary to do Part-of-Speech Tagging

# In[7]:


from com.johnsnowlabs.nlp.pretrained.pipeline.en import BasicPipeline as bp
dfAnnotated = bp.annotate(dfAuthorTilte, "title")
dfAnnotated.printSchema()


# ## Deal with Map type to query subfields

# In[9]:


dfPos = dfAnnotated.select("text", "pos.metadata", "pos.result")
dfPos.limit(5).toPandas()


# In[10]:


dfPos= dfAnnotated.select(F.explode("pos").alias("pos"))
dfPos.printSchema()
dfPos.toPandas()


# ## Keep only proper nouns NNP or NNPS

# In[12]:


nnpFilter = "pos.result = 'NNP' or pos.result = 'NNPS' "
dfNNP = dfPos.where(nnpFilter)
dfNNP.limit(10).toPandas()


# ## Extract columns form a map in a col

# In[13]:


dfWordTag = dfNNP.selectExpr("pos.metadata['word'] as word", "pos.result as tag")
dfWordTag.limit(10).toPandas()


# In[14]:


from pyspark.sql.functions import desc
dfWordTag.groupBy("word").count().orderBy(desc("count")).show()

