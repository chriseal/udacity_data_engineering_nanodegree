# Data Lake with Spark and AWS

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.
You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project Description

In this project, you'll apply what you've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, you will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. You'll deploy this Spark process on a cluster using AWS.
Project Datasets

You'll be working with two datasets that reside in S3. Here are the S3 links for each:
- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data

## How to run

To run this program, enter your AWS credentials in dl.cfg, create an S3 bucket named "s3a://udacity-dend-cseal/data-lake/", and run etl.py (such as `python etl.py`)

## Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.

The Sparkify analytical team is most interested in delving more deeply into insights regarding the songplay in their app. Due to growth, they have decided to implement a data lake in the cloud to leverage scalable AWS+Spark architecture. This distributed ETL process focuses on performance and depth around the songplay dataset. It does this by creating a songplay fact table with IDs that can be quickly joined to supporting dimension tables, such as time, users, artists, and songs. Tables are saved into partitions for faster querying across a cluster. Sparkify is a small team and went with the S3+Spark data lake option to take some pressure off the IT team and save money at a cost of slightly less performant database.

## State and justify your database schema design and ETL pipeline.

### Schema

The fact and dimension tables match those created in previous projects, but adapted to Spark+AWS in a scalable big data environment. The relevant tables are saved to columnar-style parquet files on S3 and partitioned where appropriate to match anticipated querying.

### ETL Pipeline

- The raw files are housed independently in S3.
- The ETL script is housed in `etl.py`'s `main()` function, which in turn calls `process_song_data` and `process_log_data`
- `process_song_data`: loads raw song json files, processes song and artists dimensions, and saves them to separate S3 buckets (song table partitioned by "year" and "artist_id")
- `process_log_data`: loads raw log and song json files, processes and saves users, time (partitioned by year and month), and songplays tables (partitioned by year and month) to S3

## [Optional] Provide example queries and results for song play analysis.
