# Sparkify Redshift ETL Project

1. Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.
2. State and justify your database schema design and ETL pipeline.
3. [Optional] Provide example queries and results for song play analysis.

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project Description

In this project, you'll apply what you've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, you will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

## 1. Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.

The Sparkify analytical team is most interested in delving more deeply into insights regarding the songplay in their app. This ETL+Data Warehousing process focuses on performance and depth around the songplay dataset. It does this by creating a songplay fact table with IDs that can be quickly joined to supporting dimension tables, such as time, users, artists, and songs, 

## 2. State and justify your database schema design and ETL pipeline.

The following schema printout of the songplay fact table DWH has been adapted from `manage_cluster.ipynb`:

### Staging environment

These tables concatenate individual files available in S3 for Events and Song, respectively.

#### staging_events
```
+----------+---------------+-----------------------------+------------+-------------+---------------+
| position |  column_name  |          data_type          | max_length | is_nullable | default_value |
+----------+---------------+-----------------------------+------------+-------------+---------------+
|    1     |     artist    |      character varying      |    256     |     YES     |      None     |
|    2     |      auth     |      character varying      |    256     |     YES     |      None     |
|    3     |   firstname   |      character varying      |    256     |     YES     |      None     |
|    4     |     gender    |          character          |     1      |     YES     |      None     |
|    5     | iteminsession |           integer           |     32     |     YES     |      None     |
|    6     |    lastname   |      character varying      |    256     |     YES     |      None     |
|    7     |     length    |       double precision      |     53     |     YES     |      None     |
|    8     |     level     |      character varying      |    256     |     YES     |      None     |
|    9     |    location   |      character varying      |    256     |     YES     |      None     |
|    10    |     method    |      character varying      |    256     |     YES     |      None     |
|    11    |      page     |      character varying      |    256     |     YES     |      None     |
|    12    |  registration |       double precision      |     53     |     YES     |      None     |
|    13    |   sessionid   |           integer           |     32     |     YES     |      None     |
|    14    |      song     |      character varying      |    256     |     YES     |      None     |
|    15    |     status    |           integer           |     32     |     YES     |      None     |
|    16    |       ts      | timestamp without time zone |    None    |     YES     |      None     |
|    17    |   useragent   |      character varying      |    256     |     YES     |      None     |
|    18    |     userid    |           integer           |     32     |     YES     |      None     |
+----------+---------------+-----------------------------+------------+-------------+---------------+
```

#### staging_songs
```
+----------+------------------+-------------------+------------+-------------+---------------+
| position |   column_name    |     data_type     | max_length | is_nullable | default_value |
+----------+------------------+-------------------+------------+-------------+---------------+
|    1     |    num_songs     |      integer      |     32     |     YES     |      None     |
|    2     |    artist_id     | character varying |    256     |     YES     |      None     |
|    3     | artist_latitude  |  double precision |     53     |     YES     |      None     |
|    4     | artist_longitude |  double precision |     53     |     YES     |      None     |
|    5     | artist_location  | character varying |    256     |     YES     |      None     |
|    6     |   artist_name    | character varying |    256     |     YES     |      None     |
|    7     |     song_id      | character varying |    256     |     YES     |      None     |
|    8     |      title       | character varying |    256     |     YES     |      None     |
|    9     |     duration     |  double precision |     53     |     YES     |      None     |
|    10    |       year       |      integer      |     32     |     YES     |      None     |
+----------+------------------+-------------------+------------+-------------+---------------+
```

### Songplay STAR Schema

#### songplay FACT TABLE

```
+----------+-------------+-----------------------------+------------+-------------+------------------------------------+
| position | column_name |          data_type          | max_length | is_nullable |           default_value            |
+----------+-------------+-----------------------------+------------+-------------+------------------------------------+
|    1     | songplay_id |           integer           |     32     |      NO     | "identity"(100501, 0, '0,1'::text) |
|    2     |  start_time | timestamp without time zone |    None    |      NO     |                None                |
|    3     |   user_id   |           integer           |     32     |      NO     |                None                |
|    4     |    level    |      character varying      |    256     |     YES     |                None                |
|    5     |   song_id   |      character varying      |    256     |      NO     |                None                |
|    6     |  artist_id  |      character varying      |    256     |      NO     |                None                |
|    7     |  session_id |           integer           |     32     |      NO     |                None                |
|    8     |   location  |      character varying      |    256     |     YES     |                None                |
|    9     |  user_agent |      character varying      |    256     |     YES     |                None                |
+----------+-------------+-----------------------------+------------+-------------+------------------------------------+
```

- songplay_id is the PRIMARY KEY and is auto-incremented. (Note: Redshift does not enforce a 'Primary Key', but this designation may improve query performance, as well as help with communicating the structure/role of various columns in the table.)
- Since the Fact table is large and analysts will often sort by time, `start_time` is the SORT and DISTKEY, respectively.

#### users DIMENSION
```
+----------+-------------+-------------------+------------+-------------+---------------+
| position | column_name |     data_type     | max_length | is_nullable | default_value |
+----------+-------------+-------------------+------------+-------------+---------------+
|    1     |   user_id   |      integer      |     32     |      NO     |      None     |
|    2     |  first_name | character varying |    256     |      NO     |      None     |
|    3     |  last_name  | character varying |    256     |      NO     |      None     |
|    4     |    gender   |     character     |     1      |     YES     |      None     |
|    5     |    level    | character varying |    256     |     YES     |      None     |
+----------+-------------+-------------------+------------+-------------+---------------+
```

- The `users` table has a compound SORTKEY consiting of `(last_name, first_name)`, which replicates how the business users will most often sort this table
- It is a relatively small table, and therefore, does not need a DISTKEY

#### songs DIMENSION
```
+----------+-------------+-------------------+------------+-------------+---------------+
| position | column_name |     data_type     | max_length | is_nullable | default_value |
+----------+-------------+-------------------+------------+-------------+---------------+
|    1     |   song_id   | character varying |    256     |      NO     |      None     |
|    2     |    title    | character varying |    256     |      NO     |      None     |
|    3     |  artist_id  | character varying |    256     |      NO     |      None     |
|    4     |     year    |      integer      |     32     |     YES     |      None     |
|    5     |   duration  |        real       |     24     |     YES     |      None     |
+----------+-------------+-------------------+------------+-------------+---------------+
```

- The `songs` table has a compound sortkey - `COMPOUND SORTKEY(artist_id, song_id, year, title)` - which replicates how users would like to see their results. This will slow data ingest, but help optimize query performance
- It is not the smallest table, but is small enough to not need a DISTKEY

#### artists DIMENSION
```
+----------+-------------+-------------------+------------+-------------+---------------+
| position | column_name |     data_type     | max_length | is_nullable | default_value |
+----------+-------------+-------------------+------------+-------------+---------------+
|    1     |  artist_id  | character varying |    256     |      NO     |      None     |
|    2     |     name    | character varying |    256     |     YES     |      None     |
|    3     |   location  | character varying |    256     |     YES     |      None     |
|    4     |   latitude  |  double precision |     53     |     YES     |      None     |
|    5     |  longitude  |  double precision |     53     |     YES     |      None     |
+----------+-------------+-------------------+------------+-------------+---------------+
```

- The `name` is the SORTKEY, representing a natural way the users will want to see their data
- It is a relatively small table, and therefore, does not need a DISTKEY

#### time DIMENSION
```
+----------+-------------+-----------------------------+------------+-------------+---------------+
| position | column_name |          data_type          | max_length | is_nullable | default_value |
+----------+-------------+-----------------------------+------------+-------------+---------------+
|    1     |  start_time | timestamp without time zone |    None    |      NO     |      None     |
|    2     |     hour    |           smallint          |     16     |      NO     |      None     |
|    3     |     day     |           smallint          |     16     |      NO     |      None     |
|    4     |     week    |           smallint          |     16     |      NO     |      None     |
|    5     |    month    |           smallint          |     16     |      NO     |      None     |
|    6     |     year    |           smallint          |     16     |      NO     |      None     |
|    7     |   weekday   |           smallint          |     16     |      NO     |      None     |
+----------+-------------+-----------------------------+------------+-------------+---------------+
```

- `start_time` is the main column in the `time` table, representing the PRIMARY KEY, DISTKEY, and SORTKEY, respectively.
- the `time` table may end up being as close to as large as the `songplay` table, so the DISTKEY keeps similar rows of each on the same slice in the cluster, in order to optimize query performance

### ETL Pipeline

- The raw files are housed independently in S3. 
- Because this is an exercise, the script first deletes all relevant tables, then creates them again from scratch (`create_tables.py`)
- The ETL script first loads all files into `staging_events` or `staging_songs`, respectively, to: normalize column names, correct some data types, ignore duplicates where appropriate, and otherwise concatenate all relevant files into one table each. (`etl.py`)
- Once files are loaded into staging tables, the script executes a series of insertions into the various tables in the STAR schema.

## 3. [Optional] Provide example queries and results for song play analysis.

