import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType as ST, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, TimestampType as Tmstp


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """Initialize a session in Spark."""
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    return spark


def load_song_jsons(fpath, spark):
    """ Reads the raw song json files and returns a Spark object 
    
    Args:
        fpath (str): the crawlable filepath on which the song json files reside (e.g. 'song_data/*/*/*/*.json')
        spark (obj): Spark Session
        
    Returns:
        spark data object
    """
    
    song_schema = ST([
        Fld('artist_id', Str(), nullable=True),
        Fld('artist_latitude', Dbl(), nullable=True),
        Fld('artist_location', Str(), nullable=True),
        Fld('artist_longitude', Dbl(), nullable=True),
        Fld('artist_name', Str(), nullable=True),
        Fld('duration', Dbl(), nullable=True),
        Fld('num_songs', Int(), nullable=True),
        Fld('song_id', Str(), nullable=False),
        Fld('title', Str(), nullable=True),
        Fld('year', Int(), nullable=True)
    ])
    return spark.read.json(fpath, schema=song_schema) 


def process_song_data(spark, input_data, output_data):
    """ Read raw song .json files, form songs dimension and artists dimension tables, and save 
    
    Args:
        spark (obj): Spark session
        input_data (str): Path to raw files
        output_data (str): Path to destination S3 bucket
    """
    
    print('\n\nprocess_song_data')
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    df = load_song_jsons(song_data, spark)
    df.printSchema()
    
    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()
    print("\nSongs row count: {}".format(songs_table.count()))
    songs_table.printSchema()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(os.path.join(output_data, 'songs'))

    # extract columns to create artists table
    artists_table = song_data.selectExpr( 
        ['artist_id', 'artist_name AS name', 'artist_location AS location', 
         'artist_latitude AS latititude', 'artist_longitude AS longitude']
    ).dropDuplicates()
    print("\nArtists row count: {}".format(artists_table.count()))
    artists_table.printSchema()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(os.path.join(output_data, 'artists'))


def process_log_data(spark, input_data, output_data):
    """ Read raw log .json files; form user dimension, time dimension, and songplays fact table; and save 
    
    Args:
        spark (obj): Spark session
        input_data (str): Path to raw files
        output_data (str): Path to destination S3 bucket
    """
    
    print('\n\nprocess_log_data')
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.selectExpr( 
        ['INT(userId) AS user_id', 'firstName AS first_name', 'lastName AS last_name', 'gender', 'level']
    ).dropDuplicates()
    print("\nUsers row count: {}".format(users_table.count()))
    users_table.printSchema()

    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0), returnType=Tmstp())
    df = df.withColumn("start_time", get_timestamp(df.ts))

    # extract columns to create time table
    time_table = df.select('start_time').dropDuplicates() \
        .withColumn('hour', hour(col('start_time'))) \
        .withColumn('day', dayofmonth(col('start_time'))) \
        .withColumn('week', weekofyear(col('start_time'))) \
        .withColumn('month', month(col('start_time'))) \
        .withColumn('year', year(col('start_time'))) \
        .withColumn('weekday', date_format(col("start_time"), "u")) # syntax derived from here: https://sparkbyexamples.com/spark/spark-get-day-of-week-number/
    print("\nTime row count: {}".format(time_table.count()))
    time_table.printSchema()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(os.path.join(output_data, 'time'))

    # read in song data to use for songplays table
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    song_df = load_song_jsons(song_input_data, spark)
    
    # extract columns from joined song and log datasets to create songplays table 
    joined = df.join(song_df, 
                     [df.song == song_df.title, df.artist == song_df.artist_name], 
                     'inner'
                    ).dropDuplicates()
    joined.createOrReplaceTempView("joined")
    songplays_table = spark.sql("""
        SELECT monotonically_increasing_id() AS songplay_id,
            j.start_time,
            month(j.start_time) AS month,
            year(j.start_time) AS year,
            j.userId AS user_id,
            j.level,
            j.song_id,
            j.artist_id,
            j.sessionid AS session_id,
            j.location,
            j.useragent AS user_agent
        FROM joined j
    """)
    songplays_table.printSchema()
    print('There are {} rows in songplays fact table'.format(songplays_table.count()))
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays'))


def main():
    """ Run ELT process for songplays fact table and associated dimensions """
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-cseal/data-lake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
