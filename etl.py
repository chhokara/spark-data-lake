import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    creates a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    extracts data from song json files and processes them into songs and artists tables
    """
    # get filepath to song data file
    song_data = input_data + "/song_data/A/A/A/TRAAAAW128F429D538.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + "songs/")

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "artists/")


def process_log_data(spark, input_data, output_data):
    """
    extracts data from log json files and processes them into users, time, and songplays tables
    """
    # get filepath to log data file
    log_data = input_data + "/log_data/2018-11-01-events.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter("page = 'NextSong'")

    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level")
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "users/")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn("time_stamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(int(x)/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("date_time", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select(
                    col("time_stamp").alias("start_time"), \
                    hour("date_time").alias("hour"), \
                    dayofmonth("date_time").alias("day"), \
                    weekofyear("date_time").alias("week"), \
                    month("date_time").alias("month"), \
                    year("date_time").alias("year"), \
                    date_format("date_time", "E").alias("weekday")
                )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "time/")

    # read in song data to use for songplays table
    song_data = input_data + "/song_data/A/A/A/TRAAAAW128F429D538.json"
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    cond = [df.song == song_df.title, df.length == song_df.duration, df.artist == song_df.artist_name]
    songplays_table = df.join(song_df, cond) \
                        .select(
                            monotonically_increasing_id().alias("songplay_id"), \
                            col("time_stamp").alias("start_time"), \
                            col("userId").alias("user_id"), \
                            df.level, \
                            song_df.song_id, \
                            song_df.artist_id, \
                            col("sessionId").alias("sesssion_id"), \
                            df.location, \
                            col("userAgent").alias("user_agent"), \
                            year("date_time").alias("year"), \
                            month("date_time").alias("month")
                        )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "songplays/")


def main():
    spark = create_spark_session()
    input_data = "./data"
    output_data = "s3a://udacity-parquet/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
