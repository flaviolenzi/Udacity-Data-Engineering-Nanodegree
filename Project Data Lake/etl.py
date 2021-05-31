import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Get or create a spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Processes song data set and generate song table and artist table in parquet format
    :param spark: spark session
    :param input_data: path to input data 
    :param output_data: path to output data 
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df_song = spark.read.json(song_data)

    # extract columns to create songs table
    song_fields = ["song_id", "title", "artist_id", "year", "duration"]
    songs_table = df_song.select(song_fields).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "/songs/", mode='overwrite', partitionBy=["year", "artist_id"])

    # extract columns to create artists table
    artists_fields = ["artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude"]
    artists_table = df_song.selectExpr(artists_fields).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "/artists/", mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Processes log data set and generate users table, time table and songplays table in parquet format
    :param spark: spark session
    :param input_data: path to input data 
    :param output_data: path to output data 
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df_log = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_log = df_log.filter(df_log.page == 'NextSong')

    # extract columns for users table    
    users_fields = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    users_table = df_log.selectExpr(users_fields).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "/users/", mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df_log = df_log.withColumn("timestamp", get_timestamp(col("ts")))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: to_date(x), TimestampType())
    df_log = df_log.withColumn("datetime", get_timestamp(col("ts")))
    
    # extract columns to create time table
    time_fields = ["timestamp as start_time", "hour(datetime) as hour", "dayofmonth(datetime) as day", "weekofyear(datetime) as week", "month(datetime) as month", "year(datetime) as year", "dayofweek(datetime) as weekday"]
    time_table = df_log.selectExpr(time_fields).dropDuplicates() 
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + "/time/", mode='overwrite', partitionBy=["year", "month"])

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "/songs/")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_fields = ["datetime as start_time", "userId as user_id", "level", "song_id", "artist_id", "sessionId as session_id", "location", "userAgent as user_agent", "month(datetime) as month", "year(datetime) as year"]
    songplays_table = df_log.join(song_df, song_df.title == df_log.song, "inner") \
        .distinct() \
        .selectExpr(songplays_fields) \
        .withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + "/songplays/", mode='overwrite', partitionBy=["year", "month"])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/output_data"
    
    #input_data = "data/"
    #output_data = "data/output_data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
