import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id
from pyspark.sql.types import TimestampType
import requests
import boto3


def create_spark_session():
    """
    Get or create a spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def check_data_quality(dataset):
    """Check for data quality

    :param dataset: Dataset to be checked
    """
    if dataset == 'questions':
        table_field = 'question_id'
        df_table = questions_table
    elif dataset == 'users':
        table_field = 'user_id'
        df_table = users_table

    print("Start data quality checks...")
    quality_results = { "table_count": 0, "table": ""}
    
    # Chack table
    print("Checking {} table...".format(dataset))
    df_table.createOrReplaceTempView("df_table")
    query_nulls = ("""
        SELECT  COUNT(*)
        FROM df_table
        WHERE {} IS NULL OR {} == ""
    """).format(table_field, table_field)
    table_check_nulls = spark.sql(query_nulls)

    # Check that table has > 0 rows
    table_check_count = spark.sql("""
        SELECT  COUNT(*)
        FROM df_table
    """)
    if table_check_nulls.collect()[0][0] > 0 \
        & table_check_count.collect()[0][0] < 1:
        quality_results['table_count'] = table_check_count.collect()[0][0]
        quality_results['table'] = "NOK"
    else:
        quality_results['table_count'] = table_check_count.collect()[0][0]
        quality_results['table'] = "OK"

    print("NULLS:")
    table_check_nulls.show(1)
    print("ROWS:")
    table_check_count.show(1)

    return quality_results


def process_question_data(spark, questions_data, output_data, output_data_s3):
    """
    Processes question data set, generate quesion table in parquet format and upload files to S3
    :param spark: spark session
    :param questions_data: path to input data 
    :param output_data: path to output data 
    :param output_data_s3: path to output data in S3
    """
    
    # read question data file
    df_questions = spark.read.json(questions_data)

    # extract columns to create question table
    question_fields = ["question_id", "title", "view_count", "creation_date", "owner.user_id", "is_answered", explode("tags").alias("tag")]
    questions_table = df_questions.select(question_fields).dropDuplicates()
    
    # write question table to parquet files
    questions_table.write.parquet(output_data + 'questions/', mode='overwrite')

    # get filepath to list all parquet files
    path = output_data + 'questions/'
    question_files = os.listdir(path)

    # upload files to S3
    for filename in question_files:
        if not filename.startswith('.'):
            upload_file(path + filename, output_data_s3, 'questions/{}'.format(filename))
            
    # Questions Data Quality
    check_data_quality('questions')
            

def process_user_data(spark, users_data, output_data, output_data_s3):
    """
    Processes question data set, generate quesion table in parquet format and upload files to S3
    :param spark: spark session
    :param questions_data: path to input data 
    :param output_data: path to output data 
    :param output_data_s3: path to output data in S3
    """
    
    # read user data file
    df_users = spark.read.json(users_data)

    # extract columns to create user table
    users_fields = ["user_id", "display_name", "reputation", "user_type", "location"]
    users_table = df_users.select(users_fields).dropDuplicates()
    
    # write user table to parquet files
    users_table.write.parquet(output_data + 'users/', mode='overwrite')

    # get filepath to list all parquet files
    path = output_data + 'users/'
    question_files = os.listdir(path)

    # upload files to S3
    for filename in question_files:
        if not filename.startswith('.'):
            upload_file(path + filename, output_data_s3, 'users/{}'.format(filename))

    # Questions Data Quality
    check_data_quality('users')


def process_tag_data(spark, tags_data, output_data, output_data_s3):
    """
    Processes question data set, generate quesion table in parquet format and upload files to S3
    :param spark: spark session
    :param questions_data: path to input data 
    :param output_data: path to output data 
    :param output_data_s3: path to output data in S3
    """
    
    # read tag data file
    df_tags = spark.read.json(tags_data)

    # extract columns to create tag table
    tags_fields = ["count", "name"]
    tags_table = df_tags.select(tags_fields).dropDuplicates()
    
    # write tag table to parquet files
    tags_table.write.parquet(output_data + 'tags/', mode='overwrite')

    # get filepath to list all parquet files
    path = output_data + 'tags/'
    question_files = os.listdir(path)

    # upload files to S3
    for filename in question_files:
        if not filename.startswith('.'):
            upload_file(path + filename, output_data_s3, 'tags/{}'.format(filename))


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    #spark = create_spark_session()
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['KEY']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET']
    
    input_data = "data/"
    output_data = "data/output_data/"
    output_data_s3 = "awsstagingstackoverflowbucket"

    questions_data = input_data + 'questions/*.json'
    tags_data = input_data + 'tags/*.json'
    users_data = input_data + 'users/*.json'

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    process_question_data(spark, questions_data, output_data, output_data_s3)
    process_user_data(spark, users_data, output_data, output_data_s3)
    process_tag_data(spark, tags_data, output_data, output_data_s3)
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()