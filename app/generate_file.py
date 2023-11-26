from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, udf, concat_ws, collect_list, current_date, current_timestamp, split, expr
from pyspark.sql.types import StringType
from firebase_db import add_data
import json
import os

import email
import boto3


def get_bucket(bucket_name: str):
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)
    return bucket


def read_file(bucket, key, encoding="utf-8") -> str:
    file_obj = io.BytesIO()
    bucket.download_fileobj(key, file_obj)
    wrapper = io.TextIOWrapper(file_obj, encoding=encoding)
    file_obj.seek(0)
    return wrapper.read()


def get_json_result():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("myapp") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1")\
        .getOrCreate()

    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.environ['AWS_ACCESS_KEY_ID'])
    spark._jsc.hadoopConfiguration().set(
        "fs.s3a.secret.key", os.environ['AWS_SECRET_ACCESS_KEY'])

    sdf = spark.read.csv("s3a://prj-datdang/split_email.csv", header=True)

    sdf.where(col("user") == "williams-w3")\
        .select("user", "Message-ID", "Date")\
        .show(10, False)

    df_parsed = sdf.withColumn("date_array", split(col("Date"), " "))

    df_parsed = df_parsed.withColumn("day", df_parsed["date_array"].getItem(1))
    df_parsed = df_parsed.withColumn(
        "month", df_parsed["date_array"].getItem(2))
    df_parsed = df_parsed.withColumn(
        "year", expr("cast(date_array[3] as int)"))
    df_parsed = df_parsed.withColumn(
        "time", df_parsed["date_array"].getItem(4))
    df_parsed = df_parsed.withColumn(
        "timezone", df_parsed["date_array"].getItem(5))
    df_parsed = df_parsed.drop("Date", "date_array")

    df_parsed.createOrReplaceTempView("table")
    spark.sql('SELECT user, year FROM table WHERE year == 2001').show()

    json_object = df_parsed.toJSON()
    json_object.foreach(lambda x: add_data(json.loads(x)))
    return "Success!"

    # json_object.foreach(lambda x: print(x))
