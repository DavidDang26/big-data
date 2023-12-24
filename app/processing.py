from pyspark.sql.functions import col, when, collect_list
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.functions import udf, floor
from pyspark.sql.functions import regexp_replace, col, when, regexp_extract
import numpy as np
import re
import pandas as pd
import seaborn as sn
from tqdm import tqdm
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from firebase_db import add_normal_data, add_business_data, add_office_data, add_gaming_data
import json
import os

import email
import boto3


def read_file(bucket, key, encoding="utf-8") -> str:
    file_obj = io.BytesIO()
    bucket.download_fileobj(key, file_obj)
    wrapper = io.TextIOWrapper(file_obj, encoding=encoding)
    file_obj.seek(0)
    return wrapper.read()


def get_json_result(file_name):
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("myapp") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1")\
        .getOrCreate()

    spark._jsc.hadoopConfiguration().set(
        "fs.s3a.access.key", os.environ['AWS_ACCESS_KEY_ID'])
    spark._jsc.hadoopConfiguration().set(
        "fs.s3a.secret.key", os.environ['AWS_SECRET_ACCESS_KEY'])

    sdf = spark.read.csv(f"s3a://prj-datdang/{file_name}", header=True)
    sdf = sdf.withColumnRenamed(
        "Ổ cứng", "Disk").withColumnRenamed("M.Hình", "Monitor")
    sdf = sdf.withColumn("currentPrice", regexp_replace(
        col("currentPrice"), "[^\d]", "").cast("int"))
    sdf = sdf.withColumn("oldPrice", regexp_replace(
        col("oldPrice"), "[^\d]", "").cast("int"))
    sdf = sdf.withColumn(
        "itemType",
        when(col("name").contains("New 100%"), "New 100%")
        .when(col("name").contains("New Outlet"), "New Outlet")
        .when(col("name").contains("Mới 100%"), "New 100%")
        .otherwise("Unknown")
    )
    # Convert Disk to GB
    sdf = sdf.withColumn(
        "diskCapacityGB",
        when(col("Disk").contains("GB"), regexp_extract(
            col("Disk"), r"(\d+)GB", 1).cast("int"))
        .when(col("Disk").contains("TB"), regexp_extract(col("Disk"), r"(\d+)TB", 1).cast("int") * 1024)
        .otherwise(0)
    )
    # Convert RAM
    sdf = sdf.withColumn(
        "ramCapacityGB",
        when(col("RAM").contains("GB"), regexp_extract(
            col("RAM"), r"(\d+)GB", 1).cast("int"))
        .when(col("RAM").contains("TB"), regexp_extract(col("RAM"), r"(\d+)TB", 1).cast("int") * 1024)
        .otherwise(0)
    )

    # Extract CPU type, number of processes, 1
    sdf = sdf.withColumn(
        "chip",
        when(col("CPU").isNotNull(), regexp_extract(
            col("CPU"), r"(\D+)(\d+)", 1))
        .otherwise(regexp_extract(col("name"), r"(Intel|AMD|Ryzen).+?(\d+)", 1))
    )
    sdf = sdf.withColumn(
        "chipNum",
        when(col("CPU").isNotNull(), regexp_extract(
            col("CPU"), r"(\D+)(\d+)", 2))
        .otherwise(regexp_extract(col("name"), r"(Intel|AMD|Ryzen).+?(\d+)", 2))
    )
    sdf = sdf.withColumn("chipNum", col("chipNum").cast("int"))
    sdf = sdf.withColumn(
        "chip",
        when(col("chip").contains("i"), "Intel")
        .when(col("chip").contains("R"), "Ryzen")
        .when(col("chip").contains("AMD"), "Ryzen")
        .when(col("chip").contains("I"), "Intel")
    )

    # Have card or default
    sdf = sdf.withColumn(
        "vipCard",
        when(col("Card").contains("RTX"), "RTX")
        .when(col("Card").contains("GTX"), "GTX")
        .when(col("Card").contains("AMD"), "AMD")
        .otherwise("default")
    )

    # Extract screen width
    sdf = sdf.withColumn(
        "screenWidth",
        regexp_extract(col("Monitor"), r"(\d+(\.\d+)?)\s*(?:inch)?", 1)
    )
    sdf = sdf.withColumn("screenWidth", col("screenWidth").cast("float"))

    # Extract laptop type
    sdf = sdf.withColumn(
        "LaptopType",
        when(col("name").contains("MSI"), "MSI")
        .when(col("name").contains("Lenovo"), "Lenovo")
        .when(col("name").contains("Acer"), "Acer")
        .when(col("name").contains("Dell"), "Dell")
        .when(col("name").contains("DELL"), "Dell")
        .when(col("name").contains("HP"), "HP")
        .when(col("name").contains("Asus"), "Asus")
        .when(col("name").contains("Think"), "Thinkpad")
        .when(col("name").contains("ASUS"), "ASUS")
        .when(col("name").contains("Surface"), "Microsoft Surface")
        .when(col("name").contains("Mac"), "Macbook")
    )

    # Extract CPU from name when col('CPU') = NULL
    sdf = sdf.withColumn("CPU",
                         when(col("name").contains(" - "),
                              regexp_extract("name", r'- (.*?) \|', 1).alias("CPU"))
                         .otherwise(regexp_extract("name", r'\| (.*?)\|', 1).alias("CPU"))
                         )

    # Extract CPU to Type, Gen, Core
    sdf = sdf.withColumn("CPU", regexp_replace(col("CPU"), "\\s+$", ""))

    sdf = sdf.withColumn("genraw",
                         col("CPU").substr(-6, 2).cast("int")
                         )
    sdf = sdf.withColumn("gen",
                         when(col("genraw") > 20, floor(col("genraw")/10))
                         .when(col("genraw") == 1, col("CPU").substr(-5, 2).cast("int"))
                         .when(col("genraw") == -1, col("CPU").substr(-5, 2).cast("int"))
                         .when(col("genraw") < 0, col("genraw")*(-1))
                         .when(col("genraw").isNotNull(), col("genraw"))
                         .otherwise(col("CPU").substr(-5, 1).cast("int"))
                         )
    sdf = sdf.withColumn("chipLevel",
                         when(col("CPU").contains("G7"), "G7")
                         .otherwise(regexp_extract(col("CPU"), r"([A-Za-z]+)$", 1)))

    sdf.columns

    chip_weight = [0.25,  0.25, 0.25, 0.25]
    price_weight = [0.10,  0.10, 0.20, 0.20]
    discount = [0.05,  0.05, 0.05, 0.05]
    diskCapicity_weight = [0.15,  0.10, 0.15, 0.15]
    ramCapacity_weight = [0.25,  0.25, 0.25, 0.20]
    card_weight = [0.15,  0.20, 0.15, 0.10]
    screenwidth_weight = [0.05,  0.05, 0.05, 0.05]

    # Each input string corresponding to one index
    input_list = ["normal", "gaming", "office", "business"]
    mapping_dict = {category: index for index,
                    category in enumerate(input_list)}
    output_list = [mapping_dict[category] for category in input_list]

    input_string = "business"
    index = input_list.index(input_string)
    print(index)

    # Calculate cardScore
    sdf = sdf.withColumn(
        "cardScore",
        when(col("vipCard").contains("RTX"), 8)
        .when(col("vipCard").contains("GTX"), 4)
        .when(col("vipCard").contains("AMD"), 4)
        .otherwise(0)
    )
    # Calculate CPU Score
    sdf = sdf.withColumn(
        "chipScore",
        when(col("CPU").contains("GHz"), col("chipNum")+8)
        .when(col("chip").contains("Ryzen"), col("chipNum")+col("gen")+4)
        .otherwise(col("chipNum")+col("gen"))
    )
    # New, Newoutlet or Old
    sdf = sdf.withColumn(
        "newScore",
        when(col("itemType").contains("100%"), 1)
        .when(col("itemType").contains("Outlet"), 0.95)
        .otherwise(0.9)
    )

    t0 = input_list.index("normal")  # Input
    t1 = input_list.index("gaming")
    t2 = input_list.index("office")
    t3 = input_list.index("business")

    # Calculate Fitness function of "Normal" & "Office" category
    sdf = sdf.withColumn(
        "score",
        col("newScore")
        * (col("diskCapacityGB")/64 * diskCapicity_weight[t0]
            + col("ramCapacityGB")/2 * ramCapacity_weight[t0]
            + col("screenWidth")/2 * screenwidth_weight[t0]
            - col("currentPrice")/2000000 * price_weight[t0]
            + (col("oldPrice") - col("currentPrice"))/2000000 * discount[t0]
            + (col("chipScore")) * chip_weight[t0]
            + col("cardScore") * card_weight[t0])
    )
    # Calculate Fitness function of "Gaming" category
    sdf_gaming = sdf
    sdf_gaming = sdf_gaming.withColumn(
        "chipGaming",
        when(col("chipLevel").contains("K") | col(
            "chipLevel").contains("X"), col("chipScore") * 1.2)
        # .when(col("chipLevel").contains("X"), col("chipScore") * 1.2)
        .when(col("chipLevel").contains("H"), col("chipScore") * 1)
        .when(col("chipLevel").contains("p") | col("chipLevel").contains("G"), col("chipScore") * 0.9)
        .otherwise(col("chipScore") * 0.8)
    )
    sdf_gaming = sdf_gaming.withColumn(
        "gaming_score",
        col("newScore")
        * (col("diskCapacityGB")/64 * diskCapicity_weight[t1]
            + col("ramCapacityGB")/2 * ramCapacity_weight[t1]
            + col("screenWidth")/2 * screenwidth_weight[t1]
            - col("currentPrice")/2000000 * price_weight[t1]
            + (col("oldPrice") - col("currentPrice"))/2000000 * discount[t1]
            + (col("chipGaming")) * chip_weight[t1]
            + col("cardScore") * card_weight[t1])
    )
    # Calculate Fitness function of "Office" category
    sdf_office = sdf
    sdf_office = sdf_office.withColumn(
        "chipOffice",
        when(col("chipLevel").contains("G") | col(
            "chipLevel").contains("p"), col("chipScore") * 1.2)
        .otherwise(col("chipScore"))
    )
    sdf_office = sdf_office.withColumn(
        "office_score",
        col("newScore")
        * (col("diskCapacityGB")/64 * diskCapicity_weight[t2]
            + col("ramCapacityGB")/2 * ramCapacity_weight[t2]
            + col("screenWidth")/2 * screenwidth_weight[t2]
            - col("currentPrice")/2000000 * price_weight[t2]
            + (col("oldPrice") - col("currentPrice"))/2000000 * discount[t2]
            + (col("chipOffice")) * chip_weight[t2]
            + col("cardScore") * card_weight[t2])
    )
    # Calculate Fitness function of "Business" category
    sdf_business = sdf
    sdf_business = sdf_business.withColumn(
        "chipBusiness",
        when(col("chipLevel").contains("G") | col("chipLevel").contains(
            "U") | col("chipLevel").contains("p"), col("chipScore") * 1.1)
        .otherwise(col("chipScore")*0.9)
    )
    sdf_business = sdf_business.withColumn(
        "business_score",
        col("newScore")
        * (col("diskCapacityGB")/64 * diskCapicity_weight[t3]
            + col("ramCapacityGB")/2 * ramCapacity_weight[t3]
            + col("screenWidth")/2 * screenwidth_weight[t3]
            - col("currentPrice")/2000000 * price_weight[t3]
            + (col("oldPrice") - col("currentPrice"))/2000000 * discount[t3]
            + (col("chipBusiness")) * chip_weight[t3]
            + col("cardScore") * card_weight[t3])
    )

    sdf.createOrReplaceTempView("table")
    sdf_gaming.createOrReplaceTempView("gaming")
    sdf_office.createOrReplaceTempView("office")
    sdf_business.createOrReplaceTempView("business")

    # Output of "Normal" category
    output_normal = sdf.orderBy(col("score").desc())
    output_normal = output_normal.drop(
        "genraw", "cardScore", "chipScore", "newScore")
    # Json output
    json_normal = output_normal.toJSON()
    json_normal.foreach(lambda x: add_normal_data(json.loads(x)))

    # Output of "gaming" category
    output_gaming = sdf_gaming.orderBy(col("gaming_score").desc())
    output_gaming = output_gaming.drop(
        "oldPrice", "genraw", "cardScore", "chipScore", "newScore", "score", "chipGaming")
    # Json output
    json_gaming = output_gaming.toJSON()
    json_gaming.foreach(lambda x: add_gaming_data(json.loads(x)))

    # Output of "office" category
    output_office = sdf_office.orderBy(col("office_score").desc())
    output_office = output_office.drop(
        "oldPrice", "genraw", "cardScore", "chipScore", "newScore", "score", "chipOffice")
    # Json output
    json_office = output_office.toJSON()
    json_office.foreach(lambda x: add_office_data(json.loads(x)))

    # Output of "business" category
    output_business = sdf_business.orderBy(col("business_score").desc())
    output_business = output_business.drop(
        "oldPrice", "genraw", "cardScore", "chipScore", "newScore", "score", "chipBusiness")
    # Json output
    json_business = output_business.toJSON()
    json_business.foreach(lambda x: add_business_data(json.loads(x)))

    # json_object = df_parsed.toJSON()
    # json_object.foreach(lambda x: add_data(json.loads(x)))
    return "Success!"

    # json_object.foreach(lambda x: print(x))
