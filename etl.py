from array import array
import findspark
findspark.init()
import os
from dotenv import dotenv_values
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import  col, lit,from_json, count
from pyspark.sql import SparkSession


CONFIG = dotenv_values(".env")
if not CONFIG:
    CONFIG = os.environ


def calc_stage_dfs(df1: DataFrame, df2: DataFrame , columns: array):
    cardViewDfCount = df1.groupBy(columns)\
        .agg(
            count(lit(1)).alias("card_views")
        )
    articleViewDfCount = df2.groupBy(columns)\
        .agg(
            count(lit(1)).alias("article_views")
        )
    finallDfTable = cardViewDfCount.join(articleViewDfCount, columns, how='full').fillna(0, subset=['article_views', 'card_views'])
    return finallDfTable

def prepare_main_df(df: DataFrame):
    schema = StructType([ \
        StructField("category",StringType(),True), \
        StructField("id",StringType(),True), \
        StructField("noteType",StringType(),True), \
        StructField("orientation", StringType(), True), \
        StructField("position", StringType(), True), \
        StructField("publishTime",StringType(),True), \
        StructField("sourceDomain",StringType(),True), \
        StructField("sourceName",StringType(),True), \
        StructField("stream", StringType(), True), \
        StructField("streamType", StringType(), True), \
        StructField("subcategories", ArrayType(StringType()),True), \
        StructField("title", StringType(), True), \
        StructField("url", StringType(), True) \
    ])
    dfFromCSVJSON =  df.na.drop(subset=["ATTRIBUTES"]).select(col("TIMESTAMP"), col("EVENT_NAME"), col("MD5(USER_ID)")\
        .alias("user_id"), (from_json(col("ATTRIBUTES"), schema))\
        .alias("ATTRIBUTES")).select("TIMESTAMP","USER_ID" ,"EVENT_NAME","ATTRIBUTES.*")
    finalDf = dfFromCSVJSON.select(col("id")\
        .alias("article_id"), col("publishTime"), col("title"), col("category"), col("EVENT_NAME"), col("TIMESTAMP"), col("user_id"))\
        .withColumn("date",col("publishTime").cast(DateType()))\
        .withColumn("EVENTDATE", col("TIMESTAMP").cast(DateType()))
    cardViewDf = finalDf.filter(finalDf.EVENT_NAME.isin(['top_news_card_viewed', 'my_news_card_viewed']))
    articleViewDf = finalDf.filter(finalDf.EVENT_NAME == 'article_viewed')
    return cardViewDf, articleViewDf

def load_to_db(df: DataFrame, targetTable: str, connection_uri:str):
    df\
    .write\
    .format("jdbc")\
    .option("url", connection_uri)\
    .option("driver", "org.postgresql.Driver")\
    .option("dbtable", targetTable)\
    .option("user", CONFIG["POSTGRES_USER"])\
    .option("password", CONFIG["POSTGRES_PASSWORD"])\
    .option("numPartitions", 10)\
    .mode("append")\
    .save()

def read_data_from_s3(spark):
    """
    The function to retrieve data from s3 bucket and concat them into a pandas dataframe.
        
    Returns:
        (dataFrame): a concatenated dataframe which has been loaded from all tsv files
                         in a desired directory inside s3 bucket.
    """
  
    
    spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
    spark._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "true")

    s3_url = "s3a://"+CONFIG["AWS_KEY"]+":"+CONFIG["AWS_SECRET"] +"@" + CONFIG["AWS_S3_PATH"]

    df = spark.read.option("delimiter", "\t").option("header",True)\
        .csv(s3_url)
    return df



if __name__ == "__main__":

    # build spark session
    spark = SparkSession.builder.config("spark.jars", "postgresql-42.4.2.jar")\
        .config("spark.jars","hadoop-aws-2.7.3.jar") \
        .config("spark.jars","aws-java-sdk-1.11.30.jar") \
        .config("spark.jars","jets3t-0.9.4.jar") \
        .appName("BRGROUP_ETL").getOrCreate()
    
    data_path = "/home/data/"
    
    if int(CONFIG["READ_FROM_S3"]):
        df = read_data_from_s3(spark)
    else:
        df = spark.read.option("delimiter", "\t").option("header",True).csv(data_path)

    [cardViewDf, articleViewDf] =  prepare_main_df(df)


    # calculating article_performance DataFrame
    finallDfTable = calc_stage_dfs(cardViewDf, articleViewDf, ["article_id", "date", "title", "category"] )
    # calculating CTR DataFrame

    ctrFinallDf = calc_stage_dfs(cardViewDf, articleViewDf, ["user_id", "EVENTDATE"] )
    ctrFinallDfTable = ctrFinallDf.withColumn("ctr", col("article_views")/col("card_views"))\
            .select(col("user_id"), col("EVENTDATE").alias("date"), col("ctr"))

    # write to db
    connection_uri = "jdbc:postgresql://{}:{}/{}".format(
        CONFIG["POSTGRES_HOST"],
        CONFIG["POSTGRES_PORT"],
        CONFIG["POSTGRES_DB"],
    )

    load_to_db(ctrFinallDfTable, "user_performance", connection_uri)
    load_to_db(finallDfTable, "article_performance", connection_uri)

