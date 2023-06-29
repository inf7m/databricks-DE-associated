# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, column, desc, col, expr, lit
from pyspark.sql.types import *

# SOURCE DATA
source_data = "/Users/inf7m/Downloads/Spark-The-Definitive-Guide-master/data/retail-data/by-day/*.csv"
source_data2 = "/Users/inf7m/Downloads/Spark-The-Definitive-Guide-master/data/flight-data/json/2015-summary.json"

# Manually define the schema

myManualSchema = StructType([
    StructField("DEST_COUNTRY_NAME", StringType(), True),
    StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
    StructField("count", LongType(), nullable=False, metadata={"First": "Schema"})
])

# Declare - and using Structured Streaming

SparkSession_1 = SparkSession.builder.appName("SparkApplication1").getOrCreate()
df = SparkSession_1.read.format("json") \
    .option("header", "false").schema(myManualSchema).load(source_data2)

df.createTempView("temp1")

# sample by take the fraction of DataFrame
withReplacement = False
seed = 5
fraction = 0.5
print("Some fraction?!!")
df.sample(withReplacement, fraction, seed).show(truncate=False)

# create random splits!!!

df.randomSplit([0.6, 0.4])

print(df.printSchema())
# Show 20 first rows
df.show(2)
# Set up the configuration

# Create some expressions

expression1 = expr("count -5")

# Create Row object

row1 = Row("Hello", None, 1210, True)

views = SparkSession_1.catalog.listTables()
current_views = len(views)

print(SparkSession_1.catalog.listTables())

df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME")).show(2)

# Using Parallelize by accessing core API
SparkContext = SparkSession_1.sparkContext
SparkContext.parallelize().reduce()
# new Rows initialize
newRows = [Row("United States", "United Kingdom", 9)]
# convert new Rows objects into a DataFrame
# but the creatDataFrame -> typically takes RDDs (core APIs) -> referring to SparkContext object

RDDs = SparkContext.parallelize(newRows)

newdf = SparkSession_1.createDataFrame(RDDs)

print("Number of the current RDDs " + str(df.rdd.getNumPartitions()))
df.repartition(5, col("DEST_COUNTRY_NAME"))
df.union(newdf)

print("Using lit function")
df.select(lit(5)).show()
emptyRDDs_1 = SparkContext.emptyRDD()
emptyRDDs_1.pipe()

# Write the conventional query in Spark

df.where(col("DEST_COUNTRY_NAME").rlike("^Swit")).show(2, False)

# Pre-define the expression

DOT_Code_filter = col("DEST_COUNTRY_NAME") == "DOT"

# declare complex queries by the foundation of bool

DOT_Code_filter = col("StockCode") == "DOT"
priceFilter = col("UnitPrice") > 600


# 12. RDDs - Resilient Distributed Datasets

RDDS_1 = SparkContext.parallelize().mapValues()
