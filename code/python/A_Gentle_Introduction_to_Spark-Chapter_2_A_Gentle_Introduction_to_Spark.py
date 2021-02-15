# Doing some initial set-up to run this script in the Python Console
from pyspark.sql import SparkSession
import os
spark = SparkSession.builder.appName("Spark-Practice-Ch2").getOrCreate()

# Create a simple dataframe with 0 - 999
myRange = spark.range(1000).toDF("number")

# Two types of transformations/dependencies:
# 1. narrow transformation results in a one-to-one relationship between input and output partitions
# 2. wide transformation requires a "shuffle" of partitions to calculate results
divisBy2 = myRange.where("number % 2 = 0")  # Performed "lazily"

divisBy2.count()  # An "action" on the data requiring actual execution

# Three types of actions:
# 1. Actions to view data in console
# 2. Actions to collect data to native objects in respective language
# 3. Actions to write to output data sources

# Check http://localhost:4040


# End-to-End Example
flightData2015 = spark\
  .read\
  .option("inferSchema", "true")\
  .option("header", "true")\
  .csv("{}/data/flight-data/csv/2015-summary.csv".format(os.getcwd()))

flightData2015.take(3)

flightData2015.sort("count").explain()  # Shows DAG of transformations

spark.conf.set("spark.sql.shuffle.partitions", "5")  # Default is 200

flightData2015.sort("count").take(2)

# DataFrames and SQL
flightData2015.createOrReplaceTempView("flight_data_2015")

sqlWay = spark.sql("""
SELECT DEST_COUNTRY_NAME, count(1)
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
""")

dataFrameWay = flightData2015\
  .groupBy("DEST_COUNTRY_NAME")\
  .count()

sqlWay.explain()
dataFrameWay.explain()

# COMMAND ----------
from pyspark.sql.functions import max
flightData2015.select(max("count")).take(1)


# COMMAND ----------
maxSql = spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum(count) DESC
LIMIT 5
""")

maxSql.show()


# COMMAND ----------

from pyspark.sql.functions import desc

flightData2015\
  .groupBy("DEST_COUNTRY_NAME")\
  .sum("count")\
  .withColumnRenamed("sum(count)", "destination_total")\
  .sort(desc("destination_total"))\
  .limit(5)\
  .show()


# COMMAND ----------

flightData2015\
  .groupBy("DEST_COUNTRY_NAME")\
  .sum("count")\
  .withColumnRenamed("sum(count)", "destination_total")\
  .sort(desc("destination_total"))\
  .limit(5)\
  .explain()

spark.stop()