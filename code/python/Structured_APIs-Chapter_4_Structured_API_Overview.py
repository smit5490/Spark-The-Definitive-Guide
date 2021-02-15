from pyspark.sql import SparkSession

# Set-up spark session
spark = SparkSession.builder.appName("chapter-4").getOrCreate()


# Create spark dataframe with numbers 0 - 499
df = spark.range(500).toDF("number")
# Perform addition in purely spark
df.select(df["number"] + 10)


# Returns a list of rows
spark.range(2).collect()
type(spark.range(2).collect())


# Access data types
from pyspark.sql.types import *
b = ByteType()

spark.stop()
