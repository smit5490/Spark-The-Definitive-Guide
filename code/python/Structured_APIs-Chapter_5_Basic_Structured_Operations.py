from pyspark.sql import SparkSession
import os

# Set-up spark session and import data
spark = SparkSession.builder.appName("chapter-5").getOrCreate()
df = spark.read.format("json").load(os.getcwd()+"/data/flight-data/json/2015-summary.json")

# Check schema
df.printSchema()

# Schema on read
spark.read.format("json").load(os.getcwd()+"/data/flight-data/json/2015-summary.json").schema


# Create schema ahead of time
from pyspark.sql.types import StructField, StructType, StringType, LongType

myManualSchema = StructType([
  StructField("DEST_COUNTRY_NAME", StringType(), True),
  StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
  StructField("count", LongType(), False, metadata={"hello":"world"})])

df = spark.read.format("json").schema(myManualSchema)\
  .load(os.getcwd()+"/data/flight-data/json/2015-summary.json")


# Column manipulation
# Many ways to refer to columns. Here are two..
from pyspark.sql.functions import col, column
col("someColumnName")
column("someColumnName")

# Columns are expressions
# Expressions are a set of transformations on one or more values in a record in a DataFrame.
# The expr function can parse transformations and column references from a string and be subsequently passed into
# further transformations.
from pyspark.sql.functions import expr

(((col("someCol") +5)*20) - 6) < col("otherCol") # is equivalent to...
expr("(((someCol + 5) * 200) - 6) < otherCol")

# Can programmatically access columns using
spark.read.format("json").load(os.getcwd()+"/data/flight-data/json/2015-summary.json").columns # returns a list

# Dataframes are a collection of 'Row' objects
df.first()

# Creating a Row object (note that it doesn't have a schema)...
from pyspark.sql import Row
myRow = Row("Hello", None, 1, False)

myRow[0]
myRow[2]

# DataFrame Transformations

df = spark.read.format("json").load(os.getcwd()+"/data/flight-data/json/2015-summary.json")
df.createOrReplaceTempView("dfTable")


# Create a DataFrame Manually by creating a row and associated schema:
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, LongType
myManualSchema = StructType([
  StructField("some", StringType(), True),
  StructField("col", StringType(), True),
  StructField("names", LongType(), False)
])
myRow = Row("Hello", None, 1)
myDf = spark.createDataFrame([myRow], myManualSchema)
myDf.show()


# select and selectExpr
# These along with spark.sql.functions package will solve most transformation challenges
df.select("DEST_COUNTRY_NAME").show(2)
df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)


# can refer to the same column in many different ways...
from pyspark.sql.functions import expr, col, column
df.select(
    "DEST_COUNTRY_NAME",
    expr("DEST_COUNTRY_NAME"),
    col("DEST_COUNTRY_NAME"),
    column("DEST_COUNTRY_NAME"))\
  .show(2)

# Book says this should be an error but isn't. Maybe in scala??
df.select(col("DEST_COUNTRY_NAME"), "DEST_COUNTRY_NAME").show(2)

# expr most flexible - can refer to string or a manipulation of column including changing name
df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)


# Can use .alias to change column name back:
df.select(expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME"))\
  .show(2)


# use of select and expr so common - just use selectExpr instead
# Can be used for any non-aggregating SQL statement
df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)

# Check if destination and origin are same
df.selectExpr(
  "*", # all original columns
  "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")\
  .show(2)

# Can perform aggregations over entire dataframe:
df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)


# Pass explicit,specific values into spark using literals
from pyspark.sql.functions import lit
df.select(expr("*"), lit(1).alias("One")).show(2)


# Add a column
df.withColumn("numberOne", lit(1)).show(2)

df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))\
  .show(2)


# Rename columns
df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns


# Reserved Characters and keywords
dfWithLongColName = df.withColumn(
    "This Long Column-Name",
    expr("ORIGIN_COUNTRY_NAME"))


# Need to use backticks for column names with spaces in expressions
dfWithLongColName.selectExpr(
    "`This Long Column-Name`",
    "`This Long Column-Name` as `new col`")\
  .show(2)


dfWithLongColName.select(expr("`This Long Column-Name`")).columns

# Spark is case insensitive can change it config:
# --in SQL
# set spark.sql.caseSensitive true

# Remove columns
df.drop("ORIGIN_COUNTRY_NAME").columns

# Changing a column's type (cast)
df.withColumn("count2", col("count").cast("long"))

# Filter rows
# Can use filter or where with same results
df.filter(col("count") < 2).show(2) # is equivalent to
df.where("count < 2").show()

# Chain AND filters together
df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") != "Croatia")\
  .show(2)


# Get unique rows
df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()
df.select("ORIGIN_COUNTRY_NAME").distinct().count()


# Random samples - sample records from dataframe
seed = 5
withReplacement = False
fraction = 0.5
df.sample(withReplacement, fraction, seed).count()


# Random splits - useful for train, val, test splits
dataFrames = df.randomSplit([0.25, 0.75], seed)
dataFrames[0].count() > dataFrames[1].count() # False


# Concatenating and appending rows (union)
from pyspark.sql import Row
schema = df.schema
newRows = [
  Row("New Country", "Other Country", 5),
  Row("New Country 2", "Other Country 3", 1)
]
# Can use new rows and convert directly to spark dataframe.
# parallelizedRows = spark.sparkContext.parallelize(newRows)
# newDF = spark.createDataFrame(parallelizedRows, schema)
newDF = spark.createDataFrame(newRows, schema)

df.union(newDF)\
  .where("count = 1")\
  .where(col("ORIGIN_COUNTRY_NAME") != "United States")\
  .show()


# Sorting rows
# sort and orderBy are equivalent
df.sort("count").show(5)
df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)

# Can change sort order from ascending default
from pyspark.sql.functions import desc, asc
df.orderBy(expr("count desc")).show(2)
df.orderBy(col("count").desc(), col("DEST_COUNTRY_NAME").asc()).show(2)
# Use asc_nulls_first, desc_nulls_first, desc_nulls_last, or desc_nulls_last, to specify where you would like null
# calues to appear in an ordered dataframe

# For optimization, sometimes advisable to sort within each partition before another set of transformations.
spark.read.format("json").load(os.getcwd()+"/data/flight-data/json/*-summary.json")\
  .sortWithinPartitions("count")


# Use limit to reduce number of records extracted from dataframe.
df.limit(5).show()
df.orderBy(expr("count desc")).limit(6).show()

# Another important optimization opportunity is to partition data according to some frequently filtered columns
# Repartition will incur a full shuffle of the data regardless of if necessary.
# Only repartition if increasing partitions or partitioning for a given column

df.rdd.getNumPartitions() # 1

df.repartition(5)

df.repartition(col("DEST_COUNTRY_NAME"))

df.repartition(5, col("DEST_COUNTRY_NAME"))

# Coalesce won't incur full shuffle and will try to combine partitions
# This operation will shuffle data into 5 partitions based on DEST_COUNTRY_NAME and then coalesce them (without a full
# shuffle):
df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(2)


# Collecting Rows to the Driver
# Valuable to do in order to manipulate on local machine
# collect, take, and show are all driver-based actions
# Expensive computationally and may crash driver for very big data

collectDF = df.limit(10)
collectDF.take(5) # take works with an Integer count
collectDF.show() # this prints it out nicely
collectDF.show(5, False)
collectDF.collect()

# toLocalIterator collects partitions to the driver as an iterator. Can iterate over entire dataset
# partition-by-partition in a serial manner. This is computationally expensive since it is on a one-by-one basis and
# not run in parallel
collectDF.toLocalIterator()

# Stop spark cluster
spark.stop()

