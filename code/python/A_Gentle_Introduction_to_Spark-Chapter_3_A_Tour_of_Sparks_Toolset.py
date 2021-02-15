from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName("chapter-3").getOrCreate()

# The window functions adjust the timestamps to "America/Chicago" not UTC (as stored)
print(spark.sparkContext._jvm.java.util.TimeZone.getDefault())
print(spark.conf.get("spark.sql.session.timeZone"))

# Here we'll adjust the timezone to be UTC for consistency
spark.conf.set('spark.sql.session.timeZone', 'UTC')

# spark.conf.get("spark.driver.extraJavaOptions") # Doesn't work - idk why.

# Structured Streaming Example
staticDataFrame = spark.read.format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load("{}/data/retail-data/by-day/*.csv".format(os.getcwd()))

staticDataFrame.show(2) # pretty print
staticDataFrame.take(2) # returns list of rows

staticDataFrame.createOrReplaceTempView("retail_data")
staticSchema = staticDataFrame.schema


# What days do customers spend the most?
from pyspark.sql.functions import window, column, desc, col, min

# selectExpr allows sql-like syntax
# window function allows slicing of date
staticDataFrame\
  .selectExpr(
    "CustomerId",
    "(UnitPrice * Quantity) as total_cost",
    "InvoiceDate")\
  .groupBy(
    col("CustomerId"), window(col("InvoiceDate"), "1 day"))\
  .sum("total_cost")\
  .sort(desc("sum(total_cost)"))\
  .show(5)

# Include the InvoiceDate and look at a single customer - note the window and InvoiceDates match!
staticDataFrame\
  .filter(col("CustomerId") == 17450.0)\
  .selectExpr(
    "CustomerId",
    "(UnitPrice * Quantity) as total_cost",
    "InvoiceDate")\
  .groupBy(
    col("CustomerId"),col("InvoiceDate"), window(col("InvoiceDate"), "1 day"))\
  .sum("total_cost")\
  .sort(col("InvoiceDate"))\
  .show(5)

spark.conf.set("spark.sql.shuffle.partitions", "5")

## Structured Streaming
# Set-up streaming data

streamingDataFrame = spark.readStream\
    .schema(staticSchema)\
    .option("maxFilesPerTrigger", 1)\
    .format("csv")\
    .option("header", "true")\
    .load(os.getcwd()+"/data/retail-data/by-day/*.csv")

# Check whether streaming:
streamingDataFrame.isStreaming


# Set-up same logic as before:
purchaseByCustomerPerHour = streamingDataFrame\
  .selectExpr(
    "CustomerId",
    "(UnitPrice * Quantity) as total_cost",
    "InvoiceDate")\
  .groupBy(
    col("CustomerId"), window(col("InvoiceDate"), "1 day"))\
  .sum("total_cost")


# Call streaming action to start the execution of this data flow.
purchaseByCustomerPerHour.writeStream\
    .format("memory")\
    .queryName("customer_purchases")\
    .outputMode("complete")\
    .start()


# Values will change as data is streamed in...
spark.sql("""
  SELECT *
  FROM customer_purchases
  ORDER BY `sum(total_cost)` DESC
  """)\
  .show(5)

# Can also write results out to console
purchaseByCustomerPerHour.writeStream\
    .format("console")\
    .queryName("customer_purchases_2")\
    .outputMode("complete")\
    .start()

# Machine Learning and Advanced Analytics
staticDataFrame.printSchema()

# Data Transformations
from pyspark.sql.functions import date_format, col
preppedDataFrame = staticDataFrame\
  .na.fill(0)\
  .withColumn("day_of_week", date_format(col("InvoiceDate"), "EEEE"))\
  .coalesce(5)
# Note coalesce reduces number of partitions df is in.

# Create train and test sets
trainDataFrame = preppedDataFrame\
  .where("InvoiceDate < '2011-07-01'")
testDataFrame = preppedDataFrame\
  .where("InvoiceDate >= '2011-07-01'")

trainDataFrame.count()
testDataFrame.count()

# Feature Engineering
# String indexer will create a column of numeric values from the days of week, but there is no order to days of week
from pyspark.ml.feature import StringIndexer
indexer = StringIndexer()\
  .setInputCol("day_of_week")\
  .setOutputCol("day_of_week_index")

# So we use one hot encoding - note that it takes categorical indices not the actual categories.
# It also drops one of the fields to prevent linear dependence
from pyspark.ml.feature import OneHotEncoder
encoder = OneHotEncoder()\
  .setInputCol("day_of_week_index")\
  .setOutputCol("day_of_week_encoded")


# All machine learning algorithms in Spark take a vector type as input, which consists of only numbers.
from pyspark.ml.feature import VectorAssembler
vectorAssembler = VectorAssembler()\
  .setInputCols(["UnitPrice", "Quantity", "day_of_week_encoded"])\
  .setOutputCol("features")


# Use pipeline so future data goes through same process...
from pyspark.ml import Pipeline
transformationPipeline = Pipeline(stages=[indexer, encoder, vectorAssembler])


# Apply pipeline to training data.
fittedPipeline = transformationPipeline.fit(trainDataFrame)


# Transform training data.
transformedTraining = fittedPipeline.transform(trainDataFrame)

# Put intermediate dataframe into memory - allows repeated access at lower computational cost.
transformedTraining.cache()

# Perform clustering - instantiate model
from pyspark.ml.clustering import KMeans
kmeans = KMeans()\
  .setK(20)\
  .setSeed(1)

# Fit Model
kmModel = kmeans.fit(transformedTraining)

# Check cost
kmModel.computeCost(transformedTraining)

# Transform  test data using fit pipeline
transformedTest = fittedPipeline.transform(testDataFrame)

# Lower-Level APIs
# May use when reading or manipulating raw ata
# May use to parallelize raw data stored in memory on driver.
from pyspark.sql import Row
spark.sparkContext.parallelize([Row(1), Row(2), Row(3)]).toDF()

spark.stop()

