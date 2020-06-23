# Databricks notebook source
# MAGIC %md # Structured Streaming: Streaming Data Sources and Sinks

# COMMAND ----------

# MAGIC %scala
# MAGIC val workingDir = "/tmp/streaming/part2/"

# COMMAND ----------

workingDir = "/tmp/streaming/part2/"

# COMMAND ----------

# MAGIC %md ## Files

# COMMAND ----------

# MAGIC %md ### Reading from files

# COMMAND ----------

# MAGIC %md Generate some json files for reading

# COMMAND ----------

# MAGIC %scala 
# MAGIC 
# MAGIC import org.apache.spark.sql.functions._
# MAGIC 
# MAGIC val inputDirectoryOfJsonFiles = workingDir + "jsonFiles/"
# MAGIC 
# MAGIC spark.range(1000).toDF("key")
# MAGIC   .withColumn("value", col("key") + 1)
# MAGIC   .repartition(10)
# MAGIC   .write
# MAGIC   .format("json")
# MAGIC   .mode("append")
# MAGIC   .save(inputDirectoryOfJsonFiles)

# COMMAND ----------

from pyspark.sql.functions import *

inputDirectoryOfJsonFiles = workingDir + "jsonFiles/"

(spark
  .range(1000).toDF("key")
  .withColumn("value", col("key") + 1)
  .repartition(10)
  .write
  .format("json")
  .mode("append")
  .save(inputDirectoryOfJsonFiles))

# COMMAND ----------

# MAGIC %scala
# MAGIC display(dbutils.fs.ls(inputDirectoryOfJsonFiles))

# COMMAND ----------

# MAGIC %md Read the json files as a stream

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.types._
# MAGIC 
# MAGIC val fileSchema = new StructType()
# MAGIC     .add("key", IntegerType)
# MAGIC     .add("value", IntegerType)
# MAGIC 
# MAGIC val inputDF = spark.readStream
# MAGIC   	.format("json")
# MAGIC   	.schema(fileSchema)
# MAGIC   	.load(inputDirectoryOfJsonFiles)
# MAGIC 
# MAGIC display(inputDF)

# COMMAND ----------

from pyspark.sql.types import *

fileSchema = (StructType()
  .add(StructField("key", IntegerType()))
  .add(StructField("value", IntegerType())))

inputDF = (spark
  .readStream
  .format("json")
  .schema(fileSchema)
  .load(inputDirectoryOfJsonFiles))

display(inputDF)

# COMMAND ----------

# MAGIC %md ### Writing to Files

# COMMAND ----------

# MAGIC %scala
# MAGIC val outputDirectory = workingDir + "parquetFiles/"
# MAGIC val resultDataFrame = inputDF
# MAGIC 
# MAGIC val outputDir = …
# MAGIC val checkpointDir = …
# MAGIC val resultDF = …
# MAGIC  
# MAGIC val streamingQuery = resultDF
# MAGIC   .writeStream
# MAGIC   .format("parquet")
# MAGIC   .option("path", outputDir)
# MAGIC   .option("checkpointLocation", checkpointDir)
# MAGIC   .start()

# COMMAND ----------

outputDirectory = …

streamingQuery = ( resultDataFrame.writeStream
      .format("parquet")
      .start(outputDirectory) ) 

# COMMAND ----------

# MAGIC %md ## Kafka

# COMMAND ----------

# MAGIC %md ### Reading from Kafka

# COMMAND ----------

# MAGIC %scala
# MAGIC val inputDataFrame = spark
# MAGIC   .readStream
# MAGIC   .format("kafka")
# MAGIC   .option("kafka.bootstrap.servers", "host1:port1, host2:port2")
# MAGIC   .option("subscribe", "events")
# MAGIC   .load()

# COMMAND ----------

inputDataFrame = ( 
spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1, host2:port2")
  .option("subscribe", "events")
  .load() )


# COMMAND ----------

# MAGIC %scala
# MAGIC val counts = … // DataFrame[word: string, count: long]
# MAGIC val streamingQuery = counts.selectExpr(
# MAGIC "cast(word as string) as key", 
# MAGIC "cast(count as string) as value")
# MAGIC .writeStream
# MAGIC .format("kafka")
# MAGIC .option("kafka.bootstrap.servers", "host1:port1, host2:port2")
# MAGIC   	.option("topic", "word_counts")
# MAGIC   	.outputMode("update")
# MAGIC 	.option("checkpointLocation", checkpointDir)
# MAGIC .start()

# COMMAND ----------

counts = … # DataFrame[word: string, count: long]
	streamingQuery = (
counts.selectExpr(
"cast(word as string) as key", 
"cast(count as string) as value")
.writeStream
.format("kafka")
.option("kafka.bootstrap.servers", "host1:port1,  host2:port2")
  	.option("topic", "word_counts")
  	.outputMode("update")
.option("checkpointLocation", checkpointDir)
.start() )