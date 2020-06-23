# Databricks notebook source
# MAGIC %md # Structured Streaming: The Fundamentals of a Streaming Query

# COMMAND ----------

# MAGIC %scala
# MAGIC val workingDir = "/tmp/streaming/part1/"

# COMMAND ----------

workingDir = "/tmp/streaming/part1/"

# COMMAND ----------

# MAGIC %md ## Five Steps to Define a Streaming Query

# COMMAND ----------

# MAGIC %md ### 1. Define input sources

# COMMAND ----------

# MAGIC %scala
# MAGIC // Databricks does not support reading from console
# MAGIC val lines = spark
# MAGIC   .readStream.format("socket")
# MAGIC   .option("host", "localhost")
# MAGIC   .option("port", 9999)
# MAGIC   .load()

# COMMAND ----------

lines = (spark
         .readStream.format("socket")
         .option("host", "localhost")
         .option("port", 9999)
         .load())

# COMMAND ----------

# MAGIC %md ### 2. Transform the data

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._
# MAGIC 
# MAGIC val words = lines.select(split(col("value"), "\\s").as("word"))
# MAGIC val counts = words.groupBy("word").count()

# COMMAND ----------

from pyspark.sql.functions import *

words = lines.select(split(col("value"), "\\s").alias("word"))
counts = words.groupBy("word").count()

# COMMAND ----------

# MAGIC %md ### Step 3: Define output sink and output mode

# COMMAND ----------

# MAGIC %scala 
# MAGIC val writer = counts.writeStream
# MAGIC   .format("console")
# MAGIC   .outputMode("complete")

# COMMAND ----------

writer = (counts
          .writeStream
          .format("console")
          .outputMode("complete"))

# COMMAND ----------

# MAGIC %md ### Step 4: Specify processing details

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.streaming._
# MAGIC val checkpointDir = workingDir + "/fiveSteps/"
# MAGIC 
# MAGIC val writer2 = writer
# MAGIC   .trigger(Trigger.ProcessingTime("1 second"))
# MAGIC   .option("checkpointLocation", checkpointDir)

# COMMAND ----------

checkpointDir = workingDir + "/fiveSteps/"

writer2 = (writer
           .trigger(Trigger.ProcessingTime("1 second"))
           .option("checkpointLocation", checkpointDir))

# COMMAND ----------

# MAGIC %md ### Step 5: Start the query

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val streamingQuery = writer2.start()
# MAGIC 
# MAGIC streamingQuery.awaitTermination()

# COMMAND ----------

streamingQuery = writer2.start()

streamingQuery.awaitTermination()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## Monitoring an Active Query

# COMMAND ----------

# MAGIC %md ### Querying current status using StreamingQuery

# COMMAND ----------

# MAGIC %scala
# MAGIC streamingQuery.status

# COMMAND ----------

streamingQuery.status

# COMMAND ----------

# MAGIC %scala
# MAGIC streamingQuery.lastProgress

# COMMAND ----------

streamingQuery.lastProgress

# COMMAND ----------

# MAGIC %md ### Publishing metrics using custom StreamingQueryListeners 

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.streaming._
# MAGIC 
# MAGIC val myListener = new StreamingQueryListener() {
# MAGIC     override def onQueryStarted(event: QueryStartedEvent): Unit = {
# MAGIC         println("Query started: " + event.id)
# MAGIC     }
# MAGIC     override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
# MAGIC         println("Query terminated: " + event.id)
# MAGIC     }
# MAGIC     override def onQueryProgress(event: QueryProgressEvent): Unit = {
# MAGIC         println("Query made progress: " + event.progress)
# MAGIC     }
# MAGIC }