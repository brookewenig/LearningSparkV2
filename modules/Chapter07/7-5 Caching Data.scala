// Databricks notebook source
// MAGIC %md # Caching Data

// COMMAND ----------

// MAGIC %md ### Use _cache()_

// COMMAND ----------

// MAGIC %md Create a large data set with couple of columns

// COMMAND ----------

val df = spark.range(1 * 10000000).toDF("id").withColumn("square", $"id" * $"id")
df.cache().count()

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import col
// MAGIC df = spark.range(1 * 10000000).toDF("id").withColumn("square", col("id") * col("id"))
// MAGIC df.cache().count()

// COMMAND ----------

df.count()

// COMMAND ----------

// MAGIC %python
// MAGIC df.count()

// COMMAND ----------

// MAGIC %md Check the Spark UI storage tab to see where the data is stored. 

// COMMAND ----------

// MAGIC %python
// MAGIC df.unpersist() # If you do not unpersist, df2 below will not be cached because it has the same query plan as df

// COMMAND ----------

df.unpersist() // If you do not unpersist, df2 below will not be cached because it has the same query plan as df

// COMMAND ----------

// MAGIC %md ### Use _persist(StorageLevel.Level)_

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark import StorageLevel
// MAGIC 
// MAGIC df2 = spark.range(1 * 10000000).toDF("id").withColumn("square", col("id") * col("id"))
// MAGIC df2.persist(StorageLevel.DISK_ONLY).count()

// COMMAND ----------

import org.apache.spark.storage.StorageLevel

val df2 = spark.range(1 * 10000000).toDF("id").withColumn("square", $"id" * $"id")
df2.persist(StorageLevel.DISK_ONLY).count()

// COMMAND ----------

// MAGIC %python
// MAGIC df2.count()

// COMMAND ----------

df2.count()

// COMMAND ----------

// MAGIC %md Check the Spark UI storage tab to see where the data is stored 

// COMMAND ----------

// MAGIC %python
// MAGIC df2.unpersist()

// COMMAND ----------

df2.unpersist()

// COMMAND ----------

// MAGIC %python
// MAGIC df.createOrReplaceTempView("dfTable")
// MAGIC spark.sql("CACHE TABLE dfTable")

// COMMAND ----------

df.createOrReplaceTempView("dfTable")
spark.sql("CACHE TABLE dfTable")

// COMMAND ----------

// MAGIC %md Check the Spark UI storage tab to see where the data is stored.

// COMMAND ----------

// MAGIC %python
// MAGIC spark.sql("SELECT count(*) FROM dfTable").show()

// COMMAND ----------

spark.sql("SELECT count(*) FROM dfTable").show()