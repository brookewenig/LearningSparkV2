// Databricks notebook source
// MAGIC %md # Partitions
// MAGIC This notebook shows how to check and change the number of partitions.

// COMMAND ----------

val numDF = spark.range(1000L * 1000 * 1000).repartition(16)
numDF.rdd.getNumPartitions

// COMMAND ----------

val rdd = spark.sparkContext.parallelize(1 to 100000000, 16)
rdd.getNumPartitions

// COMMAND ----------

spark.sparkContext.defaultParallelism

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)
spark.conf.get("spark.sql.shuffle.partitions")