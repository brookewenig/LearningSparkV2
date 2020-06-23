// Databricks notebook source
// MAGIC %md # Spark Configs

// COMMAND ----------

val mconf = spark.conf.getAll

// COMMAND ----------

for (k <- mconf.keySet) {println(s"${k} -> ${mconf(k)}")}

// COMMAND ----------

spark.conf.get("spark.shuffle.service.enabled")

// COMMAND ----------

spark.conf.get("spark.sql.files.maxPartitionBytes")

// COMMAND ----------

spark.sparkContext.defaultParallelism