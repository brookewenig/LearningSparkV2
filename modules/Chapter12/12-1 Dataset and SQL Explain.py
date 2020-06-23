# Databricks notebook source
# MAGIC %scala
# MAGIC val ds = spark.createDataset(Seq(20, 3, 3, 2, 4, 8, 1, 1, 3))

# COMMAND ----------

# MAGIC %scala
# MAGIC ds.show()

# COMMAND ----------

# MAGIC %scala
# MAGIC ds.groupByKey(l => l).count.show

# COMMAND ----------

# MAGIC %md ### Use `ds.explain("format")`
# MAGIC 
# MAGIC Format include `simple`, `extended`, `codegen`, `cost`, `formatted`

# COMMAND ----------

# MAGIC %scala
# MAGIC val strings = spark.read.text("/databricks-datasets/learning-spark-v2/SPARK_README.md")
# MAGIC val filtered = strings.filter($"value".contains("Spark"))
# MAGIC filtered.count()

# COMMAND ----------

strings = spark.read.text("/databricks-datasets/learning-spark-v2/SPARK_README.md")
filtered = strings.filter(strings.value.contains("Spark"))
filtered.count()

# COMMAND ----------

filtered.explain(mode="simple")

# COMMAND ----------

# MAGIC %scala
# MAGIC filtered.explain("simple")

# COMMAND ----------

filtered.explain(mode="extended")

# COMMAND ----------

# MAGIC %scala
# MAGIC filtered.explain("extended")

# COMMAND ----------

filtered.explain(mode="cost")

# COMMAND ----------

# MAGIC %scala
# MAGIC filtered.explain("cost")

# COMMAND ----------

filtered.explain(mode="codegen")

# COMMAND ----------

# MAGIC %scala
# MAGIC filtered.explain("codegen")

# COMMAND ----------

filtered.explain(mode="formatted")

# COMMAND ----------

# MAGIC %scala
# MAGIC filtered.explain("formatted")

# COMMAND ----------

# MAGIC %md ### Use SQL EXPLAIN format

# COMMAND ----------

# MAGIC %scala
# MAGIC strings.createOrReplaceTempView("tmp_spark_readme")

# COMMAND ----------

# MAGIC %python
# MAGIC strings.createOrReplaceTempView("tmp_spark_readme")

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- ALL_NOTEBOOKS
# MAGIC EXPLAIN FORMATTED SELECT * FROM tmp_spark_readme WHERE value % 'Spark'