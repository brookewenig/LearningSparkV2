# Databricks notebook source
# MAGIC %md ## Example 2-1 Line Count

# COMMAND ----------

spark.version

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.version

# COMMAND ----------

# MAGIC %scala
# MAGIC val strings = spark.read.text("/databricks-datasets/learning-spark-v2/SPARK_README.md")
# MAGIC strings.show(10, false)

# COMMAND ----------

strings = spark.read.text("/databricks-datasets/learning-spark-v2/SPARK_README.md")
strings.show(10, truncate=False)

# COMMAND ----------

# MAGIC %scala
# MAGIC strings.count()

# COMMAND ----------

strings.count()

# COMMAND ----------

filtered = strings.filter(strings.value.contains("Spark"))
filtered.count()

# COMMAND ----------

# MAGIC %scala
# MAGIC val filtered = strings.filter($"value".contains("Spark"))
# MAGIC filtered.count()