# Databricks notebook source
# MAGIC %md ## Example 2-1 M&M Count

# COMMAND ----------

from pyspark.sql.functions import *

mnm_file = "/databricks-datasets/learning-spark-v2/mnm_dataset.csv"

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._
# MAGIC 
# MAGIC val mnmFile = "/databricks-datasets/learning-spark-v2/mnm_dataset.csv"

# COMMAND ----------

# MAGIC %md ### Read from the CSV and infer the schema

# COMMAND ----------

mnm_df = (spark
          .read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(mnm_file))

display(mnm_df)

# COMMAND ----------

# MAGIC %scala
# MAGIC val mnmDF = spark
# MAGIC   .read
# MAGIC   .format("csv")
# MAGIC   .option("header", "true")
# MAGIC   .option("inferSchema", "true")
# MAGIC   .load(mnmFile)
# MAGIC 
# MAGIC display(mnmDF)

# COMMAND ----------

# MAGIC %md ### Aggregate count of all colors and groupBy state and color, orderBy descending order

# COMMAND ----------

count_mnm_df = (mnm_df
                .select("State", "Color", "Count")
                .groupBy("State", "Color")
                .agg(count("Count").alias("Total"))
                .orderBy("Total", ascending=False))

count_mnm_df.show(n=60, truncate=False)
print("Total Rows = %d" % (count_mnm_df.count()))

# COMMAND ----------

# MAGIC %scala
# MAGIC val countMnMDF = mnmDF
# MAGIC   .select("State", "Color", "Count")
# MAGIC   .groupBy("State", "Color")
# MAGIC   .agg(count("Count")
# MAGIC   .alias("Total"))
# MAGIC   .orderBy(desc("Total"))
# MAGIC 
# MAGIC countMnMDF.show(60)
# MAGIC println(s"Total Rows = ${countMnMDF.count()}")

# COMMAND ----------

# MAGIC %md ### Find the aggregate count for California by filtering on State

# COMMAND ----------

ca_count_mnm_df = (mnm_df
                   .select("State", "Color", "Count") 
                   .where(mnm_df.State == "CA") 
                   .groupBy("State", "Color") 
                   .agg(count("Count").alias("Total")) 
                   .orderBy("Total", ascending=False))

ca_count_mnm_df.show(n=10, truncate=False)

# COMMAND ----------

# MAGIC %scala
# MAGIC val caCountMnMDF = mnmDF
# MAGIC   .select("State", "Color", "Count")
# MAGIC   .where(col("State") === "CA")
# MAGIC   .groupBy("State", "Color")
# MAGIC   .agg(count("Count").alias("Total"))
# MAGIC   .orderBy(desc("Total"))
# MAGIC    
# MAGIC // show the resulting aggregation for California
# MAGIC caCountMnMDF.show(10)