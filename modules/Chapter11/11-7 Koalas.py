# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/databricks/koalas/master/Koalas-logo.png" width="220"/>
# MAGIC </div>
# MAGIC 
# MAGIC The Koalas project makes data scientists more productive when interacting with big data, by implementing the pandas DataFrame API on top of Apache Spark. By unifying the two ecosystems with a familiar API, Koalas offers a seamless transition between small and large data.
# MAGIC 
# MAGIC **Goals of this notebook:**
# MAGIC * Demonstrate the similarities of the Koalas API with the pandas API
# MAGIC * Understand the differences in syntax for the same DataFrame operations in Koalas vs PySpark
# MAGIC 
# MAGIC [Koalas Docs](https://koalas.readthedocs.io/en/latest/index.html)
# MAGIC 
# MAGIC [Koalas Github](https://github.com/databricks/koalas)
# MAGIC 
# MAGIC **NOTE**: You need to first install `koalas` (PyPI)

# COMMAND ----------

# MAGIC %md ### Read in the dataset
# MAGIC 
# MAGIC * PySpark
# MAGIC * pandas
# MAGIC * Koalas

# COMMAND ----------

# PySpark
df = spark.read.parquet("/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet")

# COMMAND ----------

# Pandas
import pandas as pd

pdDF = pd.read_parquet("/dbfs/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet")
pdDF.head()

# COMMAND ----------

# Koalas
import databricks.koalas as ks

kdf = ks.read_parquet("/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet")
kdf.head()

# COMMAND ----------

# MAGIC %md ### Converting to Koalas DataFrame to/from Spark DataFrame

# COMMAND ----------

# Creating a Koalas DataFrame from PySpark DataFrame
kdf = ks.DataFrame(df)

# COMMAND ----------

# Alternative way of creating a Koalas DataFrame from PySpark DataFrame
kdf = df.to_koalas()

# COMMAND ----------

# MAGIC %md ### Value Counts

# COMMAND ----------

# To get value counts of the different property types with PySpark
display(df.groupby("property_type").count().orderBy("count", ascending=False))

# COMMAND ----------

# Value counts in Koalas
kdf["property_type"].value_counts()

# COMMAND ----------

# MAGIC %md ### Visualizations with Koalas DataFrames

# COMMAND ----------

kdf.plot(kind="hist", x="bedrooms", y="price", bins=200)

# COMMAND ----------

# MAGIC %md ### SQL on Koalas DataFrames

# COMMAND ----------

ks.sql("select distinct(property_type) from {kdf}")