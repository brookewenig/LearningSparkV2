# Databricks notebook source
# MAGIC %md # Example 3.7

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import col, expr, when, concat, lit
# define schema for our data
""""
schema = (StructType([
   StructField("Id", IntegerType(), False),
   StructField("First", StringType(), False),
   StructField("Last", StringType(), False),
   StructField("Url", StringType(), False),
   StructField("Published", StringType(), False),
   StructField("Hits", IntegerType(), False),
   StructField("Campaigns", ArrayType(StringType()), False)]))
   """

ddl_schema = "`Id` INT,`First` STRING,`Last` STRING,`Url` STRING,`Published` STRING,`Hits` INT,`Campaigns` ARRAY<STRING>"

# create our data
data = [[1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter", "LinkedIn"]],
       [2, "Brooke","Wenig","https://tinyurl.2", "5/5/2018", 8908, ["twitter", "LinkedIn"]],
       [3, "Denny", "Lee", "https://tinyurl.3","6/7/2019",7659, ["web", "twitter", "FB", "LinkedIn"]],
       [4, "Tathagata", "Das","https://tinyurl.4", "5/12/2018", 10568, ["twitter", "FB"]],
       [5, "Matei","Zaharia", "https://tinyurl.5", "5/14/2014", 40578, ["web", "twitter", "FB", "LinkedIn"]],
       [6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568, ["twitter", "LinkedIn"]]
      ]

# COMMAND ----------

# create a DataFrame using the schema defined above
blogs_df = spark.createDataFrame(data, ddl_schema)
# show the DataFrame; it should reflect our table above
blogs_df.show()

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.types._
# MAGIC import org.apache.spark.sql.functions.{col, expr, when, concat, lit}
# MAGIC 
# MAGIC val jsonFile = "/databricks-datasets/learning-spark-v2/blogs.json"
# MAGIC 
# MAGIC val schema = StructType(Array(StructField("Id", IntegerType, false),
# MAGIC   StructField("First", StringType, false),
# MAGIC   StructField("Last", StringType, false),
# MAGIC   StructField("Url", StringType, false),
# MAGIC   StructField("Published", StringType, false),
# MAGIC   StructField("Hits", IntegerType, false),
# MAGIC   StructField("Campaigns", ArrayType(StringType), false)))
# MAGIC 
# MAGIC val blogsDF = spark.read.schema(schema).json(jsonFile)
# MAGIC 
# MAGIC blogsDF.show(false)
# MAGIC // print the schemas
# MAGIC print(blogsDF.printSchema)
# MAGIC print(blogsDF.schema)

# COMMAND ----------

blogs_df.createOrReplaceTempView("blogs")

# COMMAND ----------

# MAGIC %scala
# MAGIC blogsDF.createOrReplaceTempView("blogs")

# COMMAND ----------

# MAGIC %scala 
# MAGIC // ALL_NOTEBOOKS
# MAGIC spark.table("blogs").schema.toDDL

# COMMAND ----------

blogs_df.select(expr("Hits") * 2).show(2)

# COMMAND ----------

# MAGIC %scala
# MAGIC blogsDF.select(expr("Hits") * 2).show(2)

# COMMAND ----------

blogs_df.select(expr("Hits") + expr("Id")).show(truncate=False)

# COMMAND ----------

# MAGIC %scala
# MAGIC blogsDF.select(expr("Hits") + expr("Id")).show(false)

# COMMAND ----------

blogs_df.withColumn("Big Hitters", (expr("Hits") > 10000)).show()

# COMMAND ----------

# MAGIC %scala
# MAGIC blogsDF.withColumn("Big Hitters", (expr("Hits") > 10000)).show()

# COMMAND ----------

blogs_df.withColumn("AuthorsId", (concat(expr("First"), expr("Last"), expr("Id")))).select(expr("AuthorsId")).show(n=4)

# COMMAND ----------

# MAGIC %scala
# MAGIC blogsDF.withColumn("AuthorsId", (concat(expr("First"), expr("Last"), expr("Id")))).select(expr("AuthorsId")).show(4)