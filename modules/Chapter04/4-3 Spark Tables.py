# Databricks notebook source
# MAGIC %md # Spark Tables
# MAGIC 
# MAGIC This notebook shows how to use Spark Catalog Interface API to query databases, tables, and columns.
# MAGIC 
# MAGIC A full list of documented methods is available [here](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.Catalog)

# COMMAND ----------

us_flights_file = "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"

# COMMAND ----------

# MAGIC %scala
# MAGIC val us_flights_file = "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"

# COMMAND ----------

# MAGIC %md ### Create Managed Tables

# COMMAND ----------

# Create database and managed tables
spark.sql("DROP DATABASE IF EXISTS learn_spark_db CASCADE") 
spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")
spark.sql("CREATE TABLE us_delay_flights_tbl(date STRING, delay INT, distance INT, origin STRING, destination STRING)")

# COMMAND ----------

# MAGIC %scala
# MAGIC // Create database and managed tables
# MAGIC spark.sql("DROP DATABASE IF EXISTS learn_spark_db CASCADE")
# MAGIC spark.sql("CREATE DATABASE learn_spark_db")
# MAGIC spark.sql("USE learn_spark_db")
# MAGIC spark.sql("CREATE TABLE us_delay_flights_tbl (date STRING, delay INT, distance INT, origin STRING, destination STRING)")

# COMMAND ----------

# MAGIC %md ### Display the databases

# COMMAND ----------

display(spark.catalog.listDatabases())

# COMMAND ----------

# MAGIC %scala
# MAGIC display(spark.catalog.listDatabases())

# COMMAND ----------

# MAGIC %md ## Read our US Flights table

# COMMAND ----------

df = (spark.read.format("csv")
      .schema("date STRING, delay INT, distance INT, origin STRING, destination STRING")
      .option("header", "true")
      .option("path", "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv")
      .load())

# COMMAND ----------

# MAGIC %scala
# MAGIC val df = spark
# MAGIC   .read
# MAGIC   .format("csv")
# MAGIC   .schema("`date` STRING, `delay` INT, `distance` INT, `origin` STRING, `destination` STRING")
# MAGIC   .option("header", "true")
# MAGIC   .option("path", "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv")
# MAGIC   .load()

# COMMAND ----------

# MAGIC %md ## Save into our table

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("us_delay_flights_tbl")

# COMMAND ----------

# MAGIC %scala
# MAGIC df.write.mode("overwrite").saveAsTable("us_delay_flights_tbl")

# COMMAND ----------

# MAGIC %md ## Cache the Table

# COMMAND ----------

# MAGIC %sql 
# MAGIC --ALL_NOTEBOOKS
# MAGIC CACHE TABLE us_delay_flights_tbl

# COMMAND ----------

# MAGIC %md Check if the table is cached

# COMMAND ----------

spark.catalog.isCached("us_delay_flights_tbl")

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.catalog.isCached("us_delay_flights_tbl")

# COMMAND ----------

# MAGIC %md ### Display tables within a Database
# MAGIC 
# MAGIC Note that the table is MANGED by Spark

# COMMAND ----------

spark.catalog.listTables(dbName="learn_spark_db")

# COMMAND ----------

# MAGIC %scala
# MAGIC display(spark.catalog.listTables(dbName="learn_spark_db"))

# COMMAND ----------

# MAGIC %md ### Display Columns for a table

# COMMAND ----------

spark.catalog.listColumns("us_delay_flights_tbl")

# COMMAND ----------

# MAGIC %scala
# MAGIC display(spark.catalog.listColumns("us_delay_flights_tbl"))

# COMMAND ----------

# MAGIC %md ### Create Unmanaged Tables

# COMMAND ----------

# Drop the database and create unmanaged tables
spark.sql("DROP DATABASE IF EXISTS learn_spark_db CASCADE")
spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")
spark.sql("CREATE TABLE us_delay_flights_tbl (date STRING, delay INT, distance INT, origin STRING, destination STRING) USING csv OPTIONS (path '/databricks-datasets/learning-spark-v2/flights/departuredelays.csv')")

# COMMAND ----------

# MAGIC %scala
# MAGIC // Drop the database and create unmanaged tables
# MAGIC spark.sql("DROP DATABASE IF EXISTS learn_spark_db CASCADE")
# MAGIC spark.sql("CREATE DATABASE learn_spark_db")
# MAGIC spark.sql("USE learn_spark_db")
# MAGIC spark.sql("CREATE TABLE us_delay_flights_tbl (date INT, delay INT, distance INT, origin STRING, destination STRING) USING csv OPTIONS (path '/databricks-datasets/learning-spark-v2/flights/departuredelays.csv')")

# COMMAND ----------

# MAGIC %md ### Display Tables
# MAGIC 
# MAGIC **Note**: The table type here that tableType='EXTERNAL', which indicates it's unmanaged by Spark, whereas above the tableType='MANAGED'

# COMMAND ----------

spark.catalog.listTables(dbName="learn_spark_db")

# COMMAND ----------

# MAGIC %scala
# MAGIC display(spark.catalog.listTables(dbName="learn_spark_db"))

# COMMAND ----------

# MAGIC %md ### Display Columns for a table

# COMMAND ----------

spark.catalog.listColumns("us_delay_flights_tbl")

# COMMAND ----------

# MAGIC %scala
# MAGIC display(spark.catalog.listColumns("us_delay_flights_tbl"))