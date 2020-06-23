# Databricks notebook source
# MAGIC %md # Spark Data Sources
# MAGIC 
# MAGIC This notebook shows how to use Spark Data Sources Interface API to read file formats:
# MAGIC  * Parquet
# MAGIC  * JSON
# MAGIC  * CSV
# MAGIC  * Avro
# MAGIC  * ORC
# MAGIC  * Image
# MAGIC  * Binary
# MAGIC 
# MAGIC A full list of DataSource methods is available [here](https://docs.databricks.com/spark/latest/data-sources/index.html#id1)

# COMMAND ----------

# MAGIC %md ## Define paths for the various data sources

# COMMAND ----------

parquet_file = "/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet"
json_file = "/databricks-datasets/learning-spark-v2/flights/summary-data/json/*"
csv_file = "/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*"
orc_file = "/databricks-datasets/learning-spark-v2/flights/summary-data/orc/*"
avro_file = "/databricks-datasets/learning-spark-v2/flights/summary-data/avro/*"
schema = "DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT"

# COMMAND ----------

# MAGIC %scala
# MAGIC val parquetFile = "/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet"
# MAGIC val jsonFile = "/databricks-datasets/learning-spark-v2/flights/summary-data/json/*"
# MAGIC val csvFile = "/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*"
# MAGIC val orcFile = "/databricks-datasets/learning-spark-v2/flights/summary-data/orc/*"
# MAGIC val avroFile = "/databricks-datasets/learning-spark-v2/flights/summary-data/avro/*"
# MAGIC val schema = "DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT"

# COMMAND ----------

# MAGIC %md ## Parquet Data Source

# COMMAND ----------

df = (spark
      .read
      .format("parquet")
      .option("path", parquet_file)
      .load())

# COMMAND ----------

# MAGIC %scala
# MAGIC val df = spark
# MAGIC   .read
# MAGIC   .format("parquet")
# MAGIC   .option("path", parquetFile)
# MAGIC   .load()

# COMMAND ----------

# MAGIC %md Another way to read this same data using a variation of this API

# COMMAND ----------

df2 = spark.read.parquet(parquet_file)

# COMMAND ----------

# MAGIC %scala
# MAGIC val df2 = spark.read.parquet(parquetFile)

# COMMAND ----------

df.show(10, False)

# COMMAND ----------

# MAGIC %scala
# MAGIC df.show(10, false)

# COMMAND ----------

# MAGIC %md ## Use SQL
# MAGIC 
# MAGIC This will create an _unmanaged_ temporary view

# COMMAND ----------

# MAGIC %sql 
# MAGIC --ALL_NOTEBOOKS
# MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
# MAGIC     USING parquet
# MAGIC     OPTIONS (
# MAGIC       path "/databricks-datasets/definitive-guide/data/flight-data/parquet/2010-summary.parquet"
# MAGIC     )

# COMMAND ----------

# MAGIC %md Use SQL to query the table
# MAGIC 
# MAGIC The outcome should be the same as one read into the DataFrame above

# COMMAND ----------

spark.sql("SELECT * FROM us_delay_flights_tbl").show(10, truncate=False)

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("SELECT * FROM us_delay_flights_tbl").show(10, false)

# COMMAND ----------

# MAGIC %md ## JSON Data Source

# COMMAND ----------

df = spark.read.format("json").option("path", json_file).load()

# COMMAND ----------

# MAGIC %scala
# MAGIC val df = spark
# MAGIC   .read
# MAGIC   .format("json")
# MAGIC   .option("path", jsonFile)
# MAGIC   .load()

# COMMAND ----------

df.show(10, truncate=False)

# COMMAND ----------

# MAGIC %scala
# MAGIC df.show(10, false)

# COMMAND ----------

df2 = spark.read.json(json_file)

# COMMAND ----------

df2.show(10, False)

# COMMAND ----------

# MAGIC %md ## Use SQL
# MAGIC 
# MAGIC This will create an _unmanaged_ temporary view

# COMMAND ----------

# MAGIC %sql 
# MAGIC --ALL_NOTEBOOKS
# MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
# MAGIC     USING json
# MAGIC     OPTIONS (
# MAGIC       path "/databricks-datasets/learning-spark-v2/flights/summary-data/json/*"
# MAGIC     )

# COMMAND ----------

# MAGIC %md Use SQL to query the table
# MAGIC 
# MAGIC The outcome should be the same as one read into the DataFrame above

# COMMAND ----------

spark.sql("SELECT * FROM us_delay_flights_tbl").show(10, truncate=False)

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("SELECT * FROM us_delay_flights_tbl").show(10, false)

# COMMAND ----------

# MAGIC %md ## CSV Data Source

# COMMAND ----------

df = (spark
      .read
	 .format("csv")
	 .option("header", "true")
	 .schema(schema)
	 .option("mode", "FAILFAST")  # exit if any errors
	 .option("nullValue", "")	  # replace any null data field with “”
	 .option("path", csv_file)
	 .load())


# COMMAND ----------

# MAGIC %scala
# MAGIC val df  = spark
# MAGIC   .read
# MAGIC   .format("csv")
# MAGIC   .option("header", "true")
# MAGIC   .schema(schema)
# MAGIC   .option("mode", "FAILFAST")  // exit if any errors
# MAGIC   .option("nullValue", "")	  // replace any null data field with “”
# MAGIC   .option("path", csvFile)
# MAGIC   .load()

# COMMAND ----------

df.show(10, truncate = False)

# COMMAND ----------

# MAGIC %scala
# MAGIC df.show(10, false)

# COMMAND ----------

(df.write.format("parquet")
  .mode("overwrite")
  .option("path", "/tmp/data/parquet/df_parquet")
  .option("compression", "snappy")
  .save())

# COMMAND ----------

# MAGIC %scala
# MAGIC (df.write.format("parquet")
# MAGIC   .mode("overwrite")
# MAGIC   .option("path", "/tmp/data/parquet/df_parquet")
# MAGIC   .option("compression", "snappy")
# MAGIC   .save())

# COMMAND ----------

# MAGIC %fs ls /tmp/data/parquet/df_parquet

# COMMAND ----------

df2 = (spark
       .read
       .option("header", "true")
       .option("mode", "FAILFAST")	 # exit if any errors
       .option("nullValue", "")
       .schema(schema)
       .csv(csv_file))

# COMMAND ----------

# MAGIC %scala
# MAGIC val df2 = spark
# MAGIC   .read
# MAGIC   .option("header", "true")
# MAGIC   .option("mode", "FAILFAST") // exit if any errors
# MAGIC   .option("nullValue", "")
# MAGIC   .schema(schema)
# MAGIC   .csv(csvFile)

# COMMAND ----------

df2.show(10, truncate=False)

# COMMAND ----------

# MAGIC %scala
# MAGIC df2.show(10, false)

# COMMAND ----------

# MAGIC %md ## Use SQL
# MAGIC 
# MAGIC This will create an _unmanaged_ temporary view

# COMMAND ----------

# MAGIC %sql
# MAGIC --ALL_NOTEBOOKS
# MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
# MAGIC     USING csv
# MAGIC     OPTIONS (
# MAGIC       path "/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*",
# MAGIC       header "true",
# MAGIC       inferSchema "true",
# MAGIC       mode "FAILFAST"
# MAGIC     )

# COMMAND ----------

# MAGIC %md Use SQL to query the table
# MAGIC 
# MAGIC The outcome should be the same as one read into the DataFrame above

# COMMAND ----------

spark.sql("SELECT * FROM us_delay_flights_tbl").show(10, truncate=False)

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("SELECT * FROM us_delay_flights_tbl").show(10, false)

# COMMAND ----------

# MAGIC %md ## ORC Data Source

# COMMAND ----------

df = (spark.read
      .format("orc")
      .option("path", orc_file)
      .load())

# COMMAND ----------

# MAGIC %scala
# MAGIC val df = spark.read
# MAGIC   .format("orc")
# MAGIC   .option("path", orcFile)
# MAGIC   .load()

# COMMAND ----------

df.show(10, truncate=False)

# COMMAND ----------

# MAGIC %scala
# MAGIC df.show(10, false)

# COMMAND ----------

# MAGIC %md ## Use SQL
# MAGIC 
# MAGIC This will create an _unmanaged_ temporary view

# COMMAND ----------

# MAGIC %sql
# MAGIC --ALL_NOTEBOOKS
# MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
# MAGIC     USING orc
# MAGIC     OPTIONS (
# MAGIC       path "/databricks-datasets/learning-spark-v2/flights/summary-data/orc/*"
# MAGIC     )

# COMMAND ----------

# MAGIC %md Use SQL to query the table
# MAGIC 
# MAGIC The outcome should be the same as one read into the DataFrame above

# COMMAND ----------

spark.sql("SELECT * FROM us_delay_flights_tbl").show(10, truncate=False)

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("SELECT * FROM us_delay_flights_tbl").show(10, false)

# COMMAND ----------

# MAGIC %md ## Avro Data Source

# COMMAND ----------

df = (spark.read
      .format("avro")
      .option("path", avro_file)
      .load())

# COMMAND ----------

# MAGIC %scala
# MAGIC val df = spark.read
# MAGIC   .format("avro")
# MAGIC   .option("path", avroFile)
# MAGIC   .load()

# COMMAND ----------

df.show(10, truncate=False)

# COMMAND ----------

# MAGIC %scala
# MAGIC df.show(10, false)

# COMMAND ----------

# MAGIC %md ## Use SQL
# MAGIC 
# MAGIC This will create an _unmanaged_ temporary view

# COMMAND ----------

# MAGIC %sql
# MAGIC --ALL_NOTEBOOKS
# MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
# MAGIC     USING avro
# MAGIC     OPTIONS (
# MAGIC       path "/databricks-datasets/learning-spark-v2/flights/summary-data/avro/*"
# MAGIC     )

# COMMAND ----------

# MAGIC %md Use SQL to query the table
# MAGIC 
# MAGIC The outcome should be the same as the one read into the DataFrame above

# COMMAND ----------

spark.sql("SELECT * FROM us_delay_flights_tbl").show(10, truncate=False)

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("SELECT * FROM us_delay_flights_tbl").show(10, false)

# COMMAND ----------

# MAGIC %md ## Image

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.ml.source.image
# MAGIC 
# MAGIC val imageDir = "/databricks-datasets/cctvVideos/train_images/"
# MAGIC val imagesDF = spark.read.format("image").load(imageDir)
# MAGIC 
# MAGIC imagesDF.printSchema
# MAGIC imagesDF.select("image.height", "image.width", "image.nChannels", "image.mode", "label").show(5, false)

# COMMAND ----------

from pyspark.ml import image

image_dir = "/databricks-datasets/cctvVideos/train_images/"
images_df = spark.read.format("image").load(image_dir)
images_df.printSchema()

images_df.select("image.height", "image.width", "image.nChannels", "image.mode", "label").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md ## Binary

# COMMAND ----------

# MAGIC %scala
# MAGIC val path = "/databricks-datasets/learning-spark-v2/cctvVideos/train_images/"
# MAGIC val binaryFilesDF = spark.read.format("binaryFile")
# MAGIC   .option("pathGlobFilter", "*.jpg")
# MAGIC   .load(path)
# MAGIC 
# MAGIC binaryFilesDF.show(5)

# COMMAND ----------

path = "/databricks-datasets/learning-spark-v2/cctvVideos/train_images/"
binary_files_df = (spark.read.format("binaryFile")
  .option("pathGlobFilter", "*.jpg")
  .load(path))

binary_files_df.show(5)

# COMMAND ----------

# MAGIC %md To ignore any partitioning data discovery in a directory, you can set the `recursiveFileLookup` to `true`.

# COMMAND ----------

# MAGIC %scala
# MAGIC val binaryFilesDF = spark.read.format("binaryFile")
# MAGIC   .option("pathGlobFilter", "*.jpg")
# MAGIC   .option("recursiveFileLookup", "true")
# MAGIC   .load(path)
# MAGIC binaryFilesDF.show(5)

# COMMAND ----------

binary_files_df = (spark.read.format("binaryFile")
   .option("pathGlobFilter", "*.jpg")
   .option("recursiveFileLookup", "true")
   .load(path))
binary_files_df.show(5)