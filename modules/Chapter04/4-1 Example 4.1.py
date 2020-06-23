# Databricks notebook source
# MAGIC %md # Example 4.1
# MAGIC 
# MAGIC This notebook shows Example 4.1 from the book showing how to use SQL on a US Flights Dataset dataset.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.types._
# MAGIC import org.apache.spark.sql.functions._

# COMMAND ----------

# MAGIC %md Define a UDF to convert the date format into a legible format. 
# MAGIC 
# MAGIC *Note*: the date is a string with year missing, so it might be difficult to do any queries using SQL `year()` function

# COMMAND ----------

def to_date_format_udf(d_str):
  l = [char for char in d_str]
  return "".join(l[0:2]) + "/" +  "".join(l[2:4]) + " " + " " +"".join(l[4:6]) + ":" + "".join(l[6:])

to_date_format_udf("02190925")

# COMMAND ----------

# MAGIC %scala
# MAGIC def toDateFormatUDF(dStr:String) : String  = {
# MAGIC   return s"${dStr(0)}${dStr(1)}${'/'}${dStr(2)}${dStr(3)}${' '}${dStr(4)}${dStr(5)}${':'}${dStr(6)}${dStr(7)}"
# MAGIC }
# MAGIC 
# MAGIC // test  it
# MAGIC toDateFormatUDF("02190925")

# COMMAND ----------

# MAGIC %md Register the UDF 

# COMMAND ----------

spark.udf.register("to_date_format_udf", to_date_format_udf, StringType())

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.udf.register("toDateFormatUDF", toDateFormatUDF(_:String):String)

# COMMAND ----------

# MAGIC %md Read our US departure flight data

# COMMAND ----------

df = (spark.read.format("csv")
      .schema("date STRING, delay INT, distance INT, origin STRING, destination STRING")
      .option("header", "true")
      .option("path", "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv")
      .load())

display(df)

# COMMAND ----------

# MAGIC %scala
# MAGIC val df = spark
# MAGIC   .read
# MAGIC   .format("csv")
# MAGIC   .schema("date STRING, delay INT, distance INT, origin STRING, destination STRING")
# MAGIC   .option("header", "true")
# MAGIC   .option("path", "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv")
# MAGIC   .load()
# MAGIC 
# MAGIC display(df)

# COMMAND ----------

# MAGIC %md Test our UDF

# COMMAND ----------

df.selectExpr("to_date_format_udf(date) as data_format").show(10, truncate=False)

# COMMAND ----------

# MAGIC %scala
# MAGIC df.selectExpr("toDateFormatUDF(date) as data_format").show(10, false)

# COMMAND ----------

# MAGIC %md Create a temporary view to which we can issue SQL queries

# COMMAND ----------

df.createOrReplaceTempView("us_delay_flights_tbl")

# COMMAND ----------

# MAGIC %scala
# MAGIC df.createOrReplaceTempView("us_delay_flights_tbl")

# COMMAND ----------

# MAGIC %md Cache Table so queries are expedient

# COMMAND ----------

# MAGIC %sql 
# MAGIC --ALL_NOTEBOOKS
# MAGIC CACHE TABLE us_delay_flights_tbl

# COMMAND ----------

# MAGIC %md Convert all `date` to `date_fm` so it's more eligible
# MAGIC 
# MAGIC Note: we are using UDF to convert it on the fly. 

# COMMAND ----------

spark.sql("SELECT *, date, to_date_format_udf(date) AS date_fm FROM us_delay_flights_tbl").show(10, truncate=False)

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("SELECT *, date, toDateFormatUDF(date) AS date_fm FROM us_delay_flights_tbl").show(10, false)

# COMMAND ----------

spark.sql("SELECT COUNT(*) FROM us_delay_flights_tbl").show() # Keep case consistent for all SQL??

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("SELECT COUNT(*) FROM us_delay_flights_tbl").show()

# COMMAND ----------

# MAGIC %md ### Query 1: 
# MAGIC 
# MAGIC  Find out all flights whose distance between origin and destination is greater than 1000 

# COMMAND ----------

spark.sql("SELECT distance, origin, destination FROM us_delay_flights_tbl WHERE distance > 1000 ORDER BY distance DESC").show(10, truncate=False)

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("SELECT distance, origin, destination FROM us_delay_flights_tbl WHERE distance > 1000 ORDER BY distance DESC").show(10, false)

# COMMAND ----------

# MAGIC %md A DataFrame equivalent query

# COMMAND ----------

df.select("distance", "origin", "destination").where(col("distance") > 1000).orderBy(desc("distance")).show(10, truncate=False)

# COMMAND ----------

# MAGIC %scala
# MAGIC df.select("distance", "origin", "destination").where(col("distance") > 1000).orderBy(desc("distance")).show(10, false)

# COMMAND ----------

df.select("distance", "origin", "destination").where("distance > 1000").orderBy("distance", ascending=False).show(10)

# COMMAND ----------

# MAGIC %scala
# MAGIC df.select("distance", "origin", "destination").where($"distance" > 1000).orderBy(desc("distance")).show(10, false)

# COMMAND ----------

df.select("distance", "origin", "destination").where("distance > 1000").orderBy(desc("distance")).show(10)

# COMMAND ----------

# MAGIC %scala
# MAGIC df.select("distance", "origin", "destination").where($"distance" > 1000).orderBy($"distance".desc).show(10, false)

# COMMAND ----------

# MAGIC %md ### Query 2: 
# MAGIC 
# MAGIC  Find out all flights with 2 hour delays between San Francisco and Chicago  

# COMMAND ----------

spark.sql("""
SELECT date, delay, origin, destination 
FROM us_delay_flights_tbl 
WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' 
ORDER by delay DESC
""").show(10, truncate=False)

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("""
# MAGIC SELECT date, delay, origin, destination 
# MAGIC FROM us_delay_flights_tbl 
# MAGIC WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' 
# MAGIC ORDER by delay DESC
# MAGIC """).show(10, false)

# COMMAND ----------

# MAGIC %md ### Query 3: 
# MAGIC 
# MAGIC A more complicated query in SQL, let's label all US flights originating from airports with _high_, _medium_, _low_, _no delays_, regardless of destinations.

# COMMAND ----------

spark.sql("""SELECT delay, origin, destination,
              CASE
                  WHEN delay > 360 THEN 'Very Long Delays'
                  WHEN delay > 120 AND delay < 360 THEN  'Long Delays '
                  WHEN delay > 60 AND delay < 120 THEN  'Short Delays'
                  WHEN delay > 0 and delay < 60  THEN   'Tolerable Delays'
                  WHEN delay = 0 THEN 'No Delays'
                  ELSE 'No Delays'
               END AS Flight_Delays
               FROM us_delay_flights_tbl
               ORDER BY origin, delay DESC""").show(10, truncate=False)


# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("""SELECT delay, origin, destination,
# MAGIC               CASE
# MAGIC                   WHEN delay > 360 THEN 'Very Long Delays'
# MAGIC                   WHEN delay > 120 AND delay < 360 THEN  'Long Delays '
# MAGIC                   WHEN delay > 60 AND delay < 120 THEN  'Short Delays'
# MAGIC                   WHEN delay > 0 and delay < 60  THEN   'Tolerable Delays'
# MAGIC                   WHEN delay = 0 THEN 'No Delays'
# MAGIC                   ELSE 'No Delays'
# MAGIC                END AS Flight_Delays
# MAGIC                FROM us_delay_flights_tbl
# MAGIC                ORDER BY origin, delay DESC""").show(10, false)

# COMMAND ----------

# MAGIC %md ### Some Side Queries

# COMMAND ----------

df1 =  spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'SFO'")

# COMMAND ----------

# MAGIC %scala
# MAGIC val df1 =  spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'SFO'")

# COMMAND ----------

df1.createOrReplaceGlobalTempView("us_origin_airport_SFO_tmp_view")

# COMMAND ----------

# MAGIC %scala
# MAGIC df1.createOrReplaceGlobalTempView("us_origin_airport_SFO_tmp_view")

# COMMAND ----------

# MAGIC %sql 
# MAGIC --ALL_NOTEBOOKS
# MAGIC SELECT * FROM global_temp.us_origin_airport_SFO_tmp_view

# COMMAND ----------

# MAGIC %sql 
# MAGIC --ALL_NOTEBOOKS
# MAGIC DROP VIEW IF EXISTS global_temp.us_origin_airport_JFK_tmp_view

# COMMAND ----------

df2 = spark.sql("SELECT date, delay, origin, destination from us_delay_flights_tbl WHERE origin = 'JFK'")

# COMMAND ----------

# MAGIC %scala
# MAGIC val df2 = spark.sql("SELECT date, delay, origin, destination from us_delay_flights_tbl WHERE origin = 'JFK'")

# COMMAND ----------

df2.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")

# COMMAND ----------

# MAGIC %scala
# MAGIC df2.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")

# COMMAND ----------

# MAGIC %sql 
# MAGIC --ALL_NOTEBOOKS
# MAGIC SELECT * FROM us_origin_airport_JFK_tmp_view

# COMMAND ----------

# MAGIC %sql 
# MAGIC --ALL_NOTEBOOKS
# MAGIC DROP VIEW IF EXISTS us_origin_airport_JFK_tmp_view

# COMMAND ----------

spark.catalog.listTables(dbName="global_temp")

# COMMAND ----------

# MAGIC %scala
# MAGIC display(spark.catalog.listTables(dbName="global_temp"))