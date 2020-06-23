// Databricks notebook source
// MAGIC %md # IoT Devices
// MAGIC 
// MAGIC Define a Scala case class that will map to a Scala Dataset: _DeviceIoTData_

// COMMAND ----------

case class DeviceIoTData (battery_level: Long, c02_level: Long, 
    cca2: String, cca3: String, cn: String, device_id: Long, 
    device_name: String, humidity: Long, ip: String, latitude: Double,
    lcd: String, longitude: Double, scale:String, temp: Long, timestamp: Long)

// COMMAND ----------

// MAGIC %md Define a Scala case class that will map to a Scala Dataset: _DeviceTempByCountry_

// COMMAND ----------

case class DeviceTempByCountry(temp: Long, device_name: String, device_id: Long, cca3: String)

// COMMAND ----------

// MAGIC %md Read JSON files with device information
// MAGIC 
// MAGIC 1. The DataFrameReader will return a DataFrame and convert to Dataset[DeviceIotData]
// MAGIC 2. DS is a collection of Dataset that map to Scala case class _DeviceIotData_

// COMMAND ----------

val ds = spark.read.json("/databricks-datasets/learning-spark-v2/iot-devices/iot_devices.json").as[DeviceIoTData]

// COMMAND ----------

// MAGIC %md Schema maps to each field and type in the Scala case class object

// COMMAND ----------

ds.printSchema

// COMMAND ----------

ds.show(5, false)

// COMMAND ----------

// MAGIC %md Use Dataset API to filter temperature and humidity. Note the use of `object.field` syntax employed with Dataset JVM,
// MAGIC similar to accessing JavaBean fields. This syntax is not only readable but compile type-safe too. 
// MAGIC 
// MAGIC For example, if you compared d.temp > "30", you will get an compile error. 

// COMMAND ----------

val filterTempDS = ds.filter(d => {d.temp > 30 && d.humidity > 70})

// COMMAND ----------

filterTempDS.show(5, false)

// COMMAND ----------

// MAGIC %md Use a more complicated query with lambda functions with the original Dataset DeviceIoTData. Note the awkward column names prefix `_1`, `_2`, etc.
// MAGIC This is Spark way of handling unknown columns names returned from a Dataset when using queries with lambda expressions. We just renamed them and cast them
// MAGIC to our defined case class _DeviceTempByCountry_.

// COMMAND ----------

val dsTemp = ds
  .filter(d => {d.temp > 25}).map(d => (d.temp, d.device_name, d.device_id, d.cca3))
  .withColumnRenamed("_1", "temp")
  .withColumnRenamed("_2", "device_name")
  .withColumnRenamed("_3", "device_id")
  .withColumnRenamed("_4", "cca3").as[DeviceTempByCountry]

dsTemp.show(10)

// COMMAND ----------

// MAGIC %md This query returns a _Dataset[Row]_ since we don't have a corresponding case class to convert to, so a generic `Row` object is 
// MAGIC returned.

// COMMAND ----------

ds.select($"temp", $"device_name", $"device_id", $"humidity", $"cca3", $"cn").where("temp > 25").show(5, false)

// COMMAND ----------

// MAGIC %md By contrast, this results maps well to our case class _DeviceTempByCountry_

// COMMAND ----------

val dsTemp2 = ds.select($"temp", $"device_name", $"device_id", $"device_id", $"cca3").where("temp > 25").as[DeviceTempByCountry]

dsTemp.show(5, false)

// COMMAND ----------

// MAGIC %md Use the first() method to peek at first _DeviceTempByCountry_ object

// COMMAND ----------

val device = dsTemp.first()

// COMMAND ----------

// MAGIC %md ** Q-1) How to detect failing devices with low battery below a threshold?**
// MAGIC 
// MAGIC Note: threshold level less than 8 are potential candidates

// COMMAND ----------

ds.select($"battery_level", $"c02_level", $"device_name").where($"battery_level" < 8).sort($"c02_level").show(5, false)

// COMMAND ----------

// MAGIC %md ** Q-2) How to identify offending countries with high-levels of C02 emissions?**
// MAGIC 
// MAGIC Note: Any C02 levels above 1300 are potential violators of C02 emissions
// MAGIC 
// MAGIC Filter out c02_levels is eater than 1300, sort in descending order on C02_level. Note that this high-level domain specific language API reads like a SQL query

// COMMAND ----------

val newDS = ds
  .filter(d => {d.c02_level > 1300})
  .groupBy($"cn")
  .avg()
  .sort($"avg(c02_level)".desc)

newDS.show(10, false)

// COMMAND ----------

// MAGIC %md ** Q-3) Can we sort and group country with average temperature, C02, and humidity?**

// COMMAND ----------

ds.filter(d => {d.temp > 25 && d.humidity > 75})
  .select("temp", "humidity", "cn")
  .groupBy($"cn")
  .avg()
  .sort($"avg(temp)".desc, $"avg(humidity)".desc).as("avg_humidity").show(10, false)

// COMMAND ----------

// MAGIC %md ** Q-4) Can we compute min, max values for temperature, C02, and humidity?**

// COMMAND ----------

import org.apache.spark.sql.functions._ 

ds.select(min("temp"), max("temp"), min("humidity"), max("humidity"), min("c02_level"), max("c02_level"), min("battery_level"), max("battery_level")).show(10)