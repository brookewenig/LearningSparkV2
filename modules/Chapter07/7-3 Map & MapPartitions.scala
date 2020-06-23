// Databricks notebook source
// MAGIC %md # Map & MapPartitions

// COMMAND ----------

// MAGIC %md Create a large DataFrame with two columns: number and its square

// COMMAND ----------

val df = spark.range(1 * 10000000).toDF("id").withColumn("square", $"id" * $"id").repartition(16)

// COMMAND ----------

// MAGIC %md Create some functions to use in the _map()_ and _mapPartition()_

// COMMAND ----------

import spark.implicits._
import scala.math.sqrt
import java.io.FileWriter

// simulate a connection to a FS
def getConnection(f:String): FileWriter = {
  new FileWriter(f, true)
}

// use function with map()
def func(v: Long) = {
  // make a connection to DB
  val conn = getConnection("/tmp/sqrt.txt")
  val sr = sqrt(v)
  // write value out to DB
  conn.write(sr.toString())
  conn.write(System.lineSeparator()) 
  conn.close()
  sr
}

// use function for mapPartition
def funcMapPartitions(conn:FileWriter, v: Long) = {
  val sr = sqrt(v)
  conn.write(sr.toString())
  conn.write(System.lineSeparator())
  sr
}
// curried function to benchmark any code or function
def benchmark(name: String)(f: => Unit) {
  val startTime = System.nanoTime
  f
  val endTime = System.nanoTime
  println(s"Time taken in $name: " + (endTime - startTime).toDouble / 1000000000 + " seconds")
}

// COMMAND ----------

// MAGIC %md Benchmark Map function

// COMMAND ----------

benchmark("map function") {
  df.map(r => (func(r.getLong(1)))).show(10)
}

// COMMAND ----------

// MAGIC %md Benchmark MapPartition function

// COMMAND ----------

import spark.implicits._

benchmark("mapPartition function") {
  
val newDF = df.mapPartitions(iterator => {
        val conn = getConnection("/tmp/sqrt.txt")
        val result = iterator.map(data=>{funcMapPartitions(conn, data.getLong(1))}).toList
        conn.close()
        result.iterator
      }
  ).toDF().show(10)
}