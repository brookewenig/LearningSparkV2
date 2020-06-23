// Databricks notebook source
// MAGIC %md 
// MAGIC # Sort Merge Join
// MAGIC 
// MAGIC This notebook shows the _SortMergeJoin_ operation before bucketing and after bucketing

// COMMAND ----------

// Define a simple benchmark util function
def benchmark(name: String)(f: => Unit) {
  val startTime = System.nanoTime
  f
  val endTime = System.nanoTime
  println(s"Time taken in $name: " + (endTime - startTime).toDouble / 1000000000 + " seconds")
}

// COMMAND ----------

// This config turns off whole stage code generation, effectively changing the execution path to be similar to Spark 1.6.
spark.conf.set("spark.sql.codegen.wholeStage", false)

benchmark("Before Spark 2.x") {
  spark.range(1000L * 1000 * 1000).join(spark.range(1000L).toDF(), "id").count()
}

// COMMAND ----------

// MAGIC %md In Spark 3.0 you can have these different modes for `explain(mode)`: 'simple', 'extended', 'codegen', 'cost', 'formatted'

// COMMAND ----------

spark.range(1000L * 1000 * 1000).join(spark.range(1000L).toDF(), "id").selectExpr("count(*)").explain("simple")

// COMMAND ----------

spark.conf.set("spark.sql.codegen.wholeStage", true)

benchmark("Spark 3.0preview2") {
  spark.range(1000L * 1000 * 1005).join(spark.range(1040L).toDF(), "id").count()
}

// COMMAND ----------

spark.conf.set("spark.sql.codegen.wholeStage", true)
spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.defaultSizeInBytes", "100000")
spark.conf.set("spark.sql.shuffle.partitions", 16)

// COMMAND ----------

import scala.util.Random
var states = scala.collection.mutable.Map[Int, String]()
var items = scala.collection.mutable.Map[Int, String]()
val rnd = new scala.util.Random(42)
// initialize states and items purchased
states += (0 -> "AZ", 1 -> "CO", 2-> "CA", 3-> "TX", 4 -> "NY", 5-> "MI")
items += (0 -> "SKU-0", 1 -> "SKU-1", 2-> "SKU-2", 3-> "SKU-3", 4 -> "SKU-4", 5-> "SKU-5")
// create dataframes
val usersDF = (0 to 1000000).map(id => (id, s"user_${id}", s"user_${id}@databricks.com", states(rnd.nextInt(5)))).toDF("uid", "login", "email", "user_state")
val ordersDF = (0 to 1000000).map(r => (r, r, rnd.nextInt(10000), 10 * r* 0.2d, states(rnd.nextInt(5)), items(rnd.nextInt(5))))
                             .toDF("transaction_id", "quantity", "users_id", "amount", "state", "items")

// COMMAND ----------

display(usersDF)

// COMMAND ----------

display(ordersDF)

// COMMAND ----------

val usersOrdersDF = ordersDF.join(usersDF, $"users_id" === $"uid")

// COMMAND ----------

usersOrdersDF.show(false)

// COMMAND ----------

usersOrdersDF.cache()

// COMMAND ----------

display(usersOrdersDF)

// COMMAND ----------

usersOrdersDF.explain(true)

// COMMAND ----------

// MAGIC %sql 
// MAGIC -- ALL_NOTEBOOKS
// MAGIC DROP TABLE IF EXISTS UsersTbl

// COMMAND ----------

import org.apache.spark.sql.functions.asc
usersDF
  .orderBy(asc("uid"))
  .write.format("parquet")
  .bucketBy(8, "uid")
  .saveAsTable("UsersTbl")

// COMMAND ----------

// MAGIC %sql 
// MAGIC -- ALL_NOTEBOOKS
// MAGIC DROP TABLE IF EXISTS OrdersTbl

// COMMAND ----------

ordersDF.orderBy(asc("users_id"))
  .write.format("parquet")
  .bucketBy(8, "users_id")
  .saveAsTable("OrdersTbl")       

// COMMAND ----------

// MAGIC %sql 
// MAGIC -- ALL_NOTEBOOKS
// MAGIC SELECT * FROM OrdersTbl LIMIT 10

// COMMAND ----------

// MAGIC %sql 
// MAGIC -- ALL_NOTEBOOKS
// MAGIC SELECT * FROM UsersTbl LIMIT 10

// COMMAND ----------

// MAGIC %sql 
// MAGIC -- ALL_NOTEBOOKS
// MAGIC CACHE TABLE UsersTbl

// COMMAND ----------

val usersBucketDF = spark.table("UsersTbl")

// COMMAND ----------

// MAGIC %sql 
// MAGIC -- ALL_NOTEBOOKS
// MAGIC CACHE TABLE OrdersTbl

// COMMAND ----------

val ordersBucketDF = spark.table("OrdersTbl")

// COMMAND ----------

val joinUsersOrdersBucketDF = ordersBucketDF.join(usersBucketDF, $"users_id" === $"uid")

// COMMAND ----------


joinUsersOrdersBucketDF.show(false)

// COMMAND ----------

joinUsersOrdersBucketDF.explain(true)