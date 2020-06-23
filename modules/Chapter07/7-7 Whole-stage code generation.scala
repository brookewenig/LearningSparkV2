// Databricks notebook source
// MAGIC %md
// MAGIC # Whole-stage code generation: Spark Tungsten engine
// MAGIC 
// MAGIC This notebook demonstrates the power of whole-stage code generation, a technique that blends state-of-the-art from modern compilers and MPP databases. In order to compare the performance with Spark 1.6, we turn off whole-stage code generation in Spark 2.0, which would result in using a similar code path as in Spark 1.6.
// MAGIC 
// MAGIC To read the companion blog posts, click the following:
// MAGIC - https://databricks.com/blog/2016/05/11/spark-2-0-technical-preview-easier-faster-and-smarter.html
// MAGIC - https://databricks.com/blog/2016/05/23/apache-spark-as-a-compiler-joining-a-billion-rows-per-second-on-a-laptop.html

// COMMAND ----------

// MAGIC %md
// MAGIC ### First, let's do some benchmark setup

// COMMAND ----------

// Define a simple benchmark util function
def benchmark(name: String)(f: => Unit) {
  val startTime = System.nanoTime
  f
  val endTime = System.nanoTime
  println(s"Time taken in $name: " + (endTime - startTime).toDouble / 1000000000 + " seconds")
}

// COMMAND ----------

// MAGIC %md
// MAGIC This "cluster" has only a single node, with 3 cores allocated. The CPU is Intel(R) Xeon(R) CPU E5-2670 v2 @ 2.50GHz.

// COMMAND ----------

// MAGIC %md
// MAGIC ### How fast can Spark 1.6 sum up 1 billion numbers?

// COMMAND ----------

// This config turns off whole stage code generation, effectively changing the execution path to be similar to Spark 1.6.
spark.conf.set("spark.sql.codegen.wholeStage", false)

benchmark("Spark 1.6") {
  spark.range(1000L * 1000 * 1000).selectExpr("sum(id)").show()
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### How fast can Spark 2.0 sum up 1 billion numbers?

// COMMAND ----------

spark.conf.set("spark.sql.codegen.wholeStage", true)

benchmark("Spark 2.0") {
  spark.range(1000L * 1000 * 1000).selectExpr("sum(id)").show()
}

// COMMAND ----------

// MAGIC %md
// MAGIC Woot - that's pretty good. From ~ 8 seconds to 0.7 seconds to sum up 1 billion numbers.

// COMMAND ----------

// MAGIC %md
// MAGIC ### How fast can Spark 1.6 join 1 billion records?

// COMMAND ----------

// This config turns off whole stage code generation, effectively changing the execution path to be similar to Spark 1.6.
spark.conf.set("spark.sql.codegen.wholeStage", false)

benchmark("Spark 1.6") {
  spark.range(1000L * 1000 * 1000).join(spark.range(1000L).toDF(), "id").count()
}

// COMMAND ----------

spark.range(1000L * 1000 * 1000).join(spark.range(1000L).toDF(), "id").selectExpr("count(*)").explain(true)

// COMMAND ----------

// MAGIC %md
// MAGIC ### How fast can Spark 2.0 join 1 billion records?

// COMMAND ----------

spark.conf.set("spark.sql.codegen.wholeStage", true)

benchmark("Spark 2.0") {
  spark.range(1000L * 1000 * 1005).join(spark.range(1040L).toDF(), "id").count()
}

// COMMAND ----------

// MAGIC %md
// MAGIC This is even better. From ~ 60 seconds to 0.8 seconds to join 1 billion numbers.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Why are we doing these stupid benchmarks?
// MAGIC 
// MAGIC This is an excellent question. While the benchmarks look simple, they actually measure many of the fundamental core primitives in real data processing. For example, aggregation is a very common pattern, and joining integers is one of the most widely used patterns in a well defined star-schema data warehouse (e.g. fact table and dimension table joins).

// COMMAND ----------

// MAGIC %md
// MAGIC ### Other primitive operations
// MAGIC 
// MAGIC We have also benchmarked the efficiency of other primitive operations under whole-stage code generation. The table below summarizes the result:
// MAGIC 
// MAGIC The way we benchmark is to to measure the cost per row, in nanoseconds.
// MAGIC 
// MAGIC Runtime: Intel Haswell i7 4960HQ 2.6GHz, HotSpot 1.8.0_60-b27, Mac OS X 10.11
// MAGIC 
// MAGIC |                       | Spark 1.6 | Spark 2.0 |
// MAGIC |:---------------------:|:---------:|:---------:|
// MAGIC |         filter        |   15 ns   |   1.1 ns  |
// MAGIC |     sum w/o group     |   14 ns   |   0.9 ns  |
// MAGIC |      sum w/ group     |   79 ns   |  10.7 ns  |
// MAGIC |       hash join       |   115 ns  |   4.0 ns  |
// MAGIC |  sort (8 bit entropy) |   620 ns  |   5.3 ns  |
// MAGIC | sort (64 bit entropy) |   620 ns  |   40 ns   |
// MAGIC |    sort-merge join    |   750 ns  |   700 ns  |
// MAGIC 
// MAGIC 
// MAGIC Again, to read the companion blog post, click here: https://databricks.com/blog/2016/05/11/spark-2-0-technical-preview-easier-faster-and-smarter.html

// COMMAND ----------

// MAGIC %md
// MAGIC ### Understanding the execution plan
// MAGIC 
// MAGIC The explain function has been extended for whole-stage code generation. When an operator has a star around it (*), whole-stage code generation is enabled. In the following case, Range, Filter, and the two Aggregates are both running with whole-stage code generation. Exchange does not have whole-stage code generation because it is sending data across the network.
// MAGIC 
// MAGIC This query plan has two "stages" (divided by Exchange). In the first stage, three operators (Range, Filter, Aggregate) are collapsed into a single function. In the second stage, there is only a single operator (Aggregate).

// COMMAND ----------

spark.range(1000).filter("id > 100").selectExpr("sum(id)").explain(true)

// COMMAND ----------

spark.range(1000).filter("id > 100").selectExpr("sum(id)").show()