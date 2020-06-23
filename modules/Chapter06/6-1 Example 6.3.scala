// Databricks notebook source
// MAGIC %md # Example 6.3

// COMMAND ----------

// MAGIC %md Generate Sample Data

// COMMAND ----------

import scala.util.Random._
// our case class for the Dataset
case class Usage(uid:Int, uname:String, usage: Int)
val r = new scala.util.Random(42)
// create 1000 instances of scala Usage class 
// this generates data on the fly
val data = for (i <- 0 to 1000) 
yield (Usage(i, "user-" + r.alphanumeric.take(5).mkString(""), r.nextInt(1000)))
// create a dataset of Usage typed data
val dsUsage = spark.createDataset(data)
dsUsage.show(10)

// COMMAND ----------

import org.apache.spark.sql.functions._
dsUsage
  .filter(d => d.usage > 900)
  .orderBy(desc("usage"))
  .show(5, false)

// COMMAND ----------

// MAGIC %md Closure

// COMMAND ----------

def filterValue(f: Int) = (v: Int) => v > f 

// COMMAND ----------

// MAGIC %md High order functions

// COMMAND ----------

def filterValue2(v: Int, f: Int): Boolean =  v > f 

// COMMAND ----------

def filterWithUsage(u: Usage) = u.usage > 900

dsUsage.filter(filterWithUsage(_)).orderBy(desc("usage")).show(5)

// COMMAND ----------

// use an if-then-else lambda expression and compute a value
dsUsage.map(u => {if (u.usage > 750) u.usage * .15 else u.usage * .50 }).show(5, false)
// define a function to compute the usage
def computeCostUsage(usage: Int): Double = {
  if (usage > 750) usage * 0.15 else usage * 0.50
}
// Use the function as an argument to map
dsUsage.map(u => {computeCostUsage(u.usage)}).show(5, false)

// COMMAND ----------

def computeCostUsage(usage: Int): Double = {
  if (usage > 750) usage * 0.15 else usage * 0.50
}

// COMMAND ----------

dsUsage.map(u => {computeCostUsage(u.usage)}).show(5, false)

// COMMAND ----------

dsUsage.map(u => { if (u.usage > 750) u.usage * .15 else u.usage * .50 }).show(5, false)

// COMMAND ----------

case class UsageCosts(uid: Int, uname:String, usage: Int, cost: Double)

def computeUserCostUsage(u: Usage): UsageCosts = {
  val v = if (u.usage > 750) u.usage  * 0.15 else u.usage  * 0.50
    UsageCosts(u.uid, u.uname,u.usage, v)
}

// COMMAND ----------

dsUsage.map(u => {computeUserCostUsage(u)}).show(5)