-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Adaptive Query Execution Demo
-- MAGIC 
-- MAGIC Adaptive Query Execution (AQE) is query re-optimization that occurs during query execution based on runtime statistics. AQE in Spark 3.0 includes 3 main features:
-- MAGIC * Dynamically coalescing shuffle partitions
-- MAGIC * Dynamically switching join strategies
-- MAGIC * Dynamically optimizing skew joins

-- COMMAND ----------

-- MAGIC %md ## Enable AQE

-- COMMAND ----------

-- ALL_NOTEBOOKS
set spark.sql.adaptive.enabled = true;

-- COMMAND ----------

-- ALL_NOTEBOOKS
-- For demo purpose only.
-- Not necessary in real-life usage.

set spark.sql.adaptive.coalescePartitions.minPartitionNum = 1;

-- COMMAND ----------

-- MAGIC %md ## Create Tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/aqe_demo_db", True)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/aqe_demo_db", true)

-- COMMAND ----------

-- ALL_NOTEBOOKS
CREATE DATABASE IF NOT EXISTS aqe_demo_db;
USE aqe_demo_db;

DROP TABLE IF EXISTS items;
DROP TABLE IF EXISTS sales;

-- Create "items" table.

CREATE TABLE items
USING parquet
AS
SELECT id AS i_item_id,
CAST(rand() * 1000 AS INT) AS i_price
FROM RANGE(30000000);

-- Create "sales" table with skew.
-- Item with id 100 is in 80% of all sales.

CREATE TABLE sales
USING parquet
AS
SELECT CASE WHEN rand() < 0.8 THEN 100 ELSE CAST(rand() * 30000000 AS INT) END AS s_item_id,
CAST(rand() * 100 AS INT) AS s_quantity,
DATE_ADD(current_date(), - CAST(rand() * 360 AS INT)) AS s_date
FROM RANGE(1000000000);

-- COMMAND ----------

-- MAGIC %md ## Dynamically Coalesce Shuffle Partitions

-- COMMAND ----------

-- ALL_NOTEBOOKS
-- Get the sums of sales quantity grouped by sales date.
-- The grouped result is very small.

SELECT s_date, sum(s_quantity) AS q
FROM sales
GROUP BY s_date
ORDER BY q DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * The partition sizes after aggregation are very small: 13KB on average, 431KB in total (see the highlighted box **shuffle bytes written**).
-- MAGIC * AQE combines these small partitions into one new partition (see the highlighted box **CustomShuffleReader**).
-- MAGIC 
-- MAGIC ![screenshot_coalesce](https://docs.databricks.com/_static/images/spark/aqe/coalesce_partitions.png)

-- COMMAND ----------

-- MAGIC %md ## Dynamically Switch Join Strategies

-- COMMAND ----------

-- ALL_NOTEBOOKS
-- Get total sales amount grouped by sales date for items with a price lower than 10.
-- The selectivity of the filter by price is not known in static planning, so the initial plan opts for sort merge join.
-- But in fact, the "items" table after filtering is very small, so the query can do a broadcast hash join instead.

-- Static explain shows the initial plan with sort merge join.

EXPLAIN
SELECT s_date, sum(s_quantity * i_price) AS total_sales
FROM sales
JOIN items ON s_item_id = i_item_id
WHERE i_price < 10
GROUP BY s_date
ORDER BY total_sales DESC;

-- COMMAND ----------

-- ALL_NOTEBOOKS
-- The runtime join strategy is changed to broadcast hash join.

SELECT s_date, sum(s_quantity * i_price) AS total_sales
FROM sales
JOIN items ON s_item_id = i_item_id
WHERE i_price < 10
GROUP BY s_date
ORDER BY total_sales DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * The data size of the "items" table after filtering is very small 6.9 MB (see the highlighted box **data size**).
-- MAGIC *  AQE changes the sort merge join to broadcast hash join at runtime (see the highlighted box **BroadcastHashJoin**).
-- MAGIC 
-- MAGIC ![screenshot_strategy](https://docs.databricks.com/_static/images/spark/aqe/join_strategy.png)

-- COMMAND ----------

-- MAGIC 
-- MAGIC %md ## Dynamically Optimize Skew Join

-- COMMAND ----------

-- ALL_NOTEBOOKS
-- Get the total sales amount grouped by sales date.
-- The partition in the "sales" table containing value "100" as "s_item_id" is much larger than other partitions.
-- AQE splits the skewed partition into smaller partitions before joining the "sales" table with the "items" table.

SELECT s_date, sum(s_quantity * i_price) AS total_sales
FROM sales
JOIN items ON i_item_id = s_item_id
GROUP BY s_date
ORDER BY total_sales DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * There is a skewed partition from the "sales" table (see the highlighted box **number of skewed partitions**).
-- MAGIC * AQE splits the skewed partition into smaller partitions (see the highlighted box **number of skewed partition splits**).
-- MAGIC * The sort merge join operator is marked with a skew join flag (see the highlighted box **SortMergeJoin(isSkew=true)**).
-- MAGIC 
-- MAGIC ![screenshot_skew](https://docs.databricks.com/_static/images/spark/aqe/skew_join.png)