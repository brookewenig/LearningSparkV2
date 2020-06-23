# Databricks notebook source
timeout = 600
language = "Python"

# COMMAND ----------

# MAGIC %md # Chapter 2

# COMMAND ----------

dbutils.notebook.run("./Chapter02/2-1 Line Count", timeout)

# COMMAND ----------

dbutils.notebook.run("./Chapter02/2-2 M&M Count", timeout)

# COMMAND ----------

# MAGIC %md # Chapter 3

# COMMAND ----------

dbutils.notebook.run("./Chapter03/3-1 Example 3.7", timeout)

# COMMAND ----------

dbutils.notebook.run("./Chapter03/3-2 San Francisco Fire Calls", timeout)

# COMMAND ----------

if language == "Scala":
  dbutils.notebook.run("./Chapter03/3-3 IoT Devices", timeout)
else:
  print('no Python')

# COMMAND ----------

# MAGIC %md # Chapter 4

# COMMAND ----------

dbutils.notebook.run("./Chapter04/4-1 Example 4.1", timeout)

# COMMAND ----------

dbutils.notebook.run("./Chapter04/4-2 Spark Data Sources", timeout)

# COMMAND ----------

dbutils.notebook.run("./Chapter04/4-3 Spark Tables", timeout)

# COMMAND ----------

# MAGIC %md # Chapter 5

# COMMAND ----------

dbutils.notebook.run("./Chapter05/5-1 Spark SQL & UDFs", timeout)

# COMMAND ----------

# MAGIC %md # Chapter 6

# COMMAND ----------

if language == "Scala":
  dbutils.notebook.run("./Chapter06/6-1 Example 6.3", timeout)
else:
  print('no Python')

# COMMAND ----------

if language == "Scala":
  dbutils.notebook.run("./Chapter06/6-2 Dataset API", timeout)
else:
  print('no Python')

# COMMAND ----------

# MAGIC %md # Chapter 7

# COMMAND ----------

if language == "Scala":
  dbutils.notebook.run("./Chapter07/7-1 Spark Configs", timeout)
else:
  print('no Python')

# COMMAND ----------

if language == "Scala":
  dbutils.notebook.run("./Chapter07/7-2 Partitions", timeout)
else:
  print('no Python')

# COMMAND ----------

if language == "Scala":
  dbutils.notebook.run("./Chapter07/7-3 Map & MapPartitions", timeout)
else:
  print('no Python')

# COMMAND ----------

dbutils.notebook.run("./Chapter07/7-4 Vectorized UDFs", timeout)

# COMMAND ----------

dbutils.notebook.run("./Chapter07/7-5 Caching Data", timeout)

# COMMAND ----------

if language == "Scala":
  dbutils.notebook.run("./Chapter07/7-6 Sort Merge Join", timeout)
else:
  print('no Python')

# COMMAND ----------

if language == "Scala":
  dbutils.notebook.run("./Chapter07/7-7 Whole-stage code generation", timeout)
else:
  print('no Python')

# COMMAND ----------

dbutils.notebook.run("./Chapter07/7-8 File Formats", timeout)

# COMMAND ----------

# MAGIC %md # Chapter 8 - to update

# COMMAND ----------

# dbutils.notebook.run("./Chapter08/8-1 The Fundamentals of a Streaming Query", timeout)

# COMMAND ----------

# dbutils.notebook.run("./Chapter08/8-2 Streaming Data Sources and Sinks", timeout)

# COMMAND ----------

# dbutils.notebook.run("./Chapter08/8-3 Streaming Aggregations and Joins", timeout)

# COMMAND ----------

# dbutils.notebook.run("./Chapter08/8-4 Arbitrary Stateful Computations", timeout)

# COMMAND ----------

# dbutils.notebook.run("./Chapter08/8-5 Monitoring Queries", timeout)

# COMMAND ----------

# MAGIC %md # Chapter 9

# COMMAND ----------

dbutils.notebook.run("./Chapter09/9-1 Delta Lake", timeout)

# COMMAND ----------

# MAGIC %md # Chapter 10

# COMMAND ----------

dbutils.notebook.run("./Chapter10/10-1 Data Cleansing", timeout)

# COMMAND ----------

dbutils.notebook.run("./Chapter10/10-2 Linear Regression", timeout)

# COMMAND ----------

dbutils.notebook.run("./Chapter10/10-3 Baseline Model", timeout)

# COMMAND ----------

dbutils.notebook.run("./Chapter10/10-4 One Hot Encoding", timeout*2)

# COMMAND ----------

dbutils.notebook.run("./Chapter10/10-5 Log Scale", timeout)

# COMMAND ----------

dbutils.notebook.run("./Chapter10/10-6 Decision Trees", timeout)

# COMMAND ----------

dbutils.notebook.run("./Chapter10/10-7 Hyperparameter Tuning", timeout*2)

# COMMAND ----------

dbutils.notebook.run("./Chapter10/10-8 K-Means", timeout)

# COMMAND ----------

# MAGIC %md # Chapter 11

# COMMAND ----------

if language == "Python":
  dbutils.notebook.run("./Chapter11/11-1 MLflow", timeout)
else:
  print('no Scala')

# COMMAND ----------

dbutils.notebook.run("./Chapter11/11-2 XGBoost", timeout)

# COMMAND ----------

if language == "Python":
  dbutils.notebook.run("./Chapter11/11-3 Distributed Inference", timeout)
else:
  print('no Scala')

# COMMAND ----------

if language == "Python":
  dbutils.notebook.run("./Chapter11/11-4 Distributed IoT Model Training", timeout)
else:
  print('no Scala')

# COMMAND ----------

if language == "Python":
  dbutils.notebook.run("./Chapter11/11-5 Joblib", timeout)
else:
  print('no Scala')

# COMMAND ----------

if language == "Python":
  dbutils.notebook.run("./Chapter11/11-6 Hyperopt", timeout)
else:
  print('no Scala')

# COMMAND ----------

if language == "Python":
  dbutils.notebook.run("./Chapter11/11-7 Koalas", timeout)
else:
  print('no Scala')

# COMMAND ----------

# MAGIC %md # Chapter 12

# COMMAND ----------

dbutils.notebook.run("./Chapter12/12-1 Dataset and SQL Explain", timeout)

# COMMAND ----------

dbutils.notebook.run("./Chapter12/12-2 AQE", timeout*2)

# COMMAND ----------

if language == "Python":
  dbutils.notebook.run("./Chapter12/12-3 Pandas UDFs", timeout)
else:
  print('no Scala')