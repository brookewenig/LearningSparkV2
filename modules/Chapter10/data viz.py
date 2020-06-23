# Databricks notebook source
filePath = "/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet"
airbnbDF = spark.read.parquet(filePath)
(trainDF, testDF) = airbnbDF.randomSplit([.8, .2], seed=42)

# COMMAND ----------

from pyspark.sql.functions import *

pdDF = trainDF.selectExpr("log(price) as log_price").toPandas()

# COMMAND ----------

import matplotlib.pyplot as plt
pdDF.plot(kind="hist", bins=25, figsize=(15, 7), fontsize=15)
plt.suptitle('Log price of SF Airbnb Rentals', fontsize=25)
plt.ylabel('Frequency', fontsize=20)
plt.legend(prop={'size': 20})
plt.xlabel('Log Price', fontsize=20)
plt.show()

# COMMAND ----------

priceDF = trainDF.selectExpr("price").toPandas()

# COMMAND ----------

priceDF.plot(kind="hist",  bins=200, figsize=(15, 7), fontsize=15)
plt.suptitle("Price of SF Airbnb Rentals", fontsize=25)
plt.ylabel('Frequency', fontsize=20)
plt.legend(prop={'size': 20})
plt.xlabel('Price', fontsize=20)
plt.show()

# COMMAND ----------

