# Databricks notebook source
# MAGIC %md # Baseline Model
# MAGIC 
# MAGIC Let's compute the average `price` on the training dataset, and use that as our prediction column for our test dataset, then evaluate the result.

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions.{avg, lit}
# MAGIC 
# MAGIC val filePath = "/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet"
# MAGIC val airbnbDF = spark.read.parquet(filePath)
# MAGIC val Array(trainDF, testDF) = airbnbDF.randomSplit(Array(.8, .2), seed=42)
# MAGIC 
# MAGIC val avgPrice = trainDF.select(avg("price")).first().getDouble(0)
# MAGIC 
# MAGIC val predDF = testDF.withColumn("avgPrediction", lit(avgPrice))

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.functions import avg, lit
# MAGIC 
# MAGIC filePath = "/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet"
# MAGIC airbnbDF = spark.read.parquet(filePath)
# MAGIC trainDF, testDF = airbnbDF.randomSplit([.8, .2], seed=42)
# MAGIC 
# MAGIC avgPrice = float(trainDF.select(avg("price")).first()[0])
# MAGIC predDF = testDF.withColumn("avgPrediction", lit(avgPrice))

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.ml.evaluation.RegressionEvaluator
# MAGIC 
# MAGIC val regressionMeanEvaluator = new RegressionEvaluator()
# MAGIC   .setPredictionCol("avgPrediction")
# MAGIC   .setLabelCol("price")
# MAGIC   .setMetricName("rmse")
# MAGIC 
# MAGIC val rmse = regressionMeanEvaluator.evaluate(predDF)
# MAGIC println (f"The RMSE for predicting the average price is: $rmse%1.2f")

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.ml.evaluation import RegressionEvaluator
# MAGIC 
# MAGIC regressionMeanEvaluator = RegressionEvaluator(predictionCol="avgPrediction", labelCol="price", metricName="rmse")
# MAGIC 
# MAGIC print(f"The RMSE for predicting the average price is: {regressionMeanEvaluator.evaluate(predDF):.2f}")