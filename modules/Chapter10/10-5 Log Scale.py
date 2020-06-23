# Databricks notebook source
# MAGIC %md # Log Scale
# MAGIC 
# MAGIC In the lab, you will improve your model performance by transforming the label to log scale, predicting on log scale, then exponentiating to evaluate the result. 

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC filePath = "/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet"
# MAGIC airbnbDF = spark.read.parquet(filePath)
# MAGIC (trainDF, testDF) = airbnbDF.randomSplit([.8, .2], seed=42)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val filePath = "/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet"
# MAGIC val airbnbDF = spark.read.parquet(filePath)
# MAGIC val Array(trainDF, testDF) = airbnbDF.randomSplit(Array(.8, .2), seed=42)

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions.{col, log}
# MAGIC import org.apache.spark.ml.Pipeline
# MAGIC import org.apache.spark.ml.feature.RFormula
# MAGIC import org.apache.spark.ml.regression.LinearRegression
# MAGIC 
# MAGIC val logTrainDF = trainDF.withColumn("log_price", log(col("price")))
# MAGIC val logTestDF = testDF.withColumn("log_price", log(col("price")))
# MAGIC 
# MAGIC val rFormula = new RFormula()
# MAGIC   .setFormula("log_price ~ . - price")
# MAGIC   .setFeaturesCol("features")
# MAGIC   .setLabelCol("log_price")
# MAGIC   .setHandleInvalid("skip")
# MAGIC 
# MAGIC val lr = new LinearRegression()
# MAGIC              .setLabelCol("log_price")
# MAGIC              .setPredictionCol("log_pred")
# MAGIC 
# MAGIC val pipeline = new Pipeline().setStages(Array(rFormula, lr))
# MAGIC val pipelineModel = pipeline.fit(logTrainDF)
# MAGIC val predDF = pipelineModel.transform(logTestDF)

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.functions import col, log
# MAGIC from pyspark.ml import Pipeline
# MAGIC from pyspark.ml.feature import RFormula
# MAGIC from pyspark.ml.regression import LinearRegression
# MAGIC 
# MAGIC logTrainDF = trainDF.withColumn("log_price", log(col("price")))
# MAGIC logTestDF = testDF.withColumn("log_price", log(col("price")))
# MAGIC 
# MAGIC rFormula = RFormula(formula="log_price ~ . - price", featuresCol="features", labelCol="log_price", handleInvalid="skip") 
# MAGIC 
# MAGIC lr = LinearRegression(labelCol="log_price", predictionCol="log_pred")
# MAGIC pipeline = Pipeline(stages = [rFormula, lr])
# MAGIC pipelineModel = pipeline.fit(logTrainDF)
# MAGIC predDF = pipelineModel.transform(logTestDF)

# COMMAND ----------

# MAGIC %md ## Exponentiate
# MAGIC 
# MAGIC In order to interpret our RMSE, we need to convert our predictions back from logarithmic scale.

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.ml.evaluation.RegressionEvaluator
# MAGIC import org.apache.spark.sql.functions.{col, exp}
# MAGIC 
# MAGIC val expDF = predDF.withColumn("prediction", exp(col("log_pred")))
# MAGIC 
# MAGIC val regressionEvaluator = new RegressionEvaluator()
# MAGIC   .setPredictionCol("prediction")
# MAGIC   .setLabelCol("price")
# MAGIC 
# MAGIC val rmse = regressionEvaluator.setMetricName("rmse").evaluate(expDF)
# MAGIC val r2 = regressionEvaluator.setMetricName("r2").evaluate(expDF)
# MAGIC println(s"RMSE is $rmse")
# MAGIC println(s"R2 is $r2")
# MAGIC println("*-"*80)

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.ml.evaluation import RegressionEvaluator
# MAGIC from pyspark.sql.functions import col, exp
# MAGIC 
# MAGIC expDF = predDF.withColumn("prediction", exp(col("log_pred")))
# MAGIC 
# MAGIC regressionEvaluator = RegressionEvaluator(labelCol="price", predictionCol="prediction")
# MAGIC rmse = regressionEvaluator.setMetricName("rmse").evaluate(expDF)
# MAGIC r2 = regressionEvaluator.setMetricName("r2").evaluate(expDF)
# MAGIC print(f"RMSE is {rmse}")
# MAGIC print(f"R2 is {r2}")