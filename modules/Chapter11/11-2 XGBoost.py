# Databricks notebook source
# MAGIC %md # XGBoost
# MAGIC  
# MAGIC If you are not using the DBR 7.x ML Runtime, you will need to install `ml.dmlc:xgboost4j-spark_2.12:1.0.0` from Maven, well as `xgboost` from PyPI.
# MAGIC 
# MAGIC **NOTE:** There is currently only a distributed version of XGBoost for Scala, not Python. We will switch to Scala for that section.

# COMMAND ----------

# MAGIC %md ## Data Preparation
# MAGIC 
# MAGIC Let's go ahead and index all of our categorical features, and set our label to be `log(price)`.

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions.log
# MAGIC import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
# MAGIC import org.apache.spark.ml.Pipeline
# MAGIC 
# MAGIC val filePath = "/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet"
# MAGIC val airbnbDF = spark.read.parquet(filePath)
# MAGIC val Array(trainDF, testDF) = airbnbDF.withColumn("label", log($"price")).randomSplit(Array(.8, .2), seed=42)
# MAGIC 
# MAGIC val categoricalCols = trainDF.dtypes.filter(_._2 == "StringType").map(_._1)
# MAGIC val indexOutputCols = categoricalCols.map(_ + "Index")
# MAGIC 
# MAGIC val stringIndexer = new StringIndexer()
# MAGIC   .setInputCols(categoricalCols)
# MAGIC   .setOutputCols(indexOutputCols)
# MAGIC   .setHandleInvalid("skip")
# MAGIC 
# MAGIC val numericCols = trainDF.dtypes.filter{ case (field, dataType) => dataType == "DoubleType" && field != "price" && field != "label"}.map(_._1)
# MAGIC val assemblerInputs = indexOutputCols ++ numericCols
# MAGIC val vecAssembler = new VectorAssembler()
# MAGIC   .setInputCols(assemblerInputs)
# MAGIC   .setOutputCol("features")
# MAGIC 
# MAGIC val pipeline = new Pipeline()
# MAGIC   .setStages(Array(stringIndexer, vecAssembler))

# COMMAND ----------

from pyspark.sql.functions import log, col
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline

filePath = "/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet"
airbnbDF = spark.read.parquet(filePath)
(trainDF, testDF) = airbnbDF.withColumn("label", log(col("price"))).randomSplit([.8, .2], seed=42)

categoricalCols = [field for (field, dataType) in trainDF.dtypes if dataType == "string"]
indexOutputCols = [x + "Index" for x in categoricalCols]

stringIndexer = StringIndexer(inputCols=categoricalCols, outputCols=indexOutputCols, handleInvalid="skip")

numericCols = [field for (field, dataType) in trainDF.dtypes if ((dataType == "double") & (field != "price") & (field != "label"))]
assemblerInputs = indexOutputCols + numericCols
vecAssembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
pipeline = Pipeline(stages=[stringIndexer, vecAssembler])

# COMMAND ----------

# MAGIC %md ## Scala
# MAGIC 
# MAGIC Distributed XGBoost with Spark only has a Scala API, so we are going to create views of our DataFrames to use in Scala, as well as save our (untrained) pipeline to load in to Scala.

# COMMAND ----------

# MAGIC %scala
# MAGIC trainDF.createOrReplaceTempView("trainDF")
# MAGIC testDF.createOrReplaceTempView("testDF")
# MAGIC 
# MAGIC val fileName = "/tmp/xgboost_feature_pipeline"
# MAGIC pipeline.write.overwrite().save(fileName)

# COMMAND ----------

trainDF.createOrReplaceTempView("trainDF")
testDF.createOrReplaceTempView("testDF")

fileName = "/tmp/xgboost_feature_pipeline"
pipeline.write().overwrite().save(fileName)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data/Pipeline in Scala
# MAGIC 
# MAGIC This section is only available in Scala because there is no distributed Python API for XGBoost in Spark yet.
# MAGIC 
# MAGIC Let's load in our data/pipeline that we defined in Python. 

# COMMAND ----------

# MAGIC %scala
# MAGIC // ALL_NOTEBOOKS
# MAGIC import org.apache.spark.ml.Pipeline
# MAGIC 
# MAGIC val fileName = "tmp/xgboost_feature_pipeline"
# MAGIC val pipeline = Pipeline.load(fileName)
# MAGIC 
# MAGIC val trainDF = spark.table("trainDF")
# MAGIC val testDF = spark.table("testDF")

# COMMAND ----------

# MAGIC %md ## XGBoost
# MAGIC 
# MAGIC Now we are ready to train our XGBoost model!

# COMMAND ----------

# MAGIC %scala
# MAGIC // ALL_NOTEBOOKS
# MAGIC 
# MAGIC import ml.dmlc.xgboost4j.scala.spark._
# MAGIC import org.apache.spark.sql.functions._
# MAGIC 
# MAGIC val paramMap = List("num_round" -> 100, "eta" -> 0.1, "max_leaf_nodes" -> 50, "seed" -> 42, "missing" -> 0).toMap
# MAGIC 
# MAGIC val xgboostEstimator = new XGBoostRegressor(paramMap)
# MAGIC 
# MAGIC val xgboostPipeline = new Pipeline().setStages(pipeline.getStages ++ Array(xgboostEstimator))
# MAGIC 
# MAGIC val xgboostPipelineModel = xgboostPipeline.fit(trainDF)
# MAGIC val xgboostLogPredictedDF = xgboostPipelineModel.transform(testDF)
# MAGIC 
# MAGIC val expXgboostDF = xgboostLogPredictedDF.withColumn("prediction", exp(col("prediction")))
# MAGIC expXgboostDF.createOrReplaceTempView("expXgboostDF")

# COMMAND ----------

# MAGIC %md ## Evaluate
# MAGIC 
# MAGIC Now we can evaluate how well our XGBoost model performed.

# COMMAND ----------

# MAGIC %scala
# MAGIC val expXgboostDF = spark.table("expXgboostDF")
# MAGIC 
# MAGIC display(expXgboostDF.select("price", "prediction"))

# COMMAND ----------

expXgboostDF = spark.table("expXgboostDF")

display(expXgboostDF.select("price", "prediction"))

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.ml.evaluation.RegressionEvaluator
# MAGIC 
# MAGIC val regressionEvaluator = new RegressionEvaluator()
# MAGIC   .setLabelCol("price")
# MAGIC   .setPredictionCol("prediction")
# MAGIC   .setMetricName("rmse")
# MAGIC 
# MAGIC val rmse = regressionEvaluator.evaluate(expXgboostDF)
# MAGIC val r2 = regressionEvaluator.setMetricName("r2").evaluate(expXgboostDF)
# MAGIC println(s"RMSE is $rmse")
# MAGIC println(s"R2 is $r2")
# MAGIC println("*-"*80)

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator

regressionEvaluator = RegressionEvaluator(predictionCol="prediction", labelCol="price", metricName="rmse")

rmse = regressionEvaluator.evaluate(expXgboostDF)
r2 = regressionEvaluator.setMetricName("r2").evaluate(expXgboostDF)
print(f"RMSE is {rmse}")
print(f"R2 is {r2}")

# COMMAND ----------

# MAGIC %md ## Export to Python
# MAGIC 
# MAGIC We can also export our XGBoost model to use in Python for fast inference on small datasets.

# COMMAND ----------

# MAGIC %scala
# MAGIC // ALL_NOTEBOOKS
# MAGIC 
# MAGIC val nativeModelPath = "xgboost_native_model"
# MAGIC val xgboostModel = xgboostPipelineModel.stages.last.asInstanceOf[XGBoostRegressionModel]
# MAGIC xgboostModel.nativeBooster.saveModel(nativeModelPath)

# COMMAND ----------

# MAGIC %md ## Predictions in Python
# MAGIC 
# MAGIC Let's pass in an example record to our Python XGBoost model and see how fast we can get predictions!!
# MAGIC 
# MAGIC Don't forget to exponentiate!

# COMMAND ----------

# ALL_NOTEBOOKS
import numpy as np
import xgboost as xgb
bst = xgb.Booster({'nthread': 4})
bst.load_model("xgboost_native_model")

# Per https://stackoverflow.com/questions/55579610/xgboost-attributeerror-dataframe-object-has-no-attribute-feature-names, DMatrix did the trick

data = np.array([[0.0, 2.0, 0.0, 14.0, 1.0, 0.0, 0.0, 1.0, 37.72001, -122.39249, 2.0, 1.0, 1.0, 1.0, 2.0, 128.0, 97.0, 10.0, 10.0, 10.0, 10.0, 9.0, 10.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]])
log_pred = bst.predict(xgb.DMatrix(data))
print(f"The predicted price for this rental is ${np.exp(log_pred)[0]:.2f}")