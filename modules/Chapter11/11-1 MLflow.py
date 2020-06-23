# Databricks notebook source
# MAGIC %md # Tracking Models with MLflow
# MAGIC 
# MAGIC MLflow is pre-installed on the Databricks Runtime for ML. If you are not using the ML Runtime, you will need to install mlflow.

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

filePath = "/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet"
airbnbDF = spark.read.parquet(filePath)
(trainDF, testDF) = airbnbDF.randomSplit([.8, .2], seed=42)

categoricalCols = [field for (field, dataType) in trainDF.dtypes 
                   if dataType == "string"]
indexOutputCols = [x + "Index" for x in categoricalCols]
stringIndexer = StringIndexer(inputCols=categoricalCols, 
                              outputCols=indexOutputCols, 
                              handleInvalid="skip")

numericCols = [field for (field, dataType) in trainDF.dtypes 
               if ((dataType == "double") & (field != "price"))]
assemblerInputs = indexOutputCols + numericCols
vecAssembler = VectorAssembler(inputCols=assemblerInputs, 
                               outputCol="features")

rf = RandomForestRegressor(labelCol="price", maxBins=40, maxDepth=5, 
                           numTrees=100, seed=42)

pipeline = Pipeline(stages=[stringIndexer, vecAssembler, rf])

# COMMAND ----------

# MAGIC %md ## MLflow

# COMMAND ----------

import mlflow
import mlflow.spark
import pandas as pd

with mlflow.start_run(run_name="random-forest") as run:
  # Log params: Num Trees and Max Depth
  mlflow.log_param("num_trees", rf.getNumTrees())
  mlflow.log_param("max_depth", rf.getMaxDepth())
 
  # Log model
  pipelineModel = pipeline.fit(trainDF)
  mlflow.spark.log_model(pipelineModel, "model")

  # Log metrics: RMSE and R2
  predDF = pipelineModel.transform(testDF)
  regressionEvaluator = RegressionEvaluator(predictionCol="prediction", 
                                            labelCol="price")
  rmse = regressionEvaluator.setMetricName("rmse").evaluate(predDF)
  r2 = regressionEvaluator.setMetricName("r2").evaluate(predDF)
  mlflow.log_metrics({"rmse": rmse, "r2": r2})

  # Log artifact: Feature Importance Scores
  rfModel = pipelineModel.stages[-1]
  pandasDF = (pd.DataFrame(list(zip(vecAssembler.getInputCols(), 
                                    rfModel.featureImportances)), 
                          columns=["feature", "importance"])
              .sort_values(by="importance", ascending=False))
  # First write to local filesystem, then tell MLflow where to find that file
  pandasDF.to_csv("feature-importance.csv", index=False)
  mlflow.log_artifact("feature-importance.csv")

# COMMAND ----------

# MAGIC %md ## MLflowClient

# COMMAND ----------

from mlflow.tracking import MlflowClient

client = MlflowClient()
runs = client.search_runs(run.info.experiment_id,
                          order_by=["attributes.start_time desc"], 
                          max_results=1)
run_id = runs[0].info.run_id
runs[0].data.metrics

# COMMAND ----------

# MAGIC %md %md ## Generate Batch Predictions
# MAGIC 
# MAGIC Let's load the model back in to generate batch predictions

# COMMAND ----------

# Load saved model with MLflow
pipelineModel = mlflow.spark.load_model(f"runs:/{run_id}/model")

# Generate Predictions
inputPath = "/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet"
inputDF = spark.read.parquet(inputPath)
predDF = pipelineModel.transform(inputDF)

# COMMAND ----------

# MAGIC %md ## Generate Streaming Predictions
# MAGIC 
# MAGIC We can do the same thing to generate streaming predictions.

# COMMAND ----------

# Load saved model with MLflow
pipelineModel = mlflow.spark.load_model(f"runs:/{run_id}/model")

# Set up simulated streaming data
repartitionedPath = "/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean-100p.parquet"
schema = spark.read.parquet(repartitionedPath).schema

streamingData = (spark
                 .readStream
                 .schema(schema) # Can set the schema this way
                 .option("maxFilesPerTrigger", 1)
                 .parquet(repartitionedPath))

# Generate Predictions
streamPred = pipelineModel.transform(streamingData)

# Uncomment the line below to see the streaming predictions
# display(streamPred)

# Just remember to stop your stream at the end!
# streamPred.exit()