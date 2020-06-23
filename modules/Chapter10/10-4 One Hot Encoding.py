# Databricks notebook source
# MAGIC %md # One-Hot Encoding
# MAGIC 
# MAGIC In this notebook we will be adding additional features to our model, as well as discuss how to handle categorical features.

# COMMAND ----------

# MAGIC %scala
# MAGIC val filePath = "/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet"
# MAGIC val airbnbDF = spark.read.parquet(filePath)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC filePath = "/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet"
# MAGIC airbnbDF = spark.read.parquet(filePath)

# COMMAND ----------

# MAGIC %md ## Train/Test Split
# MAGIC 
# MAGIC Let's use the same 80/20 split with the same seed as the previous notebook so we can compare our results apples to apples (unless you changed the cluster config!)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val Array(trainDF, testDF) = airbnbDF.randomSplit(Array(.8, .2), seed=42)

# COMMAND ----------

# MAGIC %python
# MAGIC trainDF, testDF = airbnbDF.randomSplit([.8, .2], seed=42)

# COMMAND ----------

# MAGIC %md ## Option 1: StringIndexer, OneHotEncoder, and VectorAssembler 
# MAGIC 
# MAGIC Here, we are going to One Hot Encode (OHE) our categorical variables. The first approach we are going to use will combine StringIndexer, OneHotEncoder, and VectorAssembler.
# MAGIC 
# MAGIC First we need to use `StringIndexer` to map a string column of labels to an ML column of label indices [Python](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.StringIndexer)/[Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.feature.StringIndexer).
# MAGIC 
# MAGIC Then, we can apply the `OneHotEncoder` to the output of the StringIndexer [Python](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.OneHotEncoder)/[Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.feature.OneHotEncoder).

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
# MAGIC 
# MAGIC val categoricalCols = trainDF.dtypes.filter(_._2 == "StringType").map(_._1)
# MAGIC val indexOutputCols = categoricalCols.map(_ + "Index")
# MAGIC val oheOutputCols = categoricalCols.map(_ + "OHE")
# MAGIC 
# MAGIC val stringIndexer = new StringIndexer()
# MAGIC   .setInputCols(categoricalCols)
# MAGIC   .setOutputCols(indexOutputCols)
# MAGIC   .setHandleInvalid("skip")
# MAGIC 
# MAGIC val oheEncoder = new OneHotEncoder()
# MAGIC   .setInputCols(indexOutputCols)
# MAGIC   .setOutputCols(oheOutputCols)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.ml.feature import OneHotEncoder, StringIndexer
# MAGIC 
# MAGIC categoricalCols = [field for (field, dataType) in trainDF.dtypes 
# MAGIC                    if dataType == "string"]
# MAGIC indexOutputCols = [x + "Index" for x in categoricalCols]
# MAGIC oheOutputCols = [x + "OHE" for x in categoricalCols]
# MAGIC 
# MAGIC stringIndexer = StringIndexer(inputCols=categoricalCols, 
# MAGIC                               outputCols=indexOutputCols, 
# MAGIC                               handleInvalid="skip")
# MAGIC oheEncoder = OneHotEncoder(inputCols=indexOutputCols, 
# MAGIC                            outputCols=oheOutputCols)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can combine our OHE categorical features with our numeric features.

# COMMAND ----------

# MAGIC %scala 
# MAGIC 
# MAGIC import org.apache.spark.ml.feature.VectorAssembler
# MAGIC 
# MAGIC val numericCols = trainDF.dtypes.filter{ case (field, dataType) => 
# MAGIC   dataType == "DoubleType" && field != "price"}.map(_._1)
# MAGIC val assemblerInputs = oheOutputCols ++ numericCols
# MAGIC val vecAssembler = new VectorAssembler()
# MAGIC   .setInputCols(assemblerInputs)
# MAGIC   .setOutputCol("features")

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.ml.feature import VectorAssembler
# MAGIC 
# MAGIC numericCols = [field for (field, dataType) in trainDF.dtypes 
# MAGIC                if ((dataType == "double") & (field != "price"))]
# MAGIC assemblerInputs = oheOutputCols + numericCols
# MAGIC vecAssembler = VectorAssembler(inputCols=assemblerInputs, 
# MAGIC                                outputCol="features")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 2: RFormula
# MAGIC Instead of manually specifying which columns are categorical to the StringIndexer and OneHotEncoder, RFormula can do that automatically for you [Python](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.RFormula)/[Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.feature.RFormula).
# MAGIC 
# MAGIC With RFormula, if you have any columns of type String, it treats it as a categorical feature and string indexes & one hot encodes it for us. Otherwise, it leaves as it is. Then it combines all of one-hot encoded features and numeric features into a single vector, called `features`.

# COMMAND ----------

from pyspark.ml.feature import RFormula

rFormula = RFormula(formula="price ~ .", featuresCol="features", labelCol="price", handleInvalid="skip")

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.ml.feature.RFormula
# MAGIC 
# MAGIC val rFormula = new RFormula()
# MAGIC   .setFormula("price ~ .")
# MAGIC   .setFeaturesCol("features")
# MAGIC   .setLabelCol("price")
# MAGIC   .setHandleInvalid("skip")

# COMMAND ----------

# MAGIC %md ## Linear Regression
# MAGIC 
# MAGIC Now that we have all of our features, let's build a linear regression model.

# COMMAND ----------

# MAGIC %scala 
# MAGIC import org.apache.spark.ml.regression.LinearRegression
# MAGIC 
# MAGIC val lr = new LinearRegression()
# MAGIC   .setLabelCol("price")
# MAGIC   .setFeaturesCol("features")

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.ml.regression import LinearRegression
# MAGIC 
# MAGIC lr = LinearRegression(labelCol="price", featuresCol="features")

# COMMAND ----------

# MAGIC %md ## Pipeline
# MAGIC 
# MAGIC Let's put all these stages in a Pipeline. A `Pipeline` is a way of organizing all of our transformers and estimators [Python](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.Pipeline)/[Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.Pipeline).
# MAGIC 
# MAGIC Verify you get the same results with Option 1 (StringIndexer, OneHotEncoderEstimator, and VectorAssembler) and Option 2 (RFormula)

# COMMAND ----------

# MAGIC %scala
# MAGIC // Option 1: StringIndexer + OHE + VectorAssembler
# MAGIC import org.apache.spark.ml.Pipeline
# MAGIC 
# MAGIC val stages = Array(stringIndexer, oheEncoder, vecAssembler,  lr)
# MAGIC 
# MAGIC val pipeline = new Pipeline()
# MAGIC   .setStages(stages)
# MAGIC 
# MAGIC val pipelineModel = pipeline.fit(trainDF)
# MAGIC val predDF = pipelineModel.transform(testDF)
# MAGIC predDF.select("features", "price", "prediction").show(5)

# COMMAND ----------

# MAGIC %scala
# MAGIC // Option 2: RFormula
# MAGIC import org.apache.spark.ml.Pipeline
# MAGIC 
# MAGIC val pipeline = new Pipeline().setStages(Array(rFormula, lr))
# MAGIC 
# MAGIC val pipelineModel = pipeline.fit(trainDF)
# MAGIC val predDF = pipelineModel.transform(testDF)
# MAGIC predDF.select("features", "price", "prediction").show(5)

# COMMAND ----------

# MAGIC %python
# MAGIC # Option 1: StringIndexer + OHE + VectorAssembler
# MAGIC from pyspark.ml import Pipeline
# MAGIC 
# MAGIC stages = [stringIndexer, oheEncoder, vecAssembler, lr]
# MAGIC pipeline = Pipeline(stages=stages)
# MAGIC 
# MAGIC pipelineModel = pipeline.fit(trainDF)
# MAGIC predDF = pipelineModel.transform(testDF)
# MAGIC predDF.select("features", "price", "prediction").show(5)

# COMMAND ----------

# MAGIC %python
# MAGIC # Option 2: RFormula
# MAGIC from pyspark.ml import Pipeline
# MAGIC 
# MAGIC pipeline = Pipeline(stages = [rFormula, lr])
# MAGIC 
# MAGIC pipelineModel = pipeline.fit(trainDF)
# MAGIC predDF = pipelineModel.transform(testDF)
# MAGIC predDF.select("features", "price", "prediction").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate Model: RMSE

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.ml.evaluation.RegressionEvaluator
# MAGIC 
# MAGIC val regressionEvaluator = new RegressionEvaluator()
# MAGIC   .setPredictionCol("prediction")
# MAGIC   .setLabelCol("price")
# MAGIC   .setMetricName("rmse")
# MAGIC 
# MAGIC val rmse = regressionEvaluator.evaluate(predDF)
# MAGIC println(f"RMSE is $rmse%1.2f")

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.ml.evaluation import RegressionEvaluator
# MAGIC 
# MAGIC regressionEvaluator = RegressionEvaluator(predictionCol="prediction", labelCol="price", metricName="rmse")
# MAGIC 
# MAGIC rmse = round(regressionEvaluator.evaluate(predDF), 2)
# MAGIC print(f"RMSE is {rmse}")

# COMMAND ----------

# MAGIC %md ## R2
# MAGIC 
# MAGIC ![](https://files.training.databricks.com/images/r2d2.jpg) How is our R2 doing? 

# COMMAND ----------

# MAGIC %scala
# MAGIC val r2 = regressionEvaluator.setMetricName("r2").evaluate(predDF)
# MAGIC println(f"R2 is $r2%1.2f")

# COMMAND ----------

# MAGIC %python
# MAGIC r2 = round(regressionEvaluator.setMetricName("r2").evaluate(predDF), 2)
# MAGIC print(f"R2 is {r2}")

# COMMAND ----------

# MAGIC %scala 
# MAGIC 
# MAGIC val pipelinePath = "/tmp/sf-airbnb/lr-pipeline-model"
# MAGIC pipelineModel.write.overwrite().save(pipelinePath)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC pipelinePath = "/tmp/sf-airbnb/lr-pipeline-model"
# MAGIC pipelineModel.write().overwrite().save(pipelinePath)

# COMMAND ----------

# MAGIC %md ## Loading models
# MAGIC 
# MAGIC When you load in models, you need to know the type of model you are loading back in (was it a linear regression or logistic regression model?).
# MAGIC 
# MAGIC For this reason, we recommend you always put your transformers/estimators into a Pipeline, so you can always load the generic PipelineModel back in.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.ml.PipelineModel
# MAGIC 
# MAGIC val savedPipelineModel = PipelineModel.load(pipelinePath)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.ml import PipelineModel
# MAGIC 
# MAGIC savedPipelineModel = PipelineModel.load(pipelinePath)

# COMMAND ----------

# MAGIC %md ## Distributed Setting
# MAGIC 
# MAGIC If you are interested in learning how linear regression is implemented in the distributed setting and bottlenecks, check out these lecture slides:
# MAGIC * [distributed-linear-regression-1](https://files.training.databricks.com/static/docs/distributed-linear-regression-1.pdf)
# MAGIC * [distributed-linear-regression-2](https://files.training.databricks.com/static/docs/distributed-linear-regression-2.pdf)