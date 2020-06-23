# Databricks notebook source
# MAGIC %md # Hyperparameter Tuning
# MAGIC 
# MAGIC Let's perform hyperparameter tuning on a random forest to find the best hyperparameters!

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
# MAGIC 
# MAGIC val filePath = "/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet"
# MAGIC val airbnbDF = spark.read.parquet(filePath)
# MAGIC val Array(trainDF, testDF) = airbnbDF.randomSplit(Array(.8, .2), seed=42)
# MAGIC 
# MAGIC val categoricalCols = trainDF.dtypes.filter(_._2 == "StringType").map(_._1)
# MAGIC val indexOutputCols = categoricalCols.map(_ + "Index")
# MAGIC 
# MAGIC val stringIndexer = new StringIndexer()
# MAGIC   .setInputCols(categoricalCols)
# MAGIC   .setOutputCols(indexOutputCols)
# MAGIC   .setHandleInvalid("skip")
# MAGIC 
# MAGIC val numericCols = trainDF.dtypes.filter{ case (field, dataType) => dataType == "DoubleType" && field != "price"}.map(_._1)
# MAGIC val assemblerInputs = indexOutputCols ++ numericCols
# MAGIC val vecAssembler = new VectorAssembler()
# MAGIC   .setInputCols(assemblerInputs)
# MAGIC   .setOutputCol("features")

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.ml.feature import StringIndexer, VectorAssembler
# MAGIC 
# MAGIC filePath = "/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet"
# MAGIC airbnbDF = spark.read.parquet(filePath)
# MAGIC (trainDF, testDF) = airbnbDF.randomSplit([.8, .2], seed=42)
# MAGIC 
# MAGIC categoricalCols = [field for (field, dataType) in trainDF.dtypes if dataType == "string"]
# MAGIC indexOutputCols = [x + "Index" for x in categoricalCols]
# MAGIC 
# MAGIC stringIndexer = StringIndexer(inputCols=categoricalCols, outputCols=indexOutputCols, handleInvalid="skip")
# MAGIC 
# MAGIC numericCols = [field for (field, dataType) in trainDF.dtypes if ((dataType == "double") & (field != "price"))]
# MAGIC assemblerInputs = indexOutputCols + numericCols
# MAGIC vecAssembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")

# COMMAND ----------

# MAGIC %md ## Random Forest

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.ml.regression.RandomForestRegressor
# MAGIC import org.apache.spark.ml.Pipeline
# MAGIC 
# MAGIC val rf = new RandomForestRegressor()
# MAGIC   .setLabelCol("price")
# MAGIC   .setMaxBins(40)
# MAGIC   .setSeed(42)
# MAGIC 
# MAGIC val pipeline = new Pipeline()
# MAGIC   .setStages(Array(stringIndexer, vecAssembler, rf))

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.ml.regression import RandomForestRegressor
# MAGIC from pyspark.ml import Pipeline
# MAGIC 
# MAGIC rf = RandomForestRegressor(labelCol="price", maxBins=40, seed=42)
# MAGIC pipeline = Pipeline(stages = [stringIndexer, vecAssembler, rf])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grid Search
# MAGIC 
# MAGIC There are a lot of hyperparameters we could tune, and it would take a long time to manually configure.
# MAGIC 
# MAGIC Let's use Spark's `ParamGridBuilder` to find the optimal hyperparameters in a more systematic approach [Python](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.ParamGridBuilder)/[Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.tuning.ParamGridBuilder).
# MAGIC 
# MAGIC Let's define a grid of hyperparameters to test:
# MAGIC   - maxDepth: max depth of the decision tree (Use the values `2, 4, 6`)
# MAGIC   - numTrees: number of decision trees (Use the values `10, 100`)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.ml.tuning.ParamGridBuilder
# MAGIC 
# MAGIC val paramGrid = new ParamGridBuilder()
# MAGIC   .addGrid(rf.maxDepth, Array(2, 4, 6))
# MAGIC   .addGrid(rf.numTrees, Array(10, 100))
# MAGIC   .build()

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.ml.tuning import ParamGridBuilder
# MAGIC 
# MAGIC paramGrid = (ParamGridBuilder()
# MAGIC             .addGrid(rf.maxDepth, [2, 4, 6])
# MAGIC             .addGrid(rf.numTrees, [10, 100])
# MAGIC             .build())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross Validation
# MAGIC 
# MAGIC We are also going to use 3-fold cross validation to identify the optimal maxDepth.
# MAGIC 
# MAGIC ![crossValidation](https://files.training.databricks.com/images/301/CrossValidation.png)
# MAGIC 
# MAGIC With 3-fold cross-validation, we train on 2/3 of the data, and evaluate with the remaining (held-out) 1/3. We repeat this process 3 times, so each fold gets the chance to act as the validation set. We then average the results of the three rounds.

# COMMAND ----------

# MAGIC %md
# MAGIC We pass in the `estimator` (pipeline), `evaluator`, and `estimatorParamMaps` to `CrossValidator` so that it knows:
# MAGIC - Which model to use
# MAGIC - How to evaluate the model
# MAGIC - What hyperparameters to set for the model
# MAGIC 
# MAGIC We can also set the number of folds we want to split our data into (3), as well as setting a seed so we all have the same split in the data [Python](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.CrossValidator)/[Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.tuning.CrossValidator).

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.ml.evaluation.RegressionEvaluator
# MAGIC import org.apache.spark.ml.tuning.CrossValidator
# MAGIC 
# MAGIC val evaluator = new RegressionEvaluator()
# MAGIC   .setLabelCol("price")
# MAGIC   .setPredictionCol("prediction")
# MAGIC   .setMetricName("rmse")
# MAGIC 
# MAGIC val cv = new CrossValidator()
# MAGIC  .setEstimator(pipeline)
# MAGIC  .setEvaluator(evaluator)
# MAGIC  .setEstimatorParamMaps(paramGrid)
# MAGIC  .setNumFolds(3)
# MAGIC  .setSeed(42)

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.ml.evaluation import RegressionEvaluator
# MAGIC from pyspark.ml.tuning import CrossValidator
# MAGIC 
# MAGIC evaluator = RegressionEvaluator(labelCol="price", 
# MAGIC                                 predictionCol="prediction", 
# MAGIC                                 metricName="rmse")
# MAGIC 
# MAGIC cv = CrossValidator(estimator=pipeline, 
# MAGIC                     evaluator=evaluator, 
# MAGIC                     estimatorParamMaps=paramGrid, 
# MAGIC                     numFolds=3, 
# MAGIC                     seed=42)

# COMMAND ----------

# MAGIC %md **Question**: How many models are we training right now?

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val cvModel = cv.fit(trainDF)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC cvModel = cv.fit(trainDF)

# COMMAND ----------

# MAGIC %md ## Parallelism Parameter
# MAGIC 
# MAGIC Hmmm... that took a long time to run. That's because the models were being trained sequentially rather than in parallel!
# MAGIC 
# MAGIC Spark 2.3 introduced a [parallelism](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.CrossValidator.parallelism) parameter. From the docs: `the number of threads to use when running parallel algorithms (>= 1)`.
# MAGIC 
# MAGIC Let's set this value to 4 and see if we can train any faster.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val cvModel = cv.setParallelism(4).fit(trainDF)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC cvModel = cv.setParallelism(4).fit(trainDF)

# COMMAND ----------

# MAGIC %md 
# MAGIC **Question**: Hmmm... that still took a long time to run. Should we put the pipeline in the cross validator, or the cross validator in the pipeline?
# MAGIC 
# MAGIC It depends if there are estimators or transformers in the pipeline. If you have things like StringIndexer (an estimator) in the pipeline, then you have to refit it every time if you put the entire pipeline in the cross validator.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val cv = new CrossValidator()
# MAGIC   .setEstimator(rf)
# MAGIC   .setEvaluator(evaluator)
# MAGIC   .setEstimatorParamMaps(paramGrid)
# MAGIC   .setNumFolds(3)
# MAGIC   .setParallelism(4)
# MAGIC   .setSeed(42)
# MAGIC 
# MAGIC val pipeline = new Pipeline()
# MAGIC                    .setStages(Array(stringIndexer, vecAssembler, cv))
# MAGIC 
# MAGIC val pipelineModel = pipeline.fit(trainDF)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC cv = CrossValidator(estimator=rf, 
# MAGIC                     evaluator=evaluator, 
# MAGIC                     estimatorParamMaps=paramGrid, 
# MAGIC                     numFolds=3, 
# MAGIC                     parallelism=4, 
# MAGIC                     seed=42)
# MAGIC 
# MAGIC pipeline = Pipeline(stages=[stringIndexer, vecAssembler, cv])
# MAGIC 
# MAGIC pipelineModel = pipeline.fit(trainDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a look at the model with the best hyperparameter configuration

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC cvModel.getEstimatorParamMaps.zip(cvModel.avgMetrics)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC list(zip(cvModel.getEstimatorParamMaps(), cvModel.avgMetrics))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's see how it does on the test dataset.

# COMMAND ----------

# MAGIC %scala
# MAGIC val predDF = pipelineModel.transform(testDF)
# MAGIC 
# MAGIC val regressionEvaluator = new RegressionEvaluator()
# MAGIC   .setPredictionCol("prediction")
# MAGIC   .setLabelCol("price")
# MAGIC   .setMetricName("rmse")
# MAGIC 
# MAGIC val rmse = regressionEvaluator.evaluate(predDF)
# MAGIC val r2 = regressionEvaluator.setMetricName("r2").evaluate(predDF)
# MAGIC println(s"RMSE is $rmse")
# MAGIC println(s"R2 is $r2")
# MAGIC println("*-"*80)

# COMMAND ----------

# MAGIC %python
# MAGIC predDF = pipelineModel.transform(testDF)
# MAGIC 
# MAGIC regressionEvaluator = RegressionEvaluator(predictionCol="prediction", labelCol="price", metricName="rmse")
# MAGIC 
# MAGIC rmse = regressionEvaluator.evaluate(predDF)
# MAGIC r2 = regressionEvaluator.setMetricName("r2").evaluate(predDF)
# MAGIC print(f"RMSE is {rmse}")
# MAGIC print(f"R2 is {r2}")