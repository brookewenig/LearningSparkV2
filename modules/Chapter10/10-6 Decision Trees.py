# Databricks notebook source
# MAGIC %md # Decision Trees
# MAGIC 
# MAGIC In the previous notebook, you were working with the parametric model, Linear Regression. We could do some more hyperparameter tuning with the linear regression model, but we're going to try tree based methods and see if our performance improves.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val filePath = "/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet"
# MAGIC val airbnbDF = spark.read.parquet(filePath)
# MAGIC val Array(trainDF, testDF) = airbnbDF.randomSplit(Array(.8, .2), seed=42)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC filePath = "/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet"
# MAGIC airbnbDF = spark.read.parquet(filePath)
# MAGIC trainDF, testDF = airbnbDF.randomSplit([.8, .2], seed=42)

# COMMAND ----------

# MAGIC %md ## How to Handle Categorical Features?
# MAGIC 
# MAGIC We saw in the previous notebook that we can use StringIndexer/OneHotEncoderEstimator/VectorAssembler or RFormula.
# MAGIC 
# MAGIC **However, for decision trees, and in particular, random forests, we should not OHE our variables.**
# MAGIC 
# MAGIC Why is that? Well, how the splits are made is different (you can see this when you visualize the tree) and the feature importance scores are not correct.
# MAGIC 
# MAGIC For random forests (which we will discuss shortly), the result can change dramatically. So instead of using RFormula, we are going to use just StringIndexer/VectorAssembler.

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.ml.feature.StringIndexer
# MAGIC 
# MAGIC val categoricalCols = trainDF.dtypes.filter(_._2 == "StringType").map(_._1)
# MAGIC val indexOutputCols = categoricalCols.map(_ + "Index")
# MAGIC 
# MAGIC val stringIndexer = new StringIndexer()
# MAGIC   .setInputCols(categoricalCols)
# MAGIC   .setOutputCols(indexOutputCols)
# MAGIC   .setHandleInvalid("skip")

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.ml.feature import StringIndexer
# MAGIC 
# MAGIC categoricalCols = [field for (field, dataType) in trainDF.dtypes if dataType == "string"]
# MAGIC indexOutputCols = [x + "Index" for x in categoricalCols]
# MAGIC 
# MAGIC stringIndexer = StringIndexer(inputCols=categoricalCols, outputCols=indexOutputCols, handleInvalid="skip")

# COMMAND ----------

# MAGIC %md ## VectorAssembler
# MAGIC 
# MAGIC Let's use the VectorAssembler to combine all of our categorical and numeric inputs [Python](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.VectorAssembler)/[Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.feature.VectorAssembler).

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.ml.feature.VectorAssembler
# MAGIC 
# MAGIC // Filter for just numeric columns (and exclude price, our label)
# MAGIC val numericCols = trainDF.dtypes.filter{ case (field, dataType) => 
# MAGIC   dataType == "DoubleType" && field != "price"}.map(_._1)
# MAGIC // Combine output of StringIndexer defined above and numeric columns
# MAGIC val assemblerInputs = indexOutputCols ++ numericCols
# MAGIC val vecAssembler = new VectorAssembler()
# MAGIC   .setInputCols(assemblerInputs)
# MAGIC   .setOutputCol("features")

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.ml.feature import VectorAssembler
# MAGIC 
# MAGIC # Filter for just numeric columns (and exclude price, our label)
# MAGIC numericCols = [field for (field, dataType) in trainDF.dtypes 
# MAGIC                if ((dataType == "double") & (field != "price"))]
# MAGIC # Combine output of StringIndexer defined above and numeric columns
# MAGIC assemblerInputs = indexOutputCols + numericCols
# MAGIC vecAssembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")

# COMMAND ----------

# MAGIC %md ## Decision Tree
# MAGIC 
# MAGIC Now let's build a `DecisionTreeRegressor` with the default hyperparameters [Python](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.regression.DecisionTreeRegressor)/[Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.regression.DecisionTreeRegressor).

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.ml.regression.DecisionTreeRegressor
# MAGIC 
# MAGIC val dt = new DecisionTreeRegressor().setLabelCol("price")

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.ml.regression import DecisionTreeRegressor
# MAGIC 
# MAGIC dt = DecisionTreeRegressor(labelCol="price")

# COMMAND ----------

# MAGIC %md ## Fit Pipeline

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.ml.Pipeline
# MAGIC 
# MAGIC // Combine stages into pipeline
# MAGIC val stages = Array(stringIndexer, vecAssembler, dt)
# MAGIC val pipeline = new Pipeline()
# MAGIC   .setStages(stages)
# MAGIC 
# MAGIC // Uncomment to perform fit
# MAGIC // val pipelineModel = pipeline.fit(trainDF) 

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.ml import Pipeline
# MAGIC 
# MAGIC # Combine stages into pipeline
# MAGIC stages = [stringIndexer, vecAssembler, dt]
# MAGIC pipeline = Pipeline(stages=stages)
# MAGIC 
# MAGIC # Uncomment to perform fit
# MAGIC # pipelineModel = pipeline.fit(trainDF)

# COMMAND ----------

# MAGIC %md ## maxBins
# MAGIC 
# MAGIC What is this parameter [maxBins](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.regression.DecisionTreeRegressor.maxBins)? Let's take a look at the PLANET implementation of distributed decision trees (which Spark uses) and compare it to this paper called [Yggdrasil](https://cs.stanford.edu/~matei/papers/2016/nips_yggdrasil.pdf) by Matei Zaharia and others. This will help explain the `maxBins` parameter.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/DistDecisionTrees.png" height=500px>

# COMMAND ----------

# MAGIC %md In Spark, data is partitioned by row. So when it needs to make a split, each worker has to compute summary statistics for every feature for  each split point. Then these summary statistics have to be aggregated (via tree reduce) for a split to be made. 
# MAGIC 
# MAGIC Think about it: What if worker 1 had the value `32` but none of the others had it. How could you communicate how good of a split that would be? So, Spark has a maxBins parameter for discretizing continuous variables into buckets, but the number of buckets has to be as large as the number of categorical variables.

# COMMAND ----------

# MAGIC %md Let's go ahead and increase maxBins to `40`.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC dt.setMaxBins(40)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC dt.setMaxBins(40)

# COMMAND ----------

# MAGIC %md Take two.

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.ml.Pipeline
# MAGIC 
# MAGIC val stages = Array(stringIndexer, vecAssembler, dt)
# MAGIC val pipeline = new Pipeline()
# MAGIC   .setStages(stages)
# MAGIC 
# MAGIC val pipelineModel = pipeline.fit(trainDF) 

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC pipelineModel = pipeline.fit(trainDF)

# COMMAND ----------

# MAGIC %md ## Visualize the Decision Tree

# COMMAND ----------

# MAGIC %scala
# MAGIC val dtModel = pipelineModel.stages.last
# MAGIC   .asInstanceOf[org.apache.spark.ml.regression.DecisionTreeRegressionModel]
# MAGIC println(dtModel.toDebugString)

# COMMAND ----------

dtModel = pipelineModel.stages[-1]
print(dtModel.toDebugString)

# COMMAND ----------

# MAGIC %md ## Feature Importance
# MAGIC 
# MAGIC Let's go ahead and get the fitted decision tree model, and look at the feature importance scores.

# COMMAND ----------

# MAGIC %scala
# MAGIC val dtModel = pipelineModel.stages.last.asInstanceOf[org.apache.spark.ml.regression.DecisionTreeRegressionModel]
# MAGIC dtModel.featureImportances

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC dtModel = pipelineModel.stages[-1]
# MAGIC dtModel.featureImportances

# COMMAND ----------

# MAGIC %md ## Interpreting Feature Importance
# MAGIC 
# MAGIC Hmmm... it's a little hard to know what feature 4 vs 11 is. Given that the feature importance scores are "small data", let's use Pandas to help us recover the original column names.

# COMMAND ----------

# MAGIC %scala 
# MAGIC val dtModel = pipelineModel.stages.last.asInstanceOf[org.apache.spark.ml.regression.DecisionTreeRegressionModel]
# MAGIC val featureImp = vecAssembler.getInputCols.zip(dtModel.featureImportances.toArray)
# MAGIC val columns = Array("feature", "Importance")
# MAGIC val featureImpDF = spark.createDataFrame(featureImp).toDF(columns: _*)
# MAGIC 
# MAGIC featureImpDF.orderBy($"Importance".desc).show()

# COMMAND ----------

# MAGIC %python
# MAGIC import pandas as pd
# MAGIC dtModel = pipelineModel.stages[-1]
# MAGIC featureImp = pd.DataFrame(
# MAGIC   list(zip(vecAssembler.getInputCols(), dtModel.featureImportances)),
# MAGIC   columns=["feature", "importance"])
# MAGIC featureImp.sort_values(by="importance", ascending=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply model to test set

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._
# MAGIC 
# MAGIC val predDF = pipelineModel.transform(testDF)
# MAGIC 
# MAGIC display(predDF.select("features", "price", "prediction").orderBy(desc("price")))

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC predDF = pipelineModel.transform(testDF)
# MAGIC 
# MAGIC display(predDF.select("features", "price", "prediction").orderBy("price", ascending=False))

# COMMAND ----------

# MAGIC %md ## Pitfall
# MAGIC 
# MAGIC What if we get a massive Airbnb rental? It was 20 bedrooms and 20 bathrooms. What will a decision tree predict?
# MAGIC 
# MAGIC It turns out decision trees cannot predict any values larger than they were trained on. The max value in our training set was $10,000, so we can't predict any values larger than that (or technically any values larger than the )

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.ml.evaluation.RegressionEvaluator
# MAGIC 
# MAGIC val regressionEvaluator = new RegressionEvaluator()
# MAGIC                               .setPredictionCol("prediction")
# MAGIC                               .setLabelCol("price")
# MAGIC                               .setMetricName("rmse")
# MAGIC 
# MAGIC val rmse = regressionEvaluator.evaluate(predDF)
# MAGIC val r2 = regressionEvaluator.setMetricName("r2").evaluate(predDF)
# MAGIC println(s"RMSE is $rmse")
# MAGIC println(s"R2 is $r2")
# MAGIC println("*-"*80)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.ml.evaluation import RegressionEvaluator
# MAGIC 
# MAGIC regressionEvaluator = RegressionEvaluator(predictionCol="prediction", 
# MAGIC                                           labelCol="price", 
# MAGIC                                           metricName="rmse")
# MAGIC 
# MAGIC rmse = regressionEvaluator.evaluate(predDF)
# MAGIC r2 = regressionEvaluator.setMetricName("r2").evaluate(predDF)
# MAGIC print(f"RMSE is {rmse}")
# MAGIC print(f"R2 is {r2}")

# COMMAND ----------

# MAGIC %md ## Uh oh!
# MAGIC 
# MAGIC This model is worse than the linear regression model.
# MAGIC 
# MAGIC In the next few notebooks, let's look at hyperparameter tuning and ensemble models to improve upon the performance of our singular decision tree.