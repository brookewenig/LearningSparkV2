# Databricks notebook source
# MAGIC %md
# MAGIC # Data Cleansing with Airbnb
# MAGIC 
# MAGIC We're going to start by doing some exploratory data analysis & cleansing. We will be using the SF Airbnb rental dataset from [Inside Airbnb](http://insideairbnb.com/get-the-data.html).
# MAGIC 
# MAGIC <img src="http://insideairbnb.com/images/insideairbnb_graphic_site_1200px.png" style="width:800px"/>

# COMMAND ----------

# MAGIC %md
# MAGIC Let's load the SF Airbnb dataset (comment out each of the options if you want to see what they do).

# COMMAND ----------

# MAGIC %scala
# MAGIC val filePath = "/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb.csv"
# MAGIC 
# MAGIC val rawDF = spark.read
# MAGIC   .option("header", "true")
# MAGIC   .option("multiLine", "true")
# MAGIC   .option("inferSchema", "true")
# MAGIC   .option("escape", "\"")
# MAGIC   .csv(filePath)
# MAGIC 
# MAGIC display(rawDF)

# COMMAND ----------

# MAGIC %python
# MAGIC filePath = "/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb.csv"
# MAGIC 
# MAGIC rawDF = spark.read.csv(filePath, header="true", inferSchema="true", multiLine="true", escape='"')
# MAGIC 
# MAGIC display(rawDF)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC rawDF.columns

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC rawDF.columns

# COMMAND ----------

# MAGIC %md
# MAGIC For the sake of simplicity, only keep certain columns from this dataset. We will talk about feature selection later.

# COMMAND ----------

# MAGIC %scala 
# MAGIC 
# MAGIC val baseDF = rawDF.select(
# MAGIC   "host_is_superhost",
# MAGIC   "cancellation_policy",
# MAGIC   "instant_bookable",
# MAGIC   "host_total_listings_count",
# MAGIC   "neighbourhood_cleansed",
# MAGIC   "latitude",
# MAGIC   "longitude",
# MAGIC   "property_type",
# MAGIC   "room_type",
# MAGIC   "accommodates",
# MAGIC   "bathrooms",
# MAGIC   "bedrooms",
# MAGIC   "beds",
# MAGIC   "bed_type",
# MAGIC   "minimum_nights",
# MAGIC   "number_of_reviews",
# MAGIC   "review_scores_rating",
# MAGIC   "review_scores_accuracy",
# MAGIC   "review_scores_cleanliness",
# MAGIC   "review_scores_checkin",
# MAGIC   "review_scores_communication",
# MAGIC   "review_scores_location",
# MAGIC   "review_scores_value",
# MAGIC   "price")
# MAGIC 
# MAGIC baseDF.cache().count
# MAGIC display(baseDF)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC columnsToKeep = [
# MAGIC   "host_is_superhost",
# MAGIC   "cancellation_policy",
# MAGIC   "instant_bookable",
# MAGIC   "host_total_listings_count",
# MAGIC   "neighbourhood_cleansed",
# MAGIC   "latitude",
# MAGIC   "longitude",
# MAGIC   "property_type",
# MAGIC   "room_type",
# MAGIC   "accommodates",
# MAGIC   "bathrooms",
# MAGIC   "bedrooms",
# MAGIC   "beds",
# MAGIC   "bed_type",
# MAGIC   "minimum_nights",
# MAGIC   "number_of_reviews",
# MAGIC   "review_scores_rating",
# MAGIC   "review_scores_accuracy",
# MAGIC   "review_scores_cleanliness",
# MAGIC   "review_scores_checkin",
# MAGIC   "review_scores_communication",
# MAGIC   "review_scores_location",
# MAGIC   "review_scores_value",
# MAGIC   "price"]
# MAGIC 
# MAGIC baseDF = rawDF.select(columnsToKeep)
# MAGIC baseDF.cache().count()
# MAGIC display(baseDF)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Fixing Data Types
# MAGIC 
# MAGIC Take a look at the schema above. You'll notice that the `price` field got picked up as string. For our task, we need it to be a numeric (double type) field. 
# MAGIC 
# MAGIC Let's fix that.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.functions.translate
# MAGIC 
# MAGIC val fixedPriceDF = baseDF.withColumn("price", translate($"price", "$,", "").cast("double"))
# MAGIC 
# MAGIC display(fixedPriceDF)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql.functions import col, translate
# MAGIC 
# MAGIC fixedPriceDF = baseDF.withColumn("price", translate(col("price"), "$,", "").cast("double"))
# MAGIC 
# MAGIC display(fixedPriceDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary statistics
# MAGIC 
# MAGIC Two options:
# MAGIC * describe
# MAGIC * summary (describe + IQR)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC display(fixedPriceDF.describe())

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC display(fixedPriceDF.describe())

# COMMAND ----------

# MAGIC %scala 
# MAGIC 
# MAGIC display(fixedPriceDF.summary())

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC display(fixedPriceDF.summary())

# COMMAND ----------

# MAGIC %md ## Nulls
# MAGIC 
# MAGIC There are a lot of different ways to handle null values. Sometimes, null can actually be a key indicator of the thing you are trying to predict (e.g. if you don't fill in certain portions of a form, probability of it getting approved decreases).
# MAGIC 
# MAGIC Some ways to handle nulls:
# MAGIC * Drop any records that contain nulls
# MAGIC * Numeric:
# MAGIC   * Replace them with mean/median/zero/etc.
# MAGIC * Categorical:
# MAGIC   * Replace them with the mode
# MAGIC   * Create a special category for null
# MAGIC * Use techniques like ALS which are designed to impute missing values
# MAGIC   
# MAGIC **If you do ANY imputation techniques for categorical/numerical features, you MUST include an additional field specifying that field was imputed (think about why this is necessary)**

# COMMAND ----------

# MAGIC %md There are a few nulls in the categorical feature `host_is_superhost`. Let's get rid of those rows where any of these columns is null.
# MAGIC 
# MAGIC SparkML's Imputer (will cover below) does not support imputation for categorical features, so this is the simplest approach for the time being.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val noNullsDF = fixedPriceDF.na.drop(cols = Seq("host_is_superhost"))

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC noNullsDF = fixedPriceDF.na.drop(subset=["host_is_superhost"])

# COMMAND ----------

# MAGIC %md ## Impute: Cast to Double
# MAGIC 
# MAGIC SparkML's `Imputer` requires all fields be of type double [Python](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.Imputer)/[Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.feature.Imputer). Let's cast all integer fields to double.

# COMMAND ----------

# MAGIC %scala 
# MAGIC 
# MAGIC import org.apache.spark.sql.functions.col
# MAGIC import org.apache.spark.sql.types.IntegerType
# MAGIC 
# MAGIC val integerColumns = for (x <- baseDF.schema.fields if (x.dataType == IntegerType)) yield x.name  
# MAGIC var doublesDF = noNullsDF
# MAGIC 
# MAGIC for (c <- integerColumns)
# MAGIC   doublesDF = doublesDF.withColumn(c, col(c).cast("double"))
# MAGIC 
# MAGIC val columns = integerColumns.mkString("\n - ")
# MAGIC println(s"Columns converted from Integer to Double:\n - $columns \n")
# MAGIC println("*-"*80)

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

integerColumns = [x.name for x in baseDF.schema.fields if x.dataType == IntegerType()]
doublesDF = noNullsDF

for c in integerColumns:
  doublesDF = doublesDF.withColumn(c, col(c).cast("double"))

columns = "\n - ".join(integerColumns)
print(f"Columns converted from Integer to Double:\n - {columns}")

# COMMAND ----------

# MAGIC %md Add in dummy variable if we will impute any value.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.functions.when
# MAGIC 
# MAGIC val imputeCols = Array(
# MAGIC   "bedrooms",
# MAGIC   "bathrooms",
# MAGIC   "beds", 
# MAGIC   "review_scores_rating",
# MAGIC   "review_scores_accuracy",
# MAGIC   "review_scores_cleanliness",
# MAGIC   "review_scores_checkin",
# MAGIC   "review_scores_communication",
# MAGIC   "review_scores_location",
# MAGIC   "review_scores_value"
# MAGIC )
# MAGIC 
# MAGIC for (c <- imputeCols)
# MAGIC   doublesDF = doublesDF.withColumn(c + "_na", when(col(c).isNull, 1.0).otherwise(0.0))

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql.functions import when
# MAGIC 
# MAGIC imputeCols = [
# MAGIC   "bedrooms",
# MAGIC   "bathrooms",
# MAGIC   "beds", 
# MAGIC   "review_scores_rating",
# MAGIC   "review_scores_accuracy",
# MAGIC   "review_scores_cleanliness",
# MAGIC   "review_scores_checkin",
# MAGIC   "review_scores_communication",
# MAGIC   "review_scores_location",
# MAGIC   "review_scores_value"
# MAGIC ]
# MAGIC 
# MAGIC for c in imputeCols:
# MAGIC   doublesDF = doublesDF.withColumn(c + "_na", when(col(c).isNull(), 1.0).otherwise(0.0))

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC display(doublesDF.describe())

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC display(doublesDF.describe())

# COMMAND ----------

# MAGIC %scala 
# MAGIC 
# MAGIC import org.apache.spark.ml.feature.Imputer
# MAGIC 
# MAGIC val imputer = new Imputer()
# MAGIC   .setStrategy("median")
# MAGIC   .setInputCols(imputeCols)
# MAGIC   .setOutputCols(imputeCols)
# MAGIC 
# MAGIC val imputedDF = imputer.fit(doublesDF).transform(doublesDF)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.ml.feature import Imputer
# MAGIC 
# MAGIC imputer = Imputer(strategy="median", inputCols=imputeCols, outputCols=imputeCols)
# MAGIC 
# MAGIC imputedDF = imputer.fit(doublesDF).transform(doublesDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Getting rid of extreme values
# MAGIC 
# MAGIC Let's take a look at the *min* and *max* values of the `price` column:

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC display(imputedDF.select("price").describe())

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC display(imputedDF.select("price").describe())

# COMMAND ----------

# MAGIC %md There are some super-expensive listings. But that's the Data Scientist's job to decide what to do with them. We can certainly filter the "free" Airbnbs though.
# MAGIC 
# MAGIC Let's see first how many listings we can find where the *price* is zero.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC imputedDF.filter($"price" === 0).count

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC imputedDF.filter(col("price") == 0).count()

# COMMAND ----------

# MAGIC %md
# MAGIC Now only keep rows with a strictly positive *price*.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val posPricesDF = imputedDF.filter($"price" > 0)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC posPricesDF = imputedDF.filter(col("price") > 0)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a look at the *min* and *max* values of the *minimum_nights* column:

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC display(posPricesDF.select("minimum_nights").describe())

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC display(posPricesDF.select("minimum_nights").describe())

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC display(posPricesDF
# MAGIC   .groupBy("minimum_nights").count()
# MAGIC   .orderBy($"count".desc, $"minimum_nights")
# MAGIC )

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC display(posPricesDF
# MAGIC   .groupBy("minimum_nights").count()
# MAGIC   .orderBy(col("count").desc(), col("minimum_nights"))
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC A minimum stay of one year seems to be a reasonable limit here. Let's filter out those records where the *minimum_nights* is greater then 365:

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val cleanDF = posPricesDF.filter($"minimum_nights" <= 365)
# MAGIC 
# MAGIC display(cleanDF)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC cleanDF = posPricesDF.filter(col("minimum_nights") <= 365)
# MAGIC 
# MAGIC display(cleanDF)

# COMMAND ----------

# MAGIC %md
# MAGIC OK, our data is cleansed now. Let's save this DataFrame to a file so that we can start building models with it.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val outputPath = "/tmp/sf-airbnb/sf-airbnb-clean.parquet"
# MAGIC 
# MAGIC cleanDF.write.mode("overwrite").parquet(outputPath)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC outputPath = "/tmp/sf-airbnb/sf-airbnb-clean.parquet"
# MAGIC 
# MAGIC cleanDF.write.mode("overwrite").parquet(outputPath)