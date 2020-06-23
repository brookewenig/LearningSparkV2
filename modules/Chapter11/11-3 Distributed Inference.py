# Databricks notebook source
# MAGIC %md # Distributed Inference with mapInPandas

# COMMAND ----------

# MAGIC %md Train sklearn model and log with MLflow. 

# COMMAND ----------

import mlflow.sklearn
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split

with mlflow.start_run(run_name="sklearn-rf-model") as run:
  # Import the data
  df = pd.read_csv("/dbfs/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-numeric.csv").drop(["zipcode"], axis=1)
  X_train, X_test, y_train, y_test = train_test_split(df.drop(["price"], axis=1), df[["price"]].values.ravel(), random_state=42)

  # Create model, train it, and create predictions
  rf = RandomForestRegressor(n_estimators=100, max_depth=10)
  rf.fit(X_train, y_train)
  predictions = rf.predict(X_test)

  # Log model
  mlflow.sklearn.log_model(rf, "random-forest-model")

  # Log params
  mlflow.log_param("n_estimators", 100)
  mlflow.log_param("max_depth", 10)

  # Log metrics
  mlflow.log_metric("mse", mean_squared_error(y_test, predictions))
  mlflow.log_metric("mae", mean_absolute_error(y_test, predictions))  
  mlflow.log_metric("r2", r2_score(y_test, predictions))  

# COMMAND ----------

# MAGIC %md Create Spark DataFrame and apply model to Spark DataFrame.

# COMMAND ----------

sparkDF = spark.createDataFrame(X_train)

# COMMAND ----------

# MAGIC %md Apply the model in parallel with mapInPandas

# COMMAND ----------

import mlflow.sklearn
import pandas as pd

def predict(iterator):
  model_path = f"runs:/{run.info.run_uuid}/random-forest-model" # Load model
  model = mlflow.sklearn.load_model(model_path)
  for features in iterator:
    yield pd.concat([features, pd.Series(model.predict(features), name="prediction")], axis=1)
    
display(sparkDF.mapInPandas(predict, """`host_total_listings_count` DOUBLE,`neighbourhood_cleansed` BIGINT,`latitude` DOUBLE,`longitude` DOUBLE,`property_type` BIGINT,`room_type` BIGINT,`accommodates` DOUBLE,`bathrooms` DOUBLE,`bedrooms` DOUBLE,`beds` DOUBLE,`bed_type` BIGINT,`minimum_nights` DOUBLE,`number_of_reviews` DOUBLE,`review_scores_rating` DOUBLE,`review_scores_accuracy` DOUBLE,`review_scores_cleanliness` DOUBLE,`review_scores_checkin` DOUBLE,`review_scores_communication` DOUBLE,`review_scores_location` DOUBLE,`review_scores_value` DOUBLE, prediction double"""))