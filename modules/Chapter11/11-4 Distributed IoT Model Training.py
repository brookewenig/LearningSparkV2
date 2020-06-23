# Databricks notebook source
# MAGIC %md ## Distributed IoT Model Training with the Pandas Function API
# MAGIC 
# MAGIC This notebook demonstrates how to scale single node machine learning solutions with the pandas function API.

# COMMAND ----------

# MAGIC %md Create dummy data with:
# MAGIC - `device_id`: 10 different devices
# MAGIC - `record_id`: 10k unique records
# MAGIC - `feature_1`: a feature for model training
# MAGIC - `feature_2`: a feature for model training
# MAGIC - `feature_3`: a feature for model training
# MAGIC - `label`: the variable we're trying to predict

# COMMAND ----------

import pyspark.sql.functions as f

df = (spark.range(1000*1000)
  .select(f.col("id").alias("record_id"), (f.col("id")%10).alias("device_id"))
  .withColumn("feature_1", f.rand() * 1)
  .withColumn("feature_2", f.rand() * 2)
  .withColumn("feature_3", f.rand() * 3)
  .withColumn("label", (f.col("feature_1") + f.col("feature_2") + f.col("feature_3")) + f.rand())
)

display(df)

# COMMAND ----------

# MAGIC %md Define the return schema

# COMMAND ----------

import pyspark.sql.types as t

trainReturnSchema = t.StructType([
  t.StructField('device_id', t.IntegerType()), # unique device ID
  t.StructField('n_used', t.IntegerType()),    # number of records used in training
  t.StructField('model_path', t.StringType()), # path to the model for a given device
  t.StructField('mse', t.FloatType())          # metric for model performance
])

# COMMAND ----------

# MAGIC %md Define a function that takes all the data for a given device, train a model, saves it as a nested run, and returns a DataFrame with the above schema.
# MAGIC 
# MAGIC We are using MLflow to track all of these models. 

# COMMAND ----------

import mlflow
import mlflow.sklearn
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

def train_model(df_pandas: pd.DataFrame) -> pd.DataFrame:
  '''
  Trains an sklearn model on grouped instances
  '''
  # Pull metadata
  device_id = df_pandas['device_id'].iloc[0]
  n_used = df_pandas.shape[0]
  run_id = df_pandas['run_id'].iloc[0] # Pulls run ID to do a nested run
  
  # Train the model
  X = df_pandas[['feature_1', 'feature_2', 'feature_3']]
  y = df_pandas['label']
  rf = RandomForestRegressor()
  rf.fit(X, y)

  # Evaluate the model
  predictions = rf.predict(X)
  mse = mean_squared_error(y, predictions) # Note we could add a train/test split
 
  # Resume the top-level training
  with mlflow.start_run(run_id=run_id):
    # Create a nested run for the specific device
    with mlflow.start_run(run_name=str(device_id), nested=True) as run:
      mlflow.sklearn.log_model(rf, str(device_id))
      mlflow.log_metric("mse", mse)
      
      artifact_uri = f"runs:/{run.info.run_id}/{device_id}"
      # Create a return pandas DataFrame that matches the schema above
      returnDF = pd.DataFrame([[device_id, n_used, artifact_uri, mse]], 
        columns=["device_id", "n_used", "model_path", "mse"])

  return returnDF 


# COMMAND ----------

# MAGIC %md Use applyInPandas to grouped data

# COMMAND ----------

with mlflow.start_run(run_name="Training session for all devices") as run:
  run_id = run.info.run_uuid
  
  modelDirectoriesDF = (df
    .withColumn("run_id", f.lit(run_id)) # Add run_id
    .groupby("device_id")
    .applyInPandas(train_model, schema=trainReturnSchema)
  )
  
combinedDF = (df
  .join(modelDirectoriesDF, on="device_id", how="left")
)

display(combinedDF)

# COMMAND ----------

# MAGIC %md Define a function to apply the model.  *This needs only one read from DBFS per device.*

# COMMAND ----------

applyReturnSchema = t.StructType([
  t.StructField('record_id', t.IntegerType()),
  t.StructField('prediction', t.FloatType())
])

def apply_model(df_pandas: pd.DataFrame) -> pd.DataFrame:
  '''
  Applies model to data for a particular device, represented as a pandas DataFrame
  '''
  model_path = df_pandas['model_path'].iloc[0]
  
  input_columns = ['feature_1', 'feature_2', 'feature_3']
  X = df_pandas[input_columns]
  
  model = mlflow.sklearn.load_model(model_path)
  prediction = model.predict(X)
  
  returnDF = pd.DataFrame({
    "record_id": df_pandas['record_id'],
    "prediction": prediction
  })
  return returnDF

predictionDF = combinedDF.groupby("device_id").applyInPandas(apply_model, schema=applyReturnSchema)
display(predictionDF)