# Databricks notebook source
# MAGIC %md # Joblib
# MAGIC 
# MAGIC This notebook uses [Joblib](https://github.com/joblib/joblib) to parallelize the evaluation of sklearn models. 
# MAGIC 
# MAGIC **NOTE**: You will need to install `joblibspark` on your cluster for the code below.

# COMMAND ----------

from sklearn.utils import parallel_backend
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV
import pandas as pd
from joblibspark import register_spark

register_spark() # register spark backend

df = pd.read_csv("/dbfs/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-numeric.csv").drop(["zipcode"], axis=1)
X_train, X_test, y_train, y_test = train_test_split(df.drop(["price"], axis=1), df[["price"]].values.ravel(), random_state=42)

rf = RandomForestRegressor(random_state=42)
param_grid = {"max_depth": [2, 5, 10], "n_estimators": [20, 50, 100]}
gscv = GridSearchCV(rf, param_grid, cv=3)

with parallel_backend("spark", n_jobs=3):
  gscv.fit(X_train, y_train)
  
# Uses R2 to score the models
print(gscv.cv_results_)
print(gscv.best_estimator_)