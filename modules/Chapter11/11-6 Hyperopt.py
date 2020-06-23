# Databricks notebook source
# MAGIC %md 
# MAGIC # Hyperopt
# MAGIC 
# MAGIC The [Hyperopt library](https://github.com/hyperopt/hyperopt) allows for parallel hyperparameter tuning using either random search or Tree of Parzen Estimators (TPE). With MLflow, we can record the hyperparameters and corresponding metrics for each hyperparameter combination. You can read more on [SparkTrials w/ Hyperopt](https://github.com/hyperopt/hyperopt/blob/master/docs/templates/scaleout/spark.md).
# MAGIC 
# MAGIC For this example, we will parallelize the hyperparameter search for a tf.keras model with the california housing dataset.

# COMMAND ----------

from sklearn.datasets.california_housing import fetch_california_housing
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

cal_housing = fetch_california_housing()

# split 80/20 train-test
X_train, X_test, y_train, y_test = train_test_split(cal_housing.data,
                                                    cal_housing.target,
                                                    test_size=0.2,
                                                    random_state=1)
# Feature-wise standardization
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Keras Model
# MAGIC 
# MAGIC We will define our NN in Keras and use the hyperparameters given by HyperOpt.
# MAGIC 
# MAGIC :SIDENOTE: We need to import `tensorflow` within the function due to a pickling issue.  <a href="https://docs.databricks.com/applications/deep-learning/single-node-training/tensorflow.html#tensorflow-2-known-issues" target="_blank">See known issues here.</a>

# COMMAND ----------

import tensorflow as tf
from tensorflow.keras.layers import Dense
from tensorflow.keras.models import Sequential
import mlflow
import mlflow.keras
tf.random.set_seed(42)

def create_model(hpo):
  model = Sequential()
  model.add(Dense(int(hpo["dense_l1"]), input_dim=8, activation="relu"))
  model.add(Dense(int(hpo["dense_l2"]), activation="relu"))
  model.add(Dense(1, activation="linear"))
  return model

# COMMAND ----------

from hyperopt import fmin, hp, tpe, STATUS_OK, SparkTrials

def runNN(hpo):
  # Need to include the TF import due to serialization issues
  import tensorflow as tf
  
  model = create_model(hpo)

  # Select Optimizer
  optimizer_call = getattr(tf.keras.optimizers, hpo["optimizer"])
  optimizer = optimizer_call(learning_rate=hpo["learning_rate"])

  # Compile model
  model.compile(loss="mse",
                optimizer=optimizer,
                metrics=["mse"])

  history = model.fit(X_train, y_train, validation_split=.2, epochs=10, verbose=2)

  # Evaluate our model
  score = model.evaluate(X_test, y_test, verbose=0)
  obj_metric = score[0]  
  return {"loss": obj_metric, "status": STATUS_OK}

# COMMAND ----------

# MAGIC %md ### Setup hyperparameter space and training
# MAGIC 
# MAGIC We need to create a search space for HyperOpt and set up SparkTrials to allow HyperOpt to run in parallel using Spark worker nodes. We can also start a MLflow run to automatically track the results of HyperOpt's tuning trials.

# COMMAND ----------

space = {
  "dense_l1": hp.quniform("dense_l1", 10, 30, 1),
  "dense_l2": hp.quniform("dense_l2", 10, 30, 1),
  "learning_rate": hp.loguniform("learning_rate", -5, 0),
  "optimizer": hp.choice("optimizer", ["Adadelta", "Adam"])
 }

spark_trials = SparkTrials(parallelism=4)

with mlflow.start_run():
  best_hyperparam = fmin(fn=runNN, 
                         space=space, 
                         algo=tpe.suggest, 
                         max_evals=16, 
                         trials=spark_trials)

best_hyperparam

# COMMAND ----------

# MAGIC %md
# MAGIC To view the MLflow experiment associated with the notebook, click the Runs icon in the notebook context bar on the upper right. There, you can view all runs. You can also bring up the full MLflow UI by clicking the button on the upper right that reads View Experiment UI when you hover over it.
# MAGIC 
# MAGIC To understand the effect of tuning a hyperparameter:
# MAGIC 
# MAGIC 0. Select the resulting runs and click Compare.
# MAGIC 0. In the Scatter Plot, select a hyperparameter for the X-axis and loss for the Y-axis.