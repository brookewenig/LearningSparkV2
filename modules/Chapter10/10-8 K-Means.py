# Databricks notebook source
# MAGIC %md # Distributed K-Means
# MAGIC 
# MAGIC In this notebook, we are going to use K-Means to cluster our data. We will be using the Iris dataset, which has labels (the type of iris), but we will only use the labels to evaluate the model, not to train it. 
# MAGIC 
# MAGIC At the end, we will look at how it is implemented in the distributed setting.

# COMMAND ----------

# ALL_NOTEBOOKS
from sklearn.datasets import load_iris
import pandas as pd

# Load in a Dataset from sklearn and convert to a Spark DataFrame
iris = load_iris()
iris_pd = pd.concat([pd.DataFrame(iris.data, columns=iris.feature_names), pd.DataFrame(iris.target, columns=["label"])], axis=1)
irisDF = spark.createDataFrame(iris_pd)
display(irisDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Notice that we have four values as "features".  We'll reduce those down to two values (for visualization purposes) and convert them to a `DenseVector`.  To do that we'll use the `VectorAssembler`. 

# COMMAND ----------

# ALL_NOTEBOOKS
from pyspark.ml.feature import VectorAssembler
vecAssembler = VectorAssembler(inputCols=["sepal length (cm)", "sepal width (cm)"], outputCol="features")
irisTwoFeaturesDF = vecAssembler.transform(irisDF)
display(irisTwoFeaturesDF)

# COMMAND ----------

# ALL_NOTEBOOKS
from pyspark.ml.clustering import KMeans

kmeans = KMeans(k=3, seed=221, maxIter=20)

#  Call fit on the estimator and pass in irisTwoFeaturesDF
model = kmeans.fit(irisTwoFeaturesDF)

# Obtain the clusterCenters from the KMeansModel
centers = model.clusterCenters()

# Use the model to transform the DataFrame by adding cluster predictions
transformedDF = model.transform(irisTwoFeaturesDF)

print(centers)

# COMMAND ----------

# ALL_NOTEBOOKS
modelCenters = []
iterations = [0, 2, 4, 7, 10, 20]
for i in iterations:
    kmeans = KMeans(k=3, seed=221, maxIter=i)
    model = kmeans.fit(irisTwoFeaturesDF)
    modelCenters.append(model.clusterCenters())   

# COMMAND ----------

# MAGIC %python
# MAGIC # ALL_NOTEBOOKS
# MAGIC 
# MAGIC print("modelCenters:")
# MAGIC for centroids in modelCenters:
# MAGIC   print(centroids)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's visualize how our clustering performed against the true labels of our data.
# MAGIC 
# MAGIC Remember: K-means doesn't use the true labels when training, but we can use them to evaluate. 
# MAGIC 
# MAGIC Here, the star marks the cluster center.

# COMMAND ----------

# MAGIC %python
# MAGIC # ALL_NOTEBOOKS
# MAGIC 
# MAGIC import matplotlib.pyplot as plt
# MAGIC import matplotlib.cm as cm
# MAGIC import numpy as np
# MAGIC 
# MAGIC def prepareSubplot(xticks, yticks, figsize=(10.5, 6), hideLabels=False, gridColor='#999999', 
# MAGIC                 gridWidth=1.0, subplots=(1, 1)):
# MAGIC     """Template for generating the plot layout."""
# MAGIC     plt.close()
# MAGIC     fig, axList = plt.subplots(subplots[0], subplots[1], figsize=figsize, facecolor='white', 
# MAGIC                                edgecolor='white')
# MAGIC     if not isinstance(axList, np.ndarray):
# MAGIC         axList = np.array([axList])
# MAGIC     
# MAGIC     for ax in axList.flatten():
# MAGIC         ax.axes.tick_params(labelcolor='#999999', labelsize='10')
# MAGIC         for axis, ticks in [(ax.get_xaxis(), xticks), (ax.get_yaxis(), yticks)]:
# MAGIC             axis.set_ticks_position('none')
# MAGIC             axis.set_ticks(ticks)
# MAGIC             axis.label.set_color('#999999')
# MAGIC             if hideLabels: axis.set_ticklabels([])
# MAGIC         ax.grid(color=gridColor, linewidth=gridWidth, linestyle='-')
# MAGIC         map(lambda position: ax.spines[position].set_visible(False), ['bottom', 'top', 'left', 'right'])
# MAGIC         
# MAGIC     if axList.size == 1:
# MAGIC         axList = axList[0]  # Just return a single axes object for a regular plot
# MAGIC     return fig, axList

# COMMAND ----------

# MAGIC %python
# MAGIC # ALL_NOTEBOOKS
# MAGIC 
# MAGIC data = irisTwoFeaturesDF.select("features", "label").collect()
# MAGIC features, labels = zip(*data)
# MAGIC 
# MAGIC x, y = zip(*features)
# MAGIC centers = modelCenters[5]
# MAGIC centroidX, centroidY = zip(*centers)
# MAGIC colorMap = 'Set1'  # was 'Set2', 'Set1', 'Dark2', 'winter'
# MAGIC 
# MAGIC fig, ax = prepareSubplot(np.arange(-1, 1.1, .4), np.arange(-1, 1.1, .4), figsize=(8,6))
# MAGIC plt.scatter(x, y, s=14**2, c=labels, edgecolors='#8cbfd0', alpha=0.80, cmap=colorMap)
# MAGIC plt.scatter(centroidX, centroidY, s=22**2, marker='*', c='yellow')
# MAGIC cmap = cm.get_cmap(colorMap)
# MAGIC 
# MAGIC colorIndex = [.5, .99, .0]
# MAGIC for i, (x,y) in enumerate(centers):
# MAGIC     print(cmap(colorIndex[i]))
# MAGIC     for size in [.10, .20, .30, .40, .50]:
# MAGIC         circle1=plt.Circle((x,y),size,color=cmap(colorIndex[i]), alpha=.10, linewidth=2)
# MAGIC         ax.add_artist(circle1)
# MAGIC 
# MAGIC ax.set_xlabel('Sepal Length'), ax.set_ylabel('Sepal Width')
# MAGIC display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC In addition to seeing the overlay of the clusters at each iteration, we can see how the cluster centers moved with each iteration (and what our results would have looked like if we used fewer iterations).

# COMMAND ----------

# MAGIC %python
# MAGIC # ALL_NOTEBOOKS
# MAGIC 
# MAGIC x, y = zip(*features)
# MAGIC 
# MAGIC oldCentroidX, oldCentroidY = None, None
# MAGIC 
# MAGIC fig, axList = prepareSubplot(np.arange(-1, 1.1, .4), np.arange(-1, 1.1, .4), figsize=(11, 15),
# MAGIC                              subplots=(3, 2))
# MAGIC axList = axList.flatten()
# MAGIC 
# MAGIC for i,ax in enumerate(axList[:]):
# MAGIC     ax.set_title('K-means for {0} iterations'.format(iterations[i]), color='#999999')
# MAGIC     centroids = modelCenters[i]
# MAGIC     centroidX, centroidY = zip(*centroids)
# MAGIC     
# MAGIC     ax.scatter(x, y, s=10**2, c=labels, edgecolors='#8cbfd0', alpha=0.80, cmap=colorMap, zorder=0)
# MAGIC     ax.scatter(centroidX, centroidY, s=16**2, marker='*', c='yellow', zorder=2)
# MAGIC     if oldCentroidX and oldCentroidY:
# MAGIC       ax.scatter(oldCentroidX, oldCentroidY, s=16**2, marker='*', c='grey', zorder=1)
# MAGIC     cmap = cm.get_cmap(colorMap)
# MAGIC     
# MAGIC     colorIndex = [.5, .99, 0.]
# MAGIC     for i, (x1,y1) in enumerate(centroids):
# MAGIC       print(cmap(colorIndex[i]))
# MAGIC       circle1=plt.Circle((x1,y1),.35,color=cmap(colorIndex[i]), alpha=.40)
# MAGIC       ax.add_artist(circle1)
# MAGIC     
# MAGIC     ax.set_xlabel('Sepal Length'), ax.set_ylabel('Sepal Width')
# MAGIC     oldCentroidX, oldCentroidY = centroidX, centroidY
# MAGIC 
# MAGIC plt.tight_layout()
# MAGIC 
# MAGIC display(fig)

# COMMAND ----------

# MAGIC %md So let's take a look at what's happening here in the distributed setting.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img src="https://files.training.databricks.com/images/Mapstage.png" height=200px>

# COMMAND ----------

# MAGIC %md <img src="https://files.training.databricks.com/images/Mapstage2.png" height=500px>

# COMMAND ----------

# MAGIC %md <img src="https://files.training.databricks.com/images/ReduceStage.png" height=500px>

# COMMAND ----------

# MAGIC %md <img src="https://files.training.databricks.com/images/Communication.png" height=500px>

# COMMAND ----------

# MAGIC %md ## Take Aways
# MAGIC 
# MAGIC When designing/choosing distributed ML algorithms
# MAGIC * Communication is key!
# MAGIC * Consider your data/model dimensions & how much data you need.
# MAGIC * Data partitioning/organization is important.