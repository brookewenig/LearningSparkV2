# Databricks notebook source
# MAGIC %md # Chapter 9: Building Reliable Data Lakes with Delta Lake and Apache Spark™
# MAGIC 
# MAGIC Delta Lake: An open-source storage format that brings ACID transactions to Apache Spark™ and big data workloads.
# MAGIC 
# MAGIC <img src="https://docs.delta.io/latest/_static/delta-lake-logo.png" width=300/>
# MAGIC 
# MAGIC 
# MAGIC * **Open format**: Stored as Parquet format in blob storage.
# MAGIC * **ACID Transactions**: Ensures data integrity and read consistency with complex, concurrent data pipelines.
# MAGIC * **Schema Enforcement and Evolution**: Ensures data cleanliness by blocking writes with unexpected.
# MAGIC * **Audit History**: History of all the operations that happened in the table.
# MAGIC * **Time Travel**: Query previous versions of the table by time or version number.
# MAGIC * **Deletes and upserts**: Supports deleting and upserting into tables with programmatic APIs.
# MAGIC * **Scalable Metadata management**: Able to handle millions of files are scaling the metadata operations with Spark.
# MAGIC * **Unified Batch and Streaming Source and Sink**: A table in Delta Lake is both a batch table, as well as a streaming source and sink. Streaming data ingest, batch historic backfill, and interactive queries all just work out of the box. 
# MAGIC 
# MAGIC ### Source:
# MAGIC This notebook is a modified version of the [SAIS EU 2019 Delta Lake Tutorial](https://github.com/delta-io/delta/tree/master/examples/tutorials/saiseu19). The data used is a modified version of the public data from [Lending Club](https://www.kaggle.com/wendykan/lending-club-loan-data). It includes all funded loans from 2012 to 2017. Each loan includes applicant information provided by the applicant as well as the current loan status (Current, Late, Fully Paid, etc.) and latest payment information. For a full view of the data please view the data dictionary available [here](https://resources.lendingclub.com/LCDataDictionary.xlsx).

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Loading data in Delta Lake table
# MAGIC 
# MAGIC First let’s, read this data and save it as a Delta Lake table.

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("set spark.sql.shuffle.partitions = 1")
# MAGIC 
# MAGIC val sourcePath = "/databricks-datasets/learning-spark-v2/loans/loan-risks.snappy.parquet"
# MAGIC 
# MAGIC // Configure Delta Lake Path
# MAGIC val deltaPath = "/tmp/loans_delta"
# MAGIC 
# MAGIC // Remove folder if it exists
# MAGIC dbutils.fs.rm(deltaPath, recurse = true)
# MAGIC 
# MAGIC // Create the Delta table with the same loans data
# MAGIC spark.read.format("parquet").load(sourcePath)
# MAGIC   .write.format("delta").save(deltaPath)
# MAGIC 
# MAGIC spark.read.format("delta").load(deltaPath).createOrReplaceTempView("loans_delta")
# MAGIC println("Defined view 'loans_delta'")

# COMMAND ----------

# MAGIC %python
# MAGIC spark.sql("set spark.sql.shuffle.partitions = 1")
# MAGIC 
# MAGIC sourcePath = "/databricks-datasets/learning-spark-v2/loans/loan-risks.snappy.parquet"
# MAGIC 
# MAGIC # Configure Delta Lake Path
# MAGIC deltaPath = "/tmp/loans_delta"
# MAGIC 
# MAGIC # Remove folder if it exists
# MAGIC dbutils.fs.rm(deltaPath, recurse=True)
# MAGIC 
# MAGIC # Create the Delta table with the same loans data
# MAGIC (spark.read.format("parquet").load(sourcePath) 
# MAGIC   .write.format("delta").save(deltaPath))
# MAGIC 
# MAGIC spark.read.format("delta").load(deltaPath).createOrReplaceTempView("loans_delta")
# MAGIC print("Defined view 'loans_delta'")

# COMMAND ----------

# MAGIC %md Let's explore the data.

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("SELECT count(*) FROM loans_delta").show()

# COMMAND ----------

# MAGIC %python 
# MAGIC spark.sql("SELECT count(*) FROM loans_delta").show()

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("SELECT * FROM loans_delta LIMIT 5").show()

# COMMAND ----------

# MAGIC %python 
# MAGIC spark.sql("SELECT * FROM loans_delta LIMIT 5").show()

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Loading data streams into Delta Lake table
# MAGIC 
# MAGIC We will generate a stream of data from with randomly generated loan ids and amounts. 
# MAGIC In addition, we are going to define a few useful utility functions.

# COMMAND ----------

# MAGIC %scala
# MAGIC import java.util.Random
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
# MAGIC import org.apache.spark.sql.types._
# MAGIC 
# MAGIC val random = new Random()
# MAGIC 
# MAGIC def randomCheckpointDir: String = 
# MAGIC   s"/tmp/chkpt/${random.nextInt(10000)}"
# MAGIC 
# MAGIC 
# MAGIC // User-defined function to generate random state
# MAGIC val states = Seq("CA", "TX", "NY", "WA")
# MAGIC val randomState = udf(() => states(random.nextInt(states.length)))
# MAGIC 
# MAGIC // Function to start a streaming query with a stream of randomly generated data and append to the parquet table
# MAGIC def generateAndAppendDataStream(): StreamingQuery = {
# MAGIC   
# MAGIC   val newLoanStreamDF = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
# MAGIC     .withColumn("loan_id", col("value") + 10000)
# MAGIC     .withColumn("funded_amnt", (rand() * 5000 + 5000).cast("integer"))
# MAGIC     .withColumn("paid_amnt", col("funded_amnt") - (rand() * 2000))
# MAGIC     .withColumn("addr_state", randomState())
# MAGIC     .select("loan_id", "funded_amnt", "paid_amnt", "addr_state")
# MAGIC 
# MAGIC   val streamingQuery = newLoanStreamDF.writeStream
# MAGIC     .format("delta")
# MAGIC     .option("checkpointLocation", randomCheckpointDir)
# MAGIC     .trigger(Trigger.ProcessingTime("10 seconds"))
# MAGIC     .start(deltaPath)
# MAGIC 
# MAGIC   streamingQuery
# MAGIC }
# MAGIC 
# MAGIC // Function to stop all streaming queries 
# MAGIC def stopAllStreams(): Unit = {
# MAGIC   // Stop all the streams
# MAGIC   println("Stopping all streams")
# MAGIC   spark.streams.active.foreach(_.stop())
# MAGIC   println("Stopped all streams")
# MAGIC   println("Deleting checkpoints")  
# MAGIC   dbutils.fs.rm("/tmp/chkpt/", true)
# MAGIC   println("Deleted checkpoints")
# MAGIC }

# COMMAND ----------

# MAGIC %python
# MAGIC import random
# MAGIC import os
# MAGIC from pyspark.sql.functions import *
# MAGIC from pyspark.sql.types import *
# MAGIC 
# MAGIC def random_checkpoint_dir(): 
# MAGIC   return "/tmp/chkpt/%s" % str(random.randint(0, 10000))
# MAGIC 
# MAGIC 
# MAGIC # User-defined function to generate random state
# MAGIC 
# MAGIC states = ["CA", "TX", "NY", "WA"]
# MAGIC 
# MAGIC @udf(returnType=StringType())
# MAGIC def random_state():
# MAGIC   return str(random.choice(states))
# MAGIC 
# MAGIC 
# MAGIC # Function to start a streaming query with a stream of randomly generated data and append to the parquet table
# MAGIC def generate_and_append_data_stream():
# MAGIC 
# MAGIC   newLoanStreamDF = (spark.readStream.format("rate").option("rowsPerSecond", 5).load() 
# MAGIC     .withColumn("loan_id", 10000 + col("value")) 
# MAGIC     .withColumn("funded_amnt", (rand() * 5000 + 5000).cast("integer")) 
# MAGIC     .withColumn("paid_amnt", col("funded_amnt") - (rand() * 2000)) 
# MAGIC     .withColumn("addr_state", random_state())
# MAGIC     .select("loan_id", "funded_amnt", "paid_amnt", "addr_state"))
# MAGIC     
# MAGIC   checkpointDir = random_checkpoint_dir()
# MAGIC 
# MAGIC   streamingQuery = (newLoanStreamDF.writeStream 
# MAGIC     .format("delta") 
# MAGIC     .option("checkpointLocation", random_checkpoint_dir()) 
# MAGIC     .trigger(processingTime = "10 seconds") 
# MAGIC     .start(deltaPath))
# MAGIC 
# MAGIC   return streamingQuery
# MAGIC 
# MAGIC # Function to stop all streaming queries 
# MAGIC def stop_all_streams():
# MAGIC   # Stop all the streams
# MAGIC   print("Stopping all streams")
# MAGIC   for s in spark.streams.active:
# MAGIC     s.stop()
# MAGIC   print("Stopped all streams")
# MAGIC   print("Deleting checkpoints")  
# MAGIC   dbutils.fs.rm("/tmp/chkpt/", True)
# MAGIC   print("Deleted checkpoints")

# COMMAND ----------

# MAGIC %scala
# MAGIC val streamQuery = generateAndAppendDataStream() 

# COMMAND ----------

# MAGIC %python
# MAGIC streamingQuery = generate_and_append_data_stream()

# COMMAND ----------

# MAGIC %md You can see that the streaming query is adding data to the table by counting the number of records in the table. Run the following cell multiple times. 

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("SELECT count(*) FROM loans_delta").show()

# COMMAND ----------

# MAGIC %python
# MAGIC spark.sql("SELECT count(*) FROM loans_delta").show()

# COMMAND ----------

# MAGIC %md **Remember to stop all the streaming queries.**

# COMMAND ----------

# MAGIC %scala
# MAGIC stopAllStreams()

# COMMAND ----------

# MAGIC %python
# MAGIC stop_all_streams()

# COMMAND ----------

# MAGIC %md ##  ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Enforcing schema on write to prevent data corruption
# MAGIC 
# MAGIC  Let’s test this by trying to write some data with an additional column `closed` that signifies whether the loan has been terminated. Note that this column does not exist in the table.

# COMMAND ----------

# MAGIC %scala
# MAGIC val loanUpdates = Seq(
# MAGIC     (1111111L, 1000, 1000.0, "TX", false), 
# MAGIC     (2222222L, 2000, 0.0, "CA", true))
# MAGIC   .toDF("loan_id", "funded_amnt", "paid_amnt", "addr_state", "closed")

# COMMAND ----------

# MAGIC %python
# MAGIC cols = ['loan_id', 'funded_amnt', 'paid_amnt', 'addr_state', 'closed']
# MAGIC 
# MAGIC items = [
# MAGIC   (1111111, 1000, 1000.0, 'TX', True), 
# MAGIC   (2222222, 2000, 0.0, 'CA', False)
# MAGIC ]
# MAGIC 
# MAGIC from pyspark.sql.functions import *
# MAGIC 
# MAGIC loanUpdates = (spark
# MAGIC                 .createDataFrame(items, cols)
# MAGIC                 .withColumn("funded_amnt", col("funded_amnt").cast("int")))

# COMMAND ----------

# MAGIC %scala
# MAGIC // Uncomment the line below and it will error
# MAGIC // loanUpdates.write.format("delta").mode("append").save(deltaPath)

# COMMAND ----------

# MAGIC %python
# MAGIC # Uncomment the line below and it will error
# MAGIC # loanUpdates.write.format("delta").mode("append").save(deltaPath)

# COMMAND ----------

# MAGIC %md ##  ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Evolving schema to accommodate changing data

# COMMAND ----------

# MAGIC %scala 
# MAGIC loanUpdates.write.format("delta").mode("append")
# MAGIC   .option("mergeSchema", "true")
# MAGIC   .save(deltaPath)

# COMMAND ----------

# MAGIC %python 
# MAGIC (loanUpdates.write.format("delta").mode("append")
# MAGIC   .option("mergeSchema", "true")
# MAGIC   .save(deltaPath))

# COMMAND ----------

# MAGIC %md Let's query the table once again to see the schema.

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.read.format("delta").load(deltaPath).filter("loan_id = 1111111").show()

# COMMAND ----------

# MAGIC %python
# MAGIC spark.read.format("delta").load(deltaPath).filter("loan_id = 1111111").show()

# COMMAND ----------

# MAGIC %md For existing rows are read, the value of the new column is considered as NULL.  

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.read.format("delta").load(deltaPath).show(10)

# COMMAND ----------

# MAGIC %python 
# MAGIC spark.read.format("delta").load(deltaPath).show()

# COMMAND ----------

# MAGIC %md ##  ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Transforming existing data
# MAGIC 
# MAGIC 
# MAGIC Let's look into how we can transform existing data. But first, let's refine the view on the table because the schema has changed and the view needs to redefined to pick up the new schema.

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.read.format("delta").load(deltaPath).createOrReplaceTempView("loans_delta")
# MAGIC println("Defined view 'loans_delta'")

# COMMAND ----------

# MAGIC %python 
# MAGIC spark.read.format("delta").load(deltaPath).createOrReplaceTempView("loans_delta")
# MAGIC print("Defined view 'loans_delta'")

# COMMAND ----------

# MAGIC %md #### Updating loan data to fix errors
# MAGIC * Upon reviewing the data, we realized that all of the loans assigned to `addr_state = 'OR'` should have been assigned to `addr_state = 'WA'`.
# MAGIC * In Parquet, to do an `update`, you would need to 
# MAGIC   * Copy all of the rows that are not affected into a new table
# MAGIC   * Copy all of the rows that are affected into a DataFrame, perform the data modification
# MAGIC   * Insert the previously noted DataFrame's rows into the new table
# MAGIC   * Remove the old table
# MAGIC   * Rename the new table to the old table

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("""SELECT addr_state, count(1) FROM loans_delta WHERE addr_state IN ('OR', 'WA', 'CA', 'TX', 'NY') GROUP BY addr_state""").show()

# COMMAND ----------

# MAGIC %python
# MAGIC spark.sql("""SELECT addr_state, count(1) FROM loans_delta WHERE addr_state IN ('OR', 'WA', 'CA', 'TX', 'NY') GROUP BY addr_state""").show()

# COMMAND ----------

# MAGIC %md Let's fix the data.

# COMMAND ----------

# MAGIC %scala
# MAGIC import io.delta.tables.DeltaTable
# MAGIC import org.apache.spark.sql.functions._
# MAGIC 
# MAGIC val deltaTable = DeltaTable.forPath(spark, deltaPath)
# MAGIC deltaTable.update(
# MAGIC   col("addr_state") === "OR",
# MAGIC   Map("addr_state" -> lit("WA")))

# COMMAND ----------

# MAGIC %python
# MAGIC from delta.tables import *
# MAGIC 
# MAGIC deltaTable = DeltaTable.forPath(spark, deltaPath)
# MAGIC deltaTable.update("addr_state = 'OR'",  {"addr_state": "'WA'"})

# COMMAND ----------

# MAGIC %md Let's see the data once again.

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("""SELECT addr_state, count(1) FROM loans_delta WHERE addr_state IN ('OR', 'WA', 'CA', 'TX', 'NY') GROUP BY addr_state""").show()

# COMMAND ----------

# MAGIC %python 
# MAGIC spark.sql("""SELECT addr_state, count(1) FROM loans_delta WHERE addr_state IN ('OR', 'WA', 'CA', 'TX', 'NY') GROUP BY addr_state""").show()

# COMMAND ----------

# MAGIC %md #### Deleting user-related data from a table General Data Protection Regulation (GDPR)
# MAGIC 
# MAGIC You can remove data that matches a predicate from a Delta Lake table. Let's say we want to remove all the fully paid loans. Let's first see how many are there.

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("SELECT COUNT(*) FROM loans_delta WHERE funded_amnt = paid_amnt").show()

# COMMAND ----------

# MAGIC %python 
# MAGIC spark.sql("SELECT COUNT(*) FROM loans_delta WHERE funded_amnt = paid_amnt").show()

# COMMAND ----------

# MAGIC %md Now let's delete them.

# COMMAND ----------

# MAGIC %scala
# MAGIC import io.delta.tables.DeltaTable
# MAGIC 
# MAGIC val deltaTable = DeltaTable.forPath(spark, deltaPath)
# MAGIC deltaTable.delete("funded_amnt = paid_amnt")

# COMMAND ----------

# MAGIC %python
# MAGIC from delta.tables import *
# MAGIC 
# MAGIC deltaTable = DeltaTable.forPath(spark, deltaPath)
# MAGIC deltaTable.delete("funded_amnt = paid_amnt")

# COMMAND ----------

# MAGIC %md Let's check the number of fully paid loans once again.

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("SELECT COUNT(*) FROM loans_delta WHERE funded_amnt = paid_amnt").show()

# COMMAND ----------

# MAGIC %python
# MAGIC spark.sql("SELECT COUNT(*) FROM loans_delta WHERE funded_amnt = paid_amnt").show()

# COMMAND ----------

# MAGIC %md ### Upserting change data to a table using merge
# MAGIC A common use cases is Change Data Capture (CDC), where you have to replicate row changes made in an OLTP table to another table for OLAP workloads. To continue with our loan data example, say we have another table of new loan information, some of which are new loans and others are updates to existing loans. In addition, let’s say this changes table has the same schema as the loan_delta table. You can upsert these changes into the table using the DeltaTable.merge() operation which is based on the MERGE SQL command.

# COMMAND ----------

# MAGIC %scala 
# MAGIC spark.sql("select * from loans_delta where addr_state = 'NY' and loan_id < 30").show()

# COMMAND ----------

# MAGIC %python 
# MAGIC spark.sql("select * from loans_delta where addr_state = 'NY' and loan_id < 30").show()

# COMMAND ----------

# MAGIC %md Let's say we have some changes to this data, one loan has been paid off, and another new loan has been added.

# COMMAND ----------

# MAGIC %scala
# MAGIC val loanUpdates = Seq(
# MAGIC     (11, 1000, 1000.0, "NY", true),   // loan paid off
# MAGIC     (12, 1000, 0.0, "NY", false))     // new loan
# MAGIC   .toDF("loan_id", "funded_amnt", "paid_amnt", "addr_state", "closed")

# COMMAND ----------

# MAGIC %python 
# MAGIC 
# MAGIC cols = ['loan_id', 'funded_amnt', 'paid_amnt', 'addr_state', 'closed']
# MAGIC 
# MAGIC items = [
# MAGIC   (11, 1000, 1000.0, 'NY', True),   # loan paid off
# MAGIC   (12, 1000, 0.0, 'NY', False)      # new loan
# MAGIC ]
# MAGIC 
# MAGIC loanUpdates = spark.createDataFrame(items, cols)

# COMMAND ----------

# MAGIC %md Now, let's update the table with the change data using the `merge` operation.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import io.delta.tables.DeltaTable
# MAGIC 
# MAGIC val deltaTable = DeltaTable.forPath(spark, deltaPath)
# MAGIC 
# MAGIC deltaTable
# MAGIC   .alias("t")
# MAGIC   .merge(loanUpdates.alias("s"), "t.loan_id = s.loan_id")
# MAGIC   .whenMatched.updateAll()
# MAGIC   .whenNotMatched.insertAll()
# MAGIC   .execute()

# COMMAND ----------

# MAGIC %python 
# MAGIC 
# MAGIC from delta.tables import *
# MAGIC 
# MAGIC deltaTable = DeltaTable.forPath(spark, deltaPath)
# MAGIC 
# MAGIC (deltaTable
# MAGIC   .alias("t")
# MAGIC   .merge(loanUpdates.alias("s"), "t.loan_id = s.loan_id") 
# MAGIC   .whenMatchedUpdateAll() 
# MAGIC   .whenNotMatchedInsertAll() 
# MAGIC   .execute())

# COMMAND ----------

# MAGIC %md Let's see whether the table has been updated.

# COMMAND ----------

# MAGIC %scala 
# MAGIC 
# MAGIC spark.sql("select * from loans_delta where addr_state = 'NY' and loan_id < 30").show()

# COMMAND ----------

# MAGIC %python 
# MAGIC 
# MAGIC spark.sql("select * from loans_delta where addr_state = 'NY' and loan_id < 30").show()

# COMMAND ----------

# MAGIC %md ### Deduplicating data while inserting using insert-only merge
# MAGIC 
# MAGIC The merge operation in Delta Lake supports an extended syntax beyond that specified by the ANSI standard. It supports advanced features like the following. 
# MAGIC - Delete actions: For example, `MERGE … WHEN MATCHED THEN DELETE`
# MAGIC - Clause conditions: For example, `MERGE … WHEN MATCHED AND <condition> THEN ...``
# MAGIC - Optional actions: All the MATCHED and NOT MATCHED clauses are optional.
# MAGIC - Star syntax: For example, `UPDATE *` and `INSERT *` to update/insert all the columns in the target table with matching columns from the source dataset. The equivalent API in DeltaTable is `updateAll()` and `insertAll()`, which we have already seen.
# MAGIC 
# MAGIC This allows you to express many more complex use cases with little code. For example, say you want to backfill the loan_delta table with historical data of past loans. But some of the historical data may already have been inserted in the table and you don't want to update them (since their emails may already have been updated). You can deduplicate by the loan_id while inserting by running the following merge operation with only the INSERT action (since the UPDATE action is optional).

# COMMAND ----------

# MAGIC %scala 
# MAGIC 
# MAGIC spark.sql("select * from loans_delta where addr_state = 'NY' and loan_id < 30").show()

# COMMAND ----------

# MAGIC %python 
# MAGIC 
# MAGIC spark.sql("select * from loans_delta where addr_state = 'NY' and loan_id < 30").show()

# COMMAND ----------

# MAGIC %md Let's say we have some historical data that we want to merge with this table. One of the historical loan exists in the current table but the historical table has old values, therefore it should not update the current value present in the table. And another historical does not exist in the current table, therefore it should be inserted into the table.

# COMMAND ----------

# MAGIC %scala
# MAGIC val historicalUpdates = Seq(
# MAGIC     (11, 1000, 0.0, "NY", false),
# MAGIC     (-100, 1000, 10.0, "NY", false))
# MAGIC   .toDF("loan_id", "funded_amnt", "paid_amnt", "addr_state", "closed")

# COMMAND ----------

# MAGIC %python 
# MAGIC 
# MAGIC cols = ['loan_id', 'funded_amnt', 'paid_amnt', 'addr_state', 'closed']
# MAGIC 
# MAGIC items = [
# MAGIC   (11, 1000, 0.0, 'NY', False),
# MAGIC   (-100, 1000, 10.0, 'NY', False)
# MAGIC ]
# MAGIC 
# MAGIC historicalUpdates = spark.createDataFrame(items, cols)

# COMMAND ----------

# MAGIC %md Let's do the merge.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import io.delta.tables.DeltaTable
# MAGIC 
# MAGIC val deltaTable = DeltaTable.forPath(spark, deltaPath)
# MAGIC 
# MAGIC deltaTable
# MAGIC   .alias("t")
# MAGIC   .merge(historicalUpdates.alias("s"), "t.loan_id = s.loan_id")
# MAGIC   .whenNotMatched.insertAll()
# MAGIC   .execute()

# COMMAND ----------

# MAGIC %python 
# MAGIC 
# MAGIC from delta.tables import *
# MAGIC 
# MAGIC deltaTable = DeltaTable.forPath(spark, deltaPath)
# MAGIC 
# MAGIC (deltaTable
# MAGIC   .alias("t")
# MAGIC   .merge(historicalUpdates.alias("s"), "t.loan_id = s.loan_id") 
# MAGIC   .whenNotMatchedInsertAll() 
# MAGIC   .execute())

# COMMAND ----------

# MAGIC %md Let's see whether the table has been updated.

# COMMAND ----------

# MAGIC %scala 
# MAGIC 
# MAGIC spark.sql("select * from loans_delta where addr_state = 'NY' and loan_id < 30").show()

# COMMAND ----------

# MAGIC %python 
# MAGIC 
# MAGIC spark.sql("select * from loans_delta where addr_state = 'NY' and loan_id < 30").show()

# COMMAND ----------

# MAGIC %md Notice that the only change in the table is that insert of new loan, and existing loans were not updated to old values.

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Auditing data changes with operation history
# MAGIC 
# MAGIC All changes to the Delta table are recorded as commits in the table's transaction log. As you write into a Delta table or directory, every operation is automatically versioned. You can use the HISTORY command to view the table's history.

# COMMAND ----------

# MAGIC %scala
# MAGIC import io.delta.tables.DeltaTable
# MAGIC 
# MAGIC val deltaTable = DeltaTable.forPath(spark, deltaPath)
# MAGIC deltaTable.history().show()

# COMMAND ----------

# MAGIC %python
# MAGIC from delta.tables import *
# MAGIC 
# MAGIC deltaTable = DeltaTable.forPath(spark, deltaPath)
# MAGIC deltaTable.history().show()

# COMMAND ----------

# MAGIC %scala 
# MAGIC deltaTable.history(4).select("version", "timestamp", "operation", "operationParameters").show(false)

# COMMAND ----------

# MAGIC %python
# MAGIC deltaTable.history(4).select("version", "timestamp", "operation", "operationParameters").show(truncate=False)

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Querying previous snapshots of the table with time travel
# MAGIC 
# MAGIC Delta Lake’s time travel feature allows you to access previous versions of the table. Here are some possible uses of this feature:
# MAGIC 
# MAGIC * Auditing Data Changes
# MAGIC * Reproducing experiments & reports
# MAGIC * Rollbacks
# MAGIC 
# MAGIC You can query by using either a timestamp or a version number using Python, Scala, and/or SQL syntax. For this examples we will query a specific version using the Python syntax.  
# MAGIC 
# MAGIC For more information, refer to [Introducing Delta Time Travel for Large Scale Data Lakes](https://databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html) and the [docs](https://docs.delta.io/latest/delta-batch.html#deltatimetravel).
# MAGIC 
# MAGIC **Let's query the table's state before we deleted the data, which still contains the fully paid loans.**

# COMMAND ----------

# MAGIC %scala
# MAGIC val previousVersion = deltaTable.history(1).select("version").first().getLong(0) - 3
# MAGIC 
# MAGIC spark.read.format("delta")
# MAGIC   .option("versionAsOf", previousVersion)
# MAGIC   .load(deltaPath)
# MAGIC   .createOrReplaceTempView("loans_delta_pre_delete")
# MAGIC 
# MAGIC spark.sql("SELECT COUNT(*) FROM loans_delta_pre_delete WHERE funded_amnt = paid_amnt").show()

# COMMAND ----------

# MAGIC %python
# MAGIC previousVersion = deltaTable.history(1).select("version").first()[0] - 3
# MAGIC 
# MAGIC (spark.read.format("delta")
# MAGIC   .option("versionAsOf", previousVersion)
# MAGIC   .load(deltaPath)
# MAGIC   .createOrReplaceTempView("loans_delta_pre_delete"))
# MAGIC 
# MAGIC spark.sql("SELECT COUNT(*) FROM loans_delta_pre_delete WHERE funded_amnt = paid_amnt").show()

# COMMAND ----------

# MAGIC %md We see the same number of fully paid loans that we had seen before delete.