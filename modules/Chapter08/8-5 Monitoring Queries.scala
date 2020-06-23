// Databricks notebook source
// MAGIC %md # Monitoring Queries

// COMMAND ----------

val myListener = new StreamingQueryListener() {
    override def onQueryStarted(event: QueryStartedEvent): Unit = {
        println("Query started: " + event.id)
    }
    override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
        println("Query terminated: " + event.id)
    }
    override def onQueryProgress(event: QueryProgressEvent): Unit = {
        println("Query made progress: " + event.progress)
    }
}



// COMMAND ----------

spark.streams.addListener(myListener)