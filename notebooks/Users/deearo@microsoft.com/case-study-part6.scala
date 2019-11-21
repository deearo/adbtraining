// Databricks notebook source
import org.apache.spark.eventhubs._
import org.apache.spark.sql.functions._

// COMMAND ----------

var eventHubUrl = "Endpoint=sb://deepakmessages.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=OrD9Saqm8l0R9C/JPz7A8VADEGqNbJ8n/Tgfi713sBo="
val connectionString = ConnectionStringBuilder(eventHubUrl).setEventHubName("messages").build
val eventHubsConf = EventHubsConf(connectionString).setStartingPosition(EventPosition.fromEndOfStream)


// COMMAND ----------

val eventhubs = 
  spark
    .readStream
    .format("eventhubs")
    .options(eventHubsConf.toMap)
    .load()

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger.ProcessingTime

var query = 
  eventhubs
   .select(
     get_json_object(($"body").cast("string"), "$.zip").alias("zip"),    
     get_json_object(($"body").cast("string"), "$.hittime").alias("hittime"), 
     date_format(get_json_object(($"body").cast("string"), "$.hittime"), "dd.MM.yyyy").alias("day"))

// COMMAND ----------

val queryOutput =
  query
    .writeStream
    .format("parquet")
    .option("path", "/data/processed")
    .option("checkpointLocation", "/data/checkpoints")
    .partitionBy("zip", "day")
    .trigger(ProcessingTime("25 seconds"))
    .start()

// COMMAND ----------

// MAGIC %fs
// MAGIC 
// MAGIC ls /data/processed

// COMMAND ----------

// MAGIC %sql 
// MAGIC 
// MAGIC DROP TABLE IF EXISTS ProcessedRecords;
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS ProcessedRecords
// MAGIC     (zip string, hittime string, day string)
// MAGIC     STORED AS PARQUET
// MAGIC     LOCATION "/data/processed"

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT * FROM ProcessedRecords

// COMMAND ----------

