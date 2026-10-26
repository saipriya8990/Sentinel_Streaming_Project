# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
source_path='/Volumes/sentinel/source/fraud_watchlist/source_data/'

# COMMAND ----------

dbutils.fs.ls(source_path)

# COMMAND ----------

input_stream = (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "json")
              .option("cloudFiles.schemaLocation", "/Volumes/sentinel/source/fraud_watchlist/schema/")
              .option("cloudFiles.inferColumnTypes","true")
              .load(source_path)
)


# COMMAND ----------

from pyspark.sql.functions import *
tranformed_df = input_stream.select(
                    "*",
                    col("_metadata.file_path").alias("file_path"),
                    current_timestamp().alias("ingestion_timestamp")
                    )

# COMMAND ----------

streaming_query=(tranformed_df.writeStream.format("delta")
                            .outputMode("Append")
                            .option("checkpointLocation", "/Volumes/sentinel/source/fraud_watchlist/checkpoint/")
                            .trigger(availableNow=True)
                            .toTable("sentinel.bronze.fraud_watchlist_batch_test")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sentinel.bronze.fraud_watchlist_batch_test

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sentinel.bronze.fraud_watchlist;
# MAGIC select * from sentinel.bronze.transactions;
# MAGIC select * from sentinel.bronze.customers;

# COMMAND ----------

# DBTITLE 1,silver tables
# MAGIC %sql
# MAGIC select * from sentinel.silver.customers;
# MAGIC select * from sentinel.silver.transactions;
# MAGIC select * from sentinel.silver.fraud_watchlist;

# COMMAND ----------

# DBTITLE 1,gold tables
# MAGIC %sql
# MAGIC select * from sentinel.gold.fraud_card_alert;
# MAGIC select * from sentinel.gold.high_value_transactions_alert;
# MAGIC select * from sentinel.gold.transaciton_count_by_minute order by window_starttime;
# MAGIC select * from sentinel.gold.transaciton_count_by_minute_sliding_window order by window_starttime;