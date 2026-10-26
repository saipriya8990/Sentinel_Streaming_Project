# Databricks notebook source
# bootstrap_server= 'pkc-lgk0v.us-west1.gcp.confluent.cloud:9092'
# api_key= 'W7LJKOSCJDMOI2GQ' 
# api_secret= 'cfltRxCw/rXR3FyLjNtk8QCDGdFxRF2MqQGTv2+cbNG1lMG1X0BWgDV28Rtdctsw' 
# topic= 'credit_card_transactions'

# COMMAND ----------

import json

kafka_connection_json=dbutils.secrets.get(scope="sentinel-scope",key="kafka_connection_details")
kafka_config=json.loads(kafka_connection_json)

bootstrap_server=kafka_config['bootstrap_server']
api_key=kafka_config['api_key']
api_secret=kafka_config['api_secret']
topic=kafka_config['topic']

# COMMAND ----------

jaas_config=f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{api_secret}";'

# COMMAND ----------

# MAGIC %md
# MAGIC **Testing the data using batch instead of streaming**

# COMMAND ----------

sample_batch = (
        spark.read.format("kafka")
            .option("kafka.bootstrap.server", "bootstrap_server")
            .option("subscribe",topic)
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("kafka.sasl.jaas.config", jaas_config)
            .option("startingOffsets","earliest")
            .load()
)

# COMMAND ----------

sample_batch = (
    spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_server)
        .option("subscribe",topic)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas_config)
        .option("startingOffsets","earliest")
        .load()
)
#sample_batch.count()

# COMMAND ----------

from pyspark.sql.functions import col

parsed_batch = sample_batch.select(
    col("key").cast("string"),
    col("value").cast("string"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
    col("timestampType")               
)

#display(parsed_batch)

# COMMAND ----------

parsed_batch.write.saveAsTable("sentinel.bronze.transactions_batch_test")

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC
# MAGIC select * from sentinel.bronze.transactions_batch_test

# COMMAND ----------

# MAGIC %md
# MAGIC **Working with Streaming data**

# COMMAND ----------

streaming_df = (
    spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_server)
        .option("subscribe",topic)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas_config)
        .option("startingOffsets","earliest")
        .load()
)


# COMMAND ----------

from pyspark.sql.functions import col

parsed_streaming = streaming_df.select(
    col("key").cast("string"),
    col("value").cast("string"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
    col("timestampType")               
)

#display(parsed_streaming)

# COMMAND ----------

streaming_query = (
parsed_streaming.writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/sentinel/source/transactions/checkpoint/")
    .outputMode("append")
    .trigger(availableNow=True)
    .table("sentinel.bronze.transactions_streaming_test")
)

print("Query started with Query ID: ",streaming_query.id)

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC select count(*) from sentinel.bronze.transactions_streaming_test

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sentinel.bronze.customers where customer_id in ('CUST000001','CUST001001','CUST001002')

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from sentinel.silver.customers limit 5;
# MAGIC select * from sentinel.gold.high_value_transactions_alert;