from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *


@dp.table(
name="sentinel.gold.transaciton_count_by_minute_sliding_window"
,comment="Alert details where transaciton has been performed with value higher than what is configured by customer"
)
def transaciton_count_by_minute() -> DataFrame:

    transactions=spark.readStream.table("sentinel.silver.transactions")

    transactions_with_watermark=transactions.withWatermark("transaction_timestamp", "5 minutes")

    #using sliding window, checks transactions count for every 5 minutes and slides for 1 minute i.e., for every 1 minute, it gives aggregation for last 5 minutes.
    transaction_count_df=transactions_with_watermark.groupBy(
        window("transaction_timestamp","5 minutes","1 minute")
    ).agg(

    count("*").alias("transaction_count")
    ).select(
        col("window.start").alias("window_starttime"),
        col("window.end").alias("window_endtime"),
        col("transaction_count")
    )
    return transaction_count_df
