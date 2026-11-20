import os
from itertools import chain, islice

import psycopg
from pyspark.sql import DataFrame, SparkSession

from observation_stream import start_observation_stream

INSERT_SQL = """
    INSERT INTO raw.forecast_events (
        topic,
        partition_id,
        offset_id,
        message_key,
        payload,
        kafka_timestamp_ms
    )
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (topic, partition_id, offset_id)
    DO NOTHING
"""


def write_partition(rows) -> None:
    iterator = iter(rows)
    first_row = next(iterator, None)

    if first_row is None:
        return

    records = chain((first_row,), iterator)

    with psycopg.connect(connect_timeout=10) as connection:
        with connection.cursor() as cursor:
            while batch := list(islice(records, 100)):
                values = [
                    (
                        row.topic,
                        row.partition_id,
                        row.offset_id,
                        row.message_key,
                        row.payload,
                        row.kafka_timestamp_ms,
                    )
                    for row in batch
                ]

                cursor.executemany(INSERT_SQL, values)


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    records = batch_df.selectExpr(
        "topic",
        "partition AS partition_id",
        "offset AS offset_id",
        "CAST(key AS STRING) AS message_key",
        "CAST(value AS STRING) AS payload",
        "unix_millis(timestamp) AS kafka_timestamp_ms",
    )

    records.foreachPartition(write_partition)

    print(f"Successfully processed batch {batch_id}", flush=True)


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("weather-kafka-raw-ingestion")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    messages = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        )
        .option(
            "subscribe",
            os.environ["KAFKA_FORECAST_TOPIC"],
        )
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .load()
    )

    (
        messages.writeStream
        .foreachBatch(process_batch)
        .option(
            "checkpointLocation",
            "/opt/spark/checkpoints/forecast_raw",
        )
        .trigger(processingTime="30 seconds")
        .start()
    )

    start_observation_stream(spark)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
