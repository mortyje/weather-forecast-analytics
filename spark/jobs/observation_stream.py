import os
from itertools import islice

import psycopg
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

INSERT_SQL = """
    INSERT INTO raw.observation_events (
        topic,
        partition_id,
        offset_id,
        message_key,
        payload
    )
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (topic, partition_id, offset_id)
    DO NOTHING
"""


def write_observation_batch(batch: DataFrame, batch_id: int) -> None:
    records = batch.toLocalIterator()
    total_rows = 0

    with psycopg.connect(connect_timeout=10) as connection, connection.cursor() as cursor:
        while rows := list(islice(records, 1000)):
            cursor.executemany(
                INSERT_SQL,
                [
                    (row.topic, row.partition_id, row.offset_id, row.message_key, row.payload)
                    for row in rows
                ],
            )
            total_rows += len(rows)

    print(
        f"Observation microbatch {batch_id}: processed {total_rows} messages",
        flush=True,
    )


def start_observation_stream(spark: SparkSession) -> StreamingQuery:
    messages = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", os.environ["KAFKA_BOOTSTRAP_SERVERS"])
        .option("subscribe", os.environ["KAFKA_OBSERVATION_TOPIC"])
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .load()
        .selectExpr(
            "topic",
            "partition AS partition_id",
            "offset AS offset_id",
            "CAST(key AS STRING) AS message_key",
            "CAST(value AS STRING) AS payload",
        )
    )

    return (
        messages.writeStream
        .queryName("observation_raw")
        .foreachBatch(write_observation_batch)
        .option("checkpointLocation", os.environ["OBSERVATIONS_CHECKPOINT_PATH"])
        .trigger(processingTime="30 seconds")
        .start()
    )
