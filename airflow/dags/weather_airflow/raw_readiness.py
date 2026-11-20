import logging
import os

import psycopg
from confluent_kafka import Consumer, TopicPartition
from psycopg import sql

logger = logging.getLogger(__name__)

TOPIC_TABLES = {
    "weather.forecasts.raw": "forecast_events",
    "weather.observations.raw": "observation_events",
}
KAFKA_TIMEOUT = 10
KafkaRanges = dict[str, dict[str, dict[str, int]]]


def capture_kafka_ranges() -> KafkaRanges:
    """Capture the broker's current low and high offsets without consuming messages."""
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "group.id": "weather-dwh-raw-readiness",
            "enable.auto.commit": False,
        }
    )
    try:
        topics = consumer.list_topics(timeout=KAFKA_TIMEOUT).topics
        ranges: KafkaRanges = {}
        for topic in TOPIC_TABLES:
            metadata = topics.get(topic)
            if metadata is None:
                raise RuntimeError(f"Kafka topic {topic} was not found")
            if metadata.error is not None:
                raise RuntimeError(f"Kafka metadata error for {topic}: {metadata.error}")
            if not metadata.partitions:
                raise RuntimeError(f"Kafka topic {topic} has no partitions")

            partitions: dict[str, dict[str, int]] = {}
            for partition_id, partition in sorted(metadata.partitions.items()):
                topic_partition = f"{topic}:{partition_id}"
                if partition.error is not None:
                    raise RuntimeError(f"Kafka metadata error for {topic_partition}: {partition.error}")
                bounds = consumer.get_watermark_offsets(
                    TopicPartition(topic, partition_id),
                    timeout=KAFKA_TIMEOUT,
                    cached=False,
                )
                if bounds is None:
                    raise TimeoutError(f"Timed out fetching Kafka offsets for {topic_partition}")
                low, high = bounds
                if low < 0 or high < low:
                    raise ValueError(f"Invalid Kafka offset range for {topic_partition}: {bounds}")
                partitions[str(partition_id)] = {"low": low, "high": high}
            ranges[topic] = partitions
        return ranges
    finally:
        consumer.close()


def raw_ranges_are_loaded(kafka_ranges: KafkaRanges) -> bool:
    """Check that Raw contains every offset in the captured half-open ranges."""
    all_loaded = True
    with psycopg.connect(connect_timeout=10) as connection:
        for topic, table_name in TOPIC_TABLES.items():
            partitions = kafka_ranges[topic]
            if not partitions:
                raise ValueError(f"No captured Kafka partitions for {topic}")
            query = sql.SQL(
                """
                SELECT COUNT(*)
                FROM raw.{}
                WHERE topic = %s
                    AND partition_id = %s
                    AND offset_id >= %s
                    AND offset_id < %s
                """
            ).format(sql.Identifier(table_name))

            for partition_id, bounds in partitions.items():
                topic_partition = f"{topic}:{partition_id}"
                low = bounds["low"]
                high = bounds["high"]
                if low < 0 or high < low:
                    raise ValueError(f"Invalid Kafka offset range for {topic_partition}: {bounds}")
                expected_count = high - low
                if expected_count == 0:
                    continue
                # The Raw primary key guarantees distinct offsets within the range.
                actual_count = connection.execute(
                    query, (topic, int(partition_id), low, high)
                ).fetchone()[0]
                if actual_count != expected_count:
                    all_loaded = False
                    logger.info(
                        "Raw not ready: %s expected=%s actual=%s",
                        topic_partition,
                        expected_count,
                        actual_count,
                    )

    if all_loaded:
        logger.info("All captured Kafka ranges are present in Raw")
    return all_loaded
