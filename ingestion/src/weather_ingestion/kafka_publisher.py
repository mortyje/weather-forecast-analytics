import json
from typing import Any

from confluent_kafka import KafkaError, Message, Producer


class KafkaPublisher:
    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self._topic = topic
        self._delivery_errors: list[str] = []

        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "weather-ingestion",
                "acks": "all",
                "enable.idempotence": True,
                "message.timeout.ms": 20000,
            }
        )

    def publish(self, message: dict[str, Any]) -> None:
        city_id = message["city_id"]

        value = json.dumps(
            message,
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")

        def on_delivery(error: KafkaError | None, _: Message) -> None:
            if error is not None:
                self._delivery_errors.append(f"{city_id}: {error}")

        self._producer.produce(
            topic=self._topic,
            key=city_id.encode("utf-8"),
            value=value,
            on_delivery=on_delivery,
        )

        self._producer.poll(0)

    def flush(self) -> None:
        pending_messages = self._producer.flush(25)

        if pending_messages:
            raise RuntimeError(
                f"{pending_messages} Kafka message(s) were not delivered"
            )

        if self._delivery_errors:
            raise RuntimeError(
                "Kafka delivery failed: " + "; ".join(self._delivery_errors)
            )
