import json
from unittest.mock import Mock

import pytest

from weather_ingestion.shared import kafka_publisher


def make_publisher(monkeypatch, fake_producer):
    monkeypatch.setattr(
        kafka_publisher,
        "Producer",
        lambda config: fake_producer,
    )

    return kafka_publisher.KafkaPublisher(
        bootstrap_servers="kafka:9092",
        topic="weather.forecasts.raw",
    )


def test_publish_uses_city_id_as_key(monkeypatch):
    fake_producer = Mock()
    fake_producer.flush.return_value = 0

    publisher = make_publisher(monkeypatch, fake_producer)

    message = {
        "city_id": "moscow",
        "payload": {"temperature": 5.0},
    }

    publisher.publish(message)

    published = fake_producer.produce.call_args

    assert published.kwargs["topic"] == "weather.forecasts.raw"
    assert published.kwargs["key"] == b"moscow"
    assert json.loads(published.kwargs["value"]) == message

    fake_producer.poll.assert_called_once_with(0)

    publisher.flush()


def test_delivery_error_is_reported(monkeypatch):
    fake_producer = Mock()
    fake_producer.flush.return_value = 0

    publisher = make_publisher(monkeypatch, fake_producer)

    publisher.publish({"city_id": "moscow"})

    callback = fake_producer.produce.call_args.kwargs["on_delivery"]
    callback(RuntimeError("broker unavailable"), None)

    with pytest.raises(RuntimeError, match="broker unavailable"):
        publisher.flush()


def test_flush_timeout_is_reported(monkeypatch):
    fake_producer = Mock()
    fake_producer.flush.return_value = 1

    publisher = make_publisher(monkeypatch, fake_producer)

    with pytest.raises(RuntimeError, match="not delivered"):
        publisher.flush()
