import logging

from flask import json
from kafka import KafkaProducer

logger = logging.getLogger(__name__)


class KafkaPublisher:
    """Broker to publish message"""

    def __init__(self):
        self._producer = None
        self._kafka_config = dict()

    def init_app(self, app):
        config = app.config

        self._kafka_config = {
            'bootstrap_servers': config.get('KAFKA_HOST', 'localhost'),
            'security_protocol': config.get('KAFKA_SECURITY_PROTOCOL', 'PLAINTEXT'),
            'sasl_mechanism': config.get('KAFKA_SASL_MECHANISM'),
            'sasl_plain_username': config.get('KAFKA_SASL_PLAIN_USERNAME'),
            'sasl_plain_password': config.get('KAFKA_SASL_PLAIN_PASSWORD'),
            'retries': 5,
            'value_serializer': lambda m: json.dumps(m).encode('ascii'),
            'key_serializer': lambda m: str(m).encode()
        }

        self._producer = KafkaProducer(**self._kafka_config)

    @staticmethod
    def _on_send_success(record_metadata):
        logger.debug(f'Send data to [topic] {record_metadata.topic} '
                     f'[partition] {record_metadata.partition} '
                     f'[offset] {record_metadata.offset}')

    @staticmethod
    def _on_send_error(e):
        logger.error(f'Failed to send {e}')
        raise e

    def send(self, topic, data, key):
        ac_key = data[key]
        logger.debug(f'Send data "{data}"')
        logger.debug(f'Send {ac_key} to {topic}')
        self._producer.send(topic, data, ac_key).add_callback(self._on_send_success).add_errback(self._on_send_error)

    def flush(self):
        self._producer.flush()
