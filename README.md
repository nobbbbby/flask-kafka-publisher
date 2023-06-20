# Flask-Kafka-Publisher

Flask-Kafka-Publisher is a Flask extension that simplifies publishing messages to Kafka within your Flask application.
It provides an easy-to-use interface to send messages to Kafka topics directly from your business logic, making it a
breeze to integrate Kafka into your Flask-based projects.

## Features

- Seamless integration: Integrate Kafka message publishing directly into your Flask application without hassle.
- Simple configuration: Set up the Kafka connection and topic details with minimal configuration.
- Lightweight and efficient: The extension is designed to be lightweight and has minimal impact on your application's
  performance.
- Flexible message publishing: Send messages to Kafka from any part of your Flask application, including within your
  business logic.

## Installation

You can install Flask-Kafka-Publisher using pip:

```shell
pip install flask_kafka_publisher
```

## Usage

```python

from flask import Flask, request
from flask_kafka_publisher import KafkaPublisher

app = Flask(__name__)

app.config['KAFKA_HOST'] = 'localhost:9092'

kafka_publisher = KafkaPublisher()
kafka_publisher.init_app(app)


@app.route('/publish', methods=['POST'])
def publish_message():
    message = request.json['message']
    kafka_publisher.send('your_topic', message)
    return 'Message published to Kafka!'
```

## License

Flask Kafka Publisher is open source and released under the MIT License. Feel free to use, modify, and distribute it as
per the terms of the license.