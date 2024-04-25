import requests
from confluent_kafka import Producer

def fetch_recent_changes():
    url = "https://stream.wikimedia.org/v2/stream/recentchange"
    response = requests.get(url, stream=True)
    for line in response.iter_lines():
        if line:
            yield line


def kafka_producer(config):
    producer = Producer(config)
    return producer


def produce_to_kafka(producer, topic, message):
    producer.produce(topic, value=message)
    producer.flush()


def print_kafka_config(config):
    print("Kafka Configuration:")
    for key, value in config.items():
        print(f"{key}: {value}")


def main():
    kafka_config = {
        "bootstrap.servers": "localhost:9092",  # Kafka broker address
        # "client.id": "wikimedia-stream-producer",
        # Additional Kafka configuration can be added here
    }
    kafka_topic = "wikimedia_recentchange"

    producer = kafka_producer(kafka_config)

    print_kafka_config(producer.config)  # Print Kafka configuration

    for change in fetch_recent_changes():
        produce_to_kafka(producer, kafka_topic, change)


if __name__ == "__main__":
    main()