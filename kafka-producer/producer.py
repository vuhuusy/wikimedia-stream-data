import requests
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer

def fetch_wikimedia_changes():
    url = "https://stream.wikimedia.org/v2/stream/recentchange"
    response = requests.get(url, stream=True)
    for line in response.iter_lines():
        if line:
            yield line.decode('utf-8')

def kafka_producer():
    producer_config = {
        'bootstrap.servers': 'localhost:9092'
    }
    producer = Producer(producer_config)
    return producer

def produce_to_kafka(topic, data):
    try:
        producer = kafka_producer()
        producer.produce(topic, value=data.encode('utf-8'))
        producer.flush()
    except Exception as e:
        print("Error producing to Kafka:", e)

def main():
    topic = "wikimedia.recentchange"
    for change in fetch_wikimedia_changes():
        try:
            produce_to_kafka(topic, change)
        except Exception as e:
            print("Error processing change:", e)

if __name__ == "__main__":
    main()
