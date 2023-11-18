from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
import csv
import json

# Kafka configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'customer_churn'


# Function to produce data to Kafka topic
def produce_to_kafka(file_path):
    producer_conf = {
        'bootstrap.servers': bootstrap_servers
    }

    producer = Producer(producer_conf)

    try:
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                message_value = json.dumps(row)
                producer.produce(topic=topic_name, value=message_value)

        producer.flush()
        print("Data produced to Kafka topic successfully.")

    except Exception as e:
        print(f"Error producing data to Kafka topic: {e}")


# Function to consume data from Kafka topic using Kafka Streams
def consume_from_kafka_streams():
    consumer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'my_consumer_group',
        'auto.offset.reset': 'earliest'  # Start reading from the beginning of the topic
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic_name])

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                elif msg.error():
                    raise KafkaException(msg.error())

            # Process the consumed message (print it in this example)
            print(f"Consumed message: {msg.value().decode('utf-8')}")

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()


# Create Kafka topic if it doesn't exist
admin_conf = {'bootstrap.servers': bootstrap_servers}
admin_client = AdminClient(admin_conf)
topics = [NewTopic(topic=topic_name, num_partitions=1, replication_factor=1)]
admin_client.create_topics(topics)

# Produce data to Kafka topic
produce_to_kafka('customer_churn.csv')

# Consume data from Kafka topic using Kafka Streams
consume_from_kafka_streams()
