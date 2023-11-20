import csv
import json
from pykafka import KafkaClient


def send_messages_to_kafka(csvFilePath, topic_name, kafka_bootstrap_servers):
    # Open the csv reader called DictReader
    with open(csvFilePath, encoding='utf-8') as csvf:
        csvReader = csv.DictReader(csvf)

        # The Producer
        client = KafkaClient(hosts=kafka_bootstrap_servers)
        topic = client.topics[topic_name]

        # send messages synchronously
        with topic.get_sync_producer() as producer:
            # Convert each row into a dictionary
            for row in csvReader:
                # Send the JSON representation of the row as a message to the Kafka topic
                message = json.dumps(row).encode('utf-8')
                producer.produce(message)
                print(f"Sent message: {message}")


# The Kafka bootstrap servers and  topic information
kafka_bootstrap_servers = "127.0.0.1:9092"
topic_name = "churn"
csvFilePath = r'/Users/aminaterbachi/Downloads/new_customers.csv'

# The function Call
send_messages_to_kafka(csvFilePath, topic_name, kafka_bootstrap_servers)
