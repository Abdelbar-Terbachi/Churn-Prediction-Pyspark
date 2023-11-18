# kafka_producer.py
from confluent_kafka import Producer
import pandas as pd

# Load 'new_customers.csv'
df = pd.read_csv('new_customers.csv')

# Kafka configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'customer_churn'

# Create Kafka Producer
producer_conf = {'bootstrap.servers': bootstrap_servers}
producer = Producer(producer_conf)

# Produce data to Kafka
for _, row in df.iterrows():
    # Convert each row to JSON and send to Kafka
    message = row.to_json().encode('utf-8')
    producer.produce(topic_name, message)

# Close the producer
producer.flush()
