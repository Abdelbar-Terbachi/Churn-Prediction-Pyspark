# kafka_consumer.py
from confluent_kafka import Consumer, KafkaException, KafkaError
from pyspark.ml import PipelineModel
from flask_socketio import SocketIO
import json
import threading

# Load the Spark ML model
model_path = "models/churn_prediction_model"
spark_ml_model = PipelineModel.load(model_path)

# Kafka configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'customer_churn'


# Function to consume data from Kafka topic and predict in real-time
def consume_from_kafka_streams(socketio):
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

            try:
                # Try to process the consumed message (print it in this example)
                consumed_data = json.loads(msg.value().decode('utf-8'))

                # Make predictions using the loaded Spark ML model
                predictions = spark_ml_model.transform([consumed_data])

                # Extract the prediction result
                prediction_result = predictions.select("prediction").collect()[0]["prediction"]

                # Send prediction to the frontend via Socket.IO
                socketio.emit('prediction_response', {'prediction': prediction_result})

            except Exception as e:
                print(f"Error processing message: {e}")
                # Handle the error (e.g., log it, emit an error message to the frontend)

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()


if __name__ == '__main__':
    # Create Flask app and SocketIO instance
    from flask import Flask

    app = Flask(__name__)
    socketio = SocketIO(app)

    # Start Kafka data consumption and prediction in a separate thread
    kafka_thread = threading.Thread(target=consume_from_kafka_streams, args=(socketio,))
    kafka_thread.start()

    # Run the Flask app
    socketio.run(app, debug=True)
