# app.py
from flask import Flask, render_template
from flask_cors import CORS
from flask_socketio import SocketIO
from pyspark.ml import PipelineModel
from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import threading

app = Flask(__name__)
CORS(app)
socketio = SocketIO(app)

# Load the Spark ML model
model_path = "models/churn_prediction_model"
spark_ml_model = PipelineModel.load(model_path)

# Kafka configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'customer_churn'


# Function to consume data from Kafka topic and predict in real-time
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
            consumed_data = json.loads(msg.value().decode('utf-8'))

            # Make predictions using the loaded Spark ML model
            predictions = spark_ml_model.transform([consumed_data])

            # Extract the prediction result
            prediction_result = predictions.select("prediction").collect()[0]["prediction"]

            # Send prediction to the frontend via Socket.IO
            socketio.emit('prediction_response', {'prediction': prediction_result})

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()


# Start Kafka data consumption and prediction in a separate thread
kafka_thread = threading.Thread(target=consume_from_kafka_streams)
kafka_thread.start()


@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html')


if __name__ == '__main__':
    socketio.run(app, debug=True)
