import json
import pandas as pd
from pykafka import KafkaClient
import joblib
from sklearn.preprocessing import StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.exceptions import NotFittedError

# Load the model from the pickle file
model_path = "/Users/aminaterbachi/PycharmProjects/scientificProject/notebooks/best_churn_prediction_model.pkl"
ml_model = joblib.load(model_path)

# Define initial_data for fitting the ColumnTransformer
initial_data = {
    'Age': [37],
    'Total_Purchase': [9935.53],
    'Account_Manager': [1],
    'Years': [7.71],
    'Num_Sites': [8]
}

# Initialize the ColumnTransformer with a passthrough step
preprocessor = ColumnTransformer(
    transformers=[
        ('passthrough', StandardScaler(), ['Age', 'Total_Purchase', 'Account_Manager', 'Years', 'Num_Sites'])],
    remainder='drop'
)

# Initialize the pipeline
pipeline = Pipeline([
    ('preprocessor', preprocessor),
    ('classifier', ml_model)  # Assuming ml_model is your classifier
])

# Fit the ColumnTransformer with initial data before entering the loop
try:
    pipeline.named_steps['preprocessor'].fit(
        pd.DataFrame(initial_data, columns=['Age', 'Total_Purchase', 'Account_Manager', 'Years', 'Num_Sites']))
except NotFittedError:
    pass


def preprocess_input(features):
    # Assuming features is a list of dictionaries where each dictionary represents a feature vector
    feature_list = [{k: float(v) if k != 'Names' else v for k, v in feature.items()} for feature in features]

    # Create a DataFrame for proper preprocessing
    features_df = pd.DataFrame(feature_list,
                               columns=['Names', 'Age', 'Total_Purchase', 'Account_Manager', 'Years', 'Num_Sites'])

    try:
        # Transform the input features using the preprocessor
        preprocessed_features = pipeline.named_steps['preprocessor'].transform(features_df)
    except NotFittedError:
        # If the preprocessor is not fitted, fit it first
        preprocessed_features = pipeline.named_steps['preprocessor'].fit_transform(features_df)

    return preprocessed_features


def consume_and_predict(topic_name, kafka_bootstrap_servers):
    client = KafkaClient(hosts=kafka_bootstrap_servers)
    topic = client.topics[topic_name]
    consumer = topic.get_simple_consumer()

    try:
        for message in consumer:
            if message is not None and message.value:
                try:
                    # Try to decode the message and load it as a JSON object
                    message_value = json.loads(message.value.decode('utf-8'))

                    # Check if the loaded value is a JSON object
                    if isinstance(message_value, dict):
                        # Convert string values to appropriate data types
                        feature_vector = {
                            'Age': float(message_value.get('Age', 0.0)),
                            'Total_Purchase': float(message_value.get('Total_Purchase', 0.0)),
                            'Account_Manager': float(message_value.get('Account_Manager', 0.0)),
                            'Years': float(message_value.get('Years', 0.0)),
                            'Num_Sites': float(message_value.get('Num_Sites', 0.0))
                        }

                        # Ensure DataFrame has the necessary columns before transformation
                        input_df = pd.DataFrame([feature_vector])

                        # Preprocess the input features
                        preprocessed_features = pipeline.named_steps['preprocessor'].transform(input_df)

                        # Perform prediction using the machine learning model
                        prediction = pipeline.named_steps['classifier'].predict(preprocessed_features)

                        # Print the prediction result along with 'Names'
                        print(f"Real-time Prediction for {message_value.get('Names')}: {prediction}")
                    else:
                        print(f"Non-JSON message: {message.value.decode('utf-8')}")

                except json.decoder.JSONDecodeError:
                    print(f"Invalid JSON message: {message.value.decode('utf-8')}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.stop()


# Example usage
topic_name = "tutorial"  # Update with your actual Kafka topic name
kafka_bootstrap_servers = "127.0.0.1:9092"  # Update with your actual Kafka bootstrap servers

consume_and_predict(topic_name, kafka_bootstrap_servers)
