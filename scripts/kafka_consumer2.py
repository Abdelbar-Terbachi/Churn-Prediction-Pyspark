from pykafka import KafkaClient
import json
import pickle
import pandas as pd


def consume_and_store_and_convert(topic_name, kafka_bootstrap_servers):
    client = KafkaClient(hosts=kafka_bootstrap_servers)
    topic = client.topics[topic_name]

    consumer = topic.get_simple_consumer(auto_offset_reset='earliest')

    # List to store consumed and processed data
    processed_data = []

    try:
        for message in consumer:
            if message is not None and message.value:
                # Decode the message and load it as a JSON object
                message_value = json.loads(message.value.decode('utf-8'))

                # Extract values and convert them to appropriate data types
                processed_values = [
                    message_value.get('Names', ''),
                    int(message_value.get('Age', 0)),
                    float(message_value.get('Total_Purchase', 0.0)),
                    int(message_value.get('Account_Manager', 0)),
                    float(message_value.get('Years', 0.0)),
                    int(message_value.get('Num_Sites', 0)),
                    message_value.get('Onboard_date', ''),
                    message_value.get('Location', ''),
                    message_value.get('Company', '')
                ]

                # Append the processed values to the list
                processed_data.append(processed_values)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.stop()

    return processed_data


# The Kafka Server amd Topic information
topic_name = "churn"
kafka_bootstrap_servers = "127.0.0.1:9092"

# Call the function to consume, process, and store data in a list
processed_data_list = consume_and_store_and_convert(topic_name, kafka_bootstrap_servers)

# Print the final processed data list
print(processed_data_list)


# Load the trained model using pickle
with open('/Users/aminaterbachi/PycharmProjects/scientificProject/notebooks/best_churn_prediction_model.pkl',
          'rb') as model_file:
    loaded_model = pickle.load(model_file)

# Your final data
final_data = processed_data_list
# Convert the final data to a pandas DataFrame
column_names = ['Names', 'Age', 'Total_Purchase', 'Account_Manager', 'Years', 'Num_Sites', 'Onboard_date', 'Location',
                'Company']
df = pd.DataFrame(final_data, columns=column_names)

# Preprocess each data point and make predictions
result_dict = {}

for index, data_point in df.iterrows():
    # Extract relevant features for prediction
    features_for_prediction = data_point[['Age', 'Total_Purchase', 'Account_Manager', 'Years', 'Num_Sites']]

    # Preprocess the data point
    preprocessed_data_point = loaded_model.named_steps['preprocessor'].transform(
        pd.DataFrame(features_for_prediction).T)

    # Make predictions using the preprocessed data
    prediction = loaded_model.named_steps['classifier'].predict(preprocessed_data_point)

    # Store data_point and prediction in the dictionary
    result_dict[str(data_point)] = int(prediction[0])  # Assuming prediction is a numpy array
