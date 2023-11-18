# app.py
from flask import Flask, request, jsonify
from pyspark.ml import PipelineModel

app = Flask(__name__)

# Load the Spark ML model
model_path = "models/churn_prediction_model"
spark_ml_model = PipelineModel.load(model_path)


@app.route('/predict', methods=['POST'])
def predict():
    try:
        # Get input data from the request
        data = request.get_json()

        # Assuming your input data is in the format expected by your model
        features = data['features']

        # Make predictions using the loaded Spark ML model
        predictions = spark_ml_model.transform(features)

        # Extract the prediction result (adjust this based on your model output)
        prediction_result = predictions.select("prediction").collect()[0]["prediction"]

        return jsonify({'prediction': prediction_result})

    except Exception as e:
        return jsonify({'error': str(e)})


if __name__ == '__main__':
    app.run(debug=True)
