from flask import Flask, render_template, jsonify
from scripts.kafka_consumer2 import processed_data_list

app = Flask(__name__)


@app.route('/dashboard')
def dashboard():
    # You can pass the processed_data_list directly to the template
    return render_template('dashboard.html', data=processed_data_list)


if __name__ == '__main__':
    app.run(debug=True)
