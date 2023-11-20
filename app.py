from flask import Flask, render_template
from scripts.kafka_consumer2 import result_dict

app = Flask(__name__)


@app.route('/display_data')
def display_data():
    return render_template('dashboard.html', result_dict=result_dict)


if __name__ == '__main__':
    app.run(debug=False)
