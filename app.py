from flask import Flask, render_template

app = Flask(__name__)

# Sample data list
data_list = [
    {'Name': 'John Doe', 'Age': 25, 'City': 'New York'},
    {'Name': 'Jane Smith', 'Age': 30, 'City': 'San Francisco'},
    {'Name': 'Bob Johnson', 'Age': 22, 'City': 'Los Angeles'}
]


@app.route('/display_data')
def display_data():
    return render_template('dashboard.html', data_list=data_list)


if __name__ == '__main__':
    app.run(debug=True)
