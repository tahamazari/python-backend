from flask import Flask, request, jsonify
from flask_cors import CORS
from main import getFilteredDataMultiple

app = Flask(__name__)
CORS(app)

@app.route('/process', methods=['POST'])
def process_data():
    data = request.json  # Parse the JSON data from the request body

    # Pass the entire data to getFilteredData
    response = getFilteredDataMultiple(data)

    # Return the processed data as JSON
    return jsonify(response)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
