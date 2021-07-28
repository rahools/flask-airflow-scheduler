from flask import Flask, request
import requests
import json

app = Flask(__name__)
app.config["DEBUG"] = True

@app.route('/', methods=['GET'])
def index():
    example = '''
        {
        "schedule_id": "your-tag-id-1",
        "schedule_interval": "@daily",
        "namespace": "sub-directory-name-1",
        "template": "mongoToPostgres.template",
        "chunk_size": "20",
        "mongo_query": "{\"country\": \"Canada\"}"
        }
    '''

    return 'Hello! Here\'s an example: ' + example

@app.route('/scheduler', methods=['POST'])
def schedule():
    data = request.get_json(force=True)

    print(data)

    url = "http://localhost:8080/api/v1/dags/dynamic-dag-creator/dagRuns"
    requestData = {
        "conf": {
            "schedule_id": f"{data['schedule_id']}",
            "schedule_interval": "@daily",
            "namespace": "mongo-postgresql",
            "template": "mongoToPostgres.template",
            "chunk_size": f"{data['chunk_size']}",
            "mongo_query": f"{data['mongo_query']}",
        }
    }
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
    }
    auth = ('admin', 'admin')
    print(json.dumps(requestData))
    response = requests.post(url, data = json.dumps(requestData), headers = headers, auth = auth)

    return response.text

if __name__ == '__main__':
    app.run(host = '0.0.0.0', port = 5000)