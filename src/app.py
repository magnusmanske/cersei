#!/usr/bin/env python3

from flask import Flask, request, Response
import json
from tooldatabase import ToolDatabase
from dotenv import load_dotenv

dotenv_path = '/data/project/cersei/www/python/.env'
load_dotenv(dotenv_path)

app = Flask(__name__)
app.config["FLASK_ENV"] = "development"

#whitelisted = ['project.query', 'maniphest.info']

@app.route('/')
def home():
    return Response("""
YAY!
    """.strip(), content_type='text/plain')


@app.route('/api/<action>')
def query(action):
    data = { "test":"it works!", "action":action}
    return json.dumps(data)
    """
    if action not in whitelisted:
        return json.dumps({'error': 'The %s action is not whitelisted' % action})

    params = request.form.get('data')
    if not params:
        return json.dumps({'error': 'No form data provided.'})
    data = phab.request(action, json.loads(params))
    return json.dumps(data)
    """

if __name__ == '__main__':
    app.run(debug=True)
