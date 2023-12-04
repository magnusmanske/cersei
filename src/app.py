#!/usr/bin/env python3

from flask import Flask, request, Response, jsonify, send_from_directory
import json
import re
from tooldatabase import ToolDatabase
from dotenv import load_dotenv

dotenv_path = '/data/project/cersei/www/python/.env'
load_dotenv(dotenv_path)

app = Flask(__name__)
app.config["FLASK_ENV"] = "development"


@app.route('/<path:path>')
def send_path(path):
    return send_from_directory('/data/project/cersei/scripts/cersei/web/www', path)


@app.route('/')
def main_page():
    return send_path('index.html')

@app.route('/api/scrapers')
def query_scrapers():
    try:
        db = ToolDatabase()
        scrapers = db.query_scrapers()
        data = { "status":"OK", "scrapers":scrapers }
        return jsonify(data)
    except Exception as err:
        return jsonify({'status': f"Unexpected {err=}, {type(err)=}"})

@app.route('/api/get_entries/<conditions>')
def query_get_entries(conditions):
    try:
        conditions = json.loads(conditions)
        db = ToolDatabase()
        entries = db.query_entries(conditions)
        data = { "status":"OK", "conditions":conditions , "entries":entries }
        return jsonify(data)
    except Exception as err:
        return jsonify({'status': f"Unexpected {err=}, {type(err)=}"})

@app.route('/api.php')
def query_api_php():
    action = request.args.get('action', default='', type=str)
    if action=='wbgetentities':
        ids = []
        for entry_id in request.args.get('ids', default='', type=str).split(','):
            if entry_id.strip()!='':
                numeric_id = int(re.sub(r"\D",'',entry_id))
                if numeric_id>0:
                    ids.append(numeric_id)
        if len(ids)==0:
            return jsonify({'success':0, 'error': 'No ids given'})
        db = ToolDatabase()
        entities = db.get_entities(ids)
        print(entities)
        return jsonify({ 'success':1 , 'entities': entities})
    return jsonify({'success':0, 'error': f"Unknown action '{action}'"})

if __name__ == '__main__':
    app.run(debug=True)
