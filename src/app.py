#!/usr/bin/env python3

from flask import Flask, request, Response, jsonify
import json
from tooldatabase import ToolDatabase
from dotenv import load_dotenv

dotenv_path = '/data/project/cersei/www/python/.env'
load_dotenv(dotenv_path)

app = Flask(__name__)
app.config["FLASK_ENV"] = "development"

@app.route('/')
def home():
    return Response("""
<h1>Central External Resource Scraping and Extraction Infrastructure</h1>
<p>A tool to scrape external resources, extract data in a Wikidata-compatible format, and allow comparison with matching Wikidata items.</p>
<p>
<h2>API examples</h2>
<ul>
<li>
    <a href='/api/get_entries/{"offset":0,"scraper_id":1,"links":[["P31","Q5"]]}'>First 50 humans (Q5) for scraper #1</a><br/>
    <i>Note:</i> The entries will have an "entry" field that is almost identical to a Wikidata item JSON.
    Key differences include no internal IDs (statements, refernces etc), and the presence of a "freetext" field.
</li>
</ul>
</p>
<p>Source: <a href="https://github.com/magnusmanske/cersei/">GitHub</a></p>
    """.strip(), content_type='text/html')

@app.route('/api/get_entries/<conditions>')
def query(conditions):
    try:
        conditions = json.loads(conditions)
        db = ToolDatabase()
        entries = db.query_entries(conditions)
        data = { "status":"OK", "conditions":conditions , "entries":entries }
        return jsonify(data)
    except Exception as err:
        return jsonify({'status': f"Unexpected {err=}, {type(err)=}"})


if __name__ == '__main__':
    app.run(debug=True)
