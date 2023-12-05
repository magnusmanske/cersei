#!/usr/bin/env python3

import sys
import importlib
import json # For test123
import toolforge  # For oneoff TESTING FIXME
from src.tooldatabase import ToolDatabase # For oneoff TESTING FIXME
from src.entry import Entry # For oneoff TESTING FIXME

def get_scraper_from_id(scraper_id):
	try:
		module_name = "src.scrapers.scraper_"+str(scraper_id)
		class_name = "Scraper"+str(scraper_id)
		module = importlib.import_module(module_name)
		class_ = getattr(module, class_name)
		scraper = class_()
		return scraper
	except:
		print(f"No or broken scraper found for #{scraper_id}")
		exit(0)


if __name__ == "__main__":
	if sys.argv[1] in['run','test']:
		scraper = get_scraper_from_id(sys.argv[2])
		if scraper.is_scraper_running():
			print ("Scraper appears to be running")
			exit(0)
		if sys.argv[1]=="run":
			scraper.get_db().add_log("begin_scrape",scraper.scraper_id)
		try:
			scraper.scrape_everything()
		except:
			pass
		if sys.argv[1]=="run":
			scraper.get_db().add_log("end_scrape",scraper.scraper_id)
	elif sys.argv[1] == 'new':
		scraper = get_scraper_from_id(sys.argv[2])
		if scraper.is_scraper_running():
			print ("Scraper appears to be running")
			exit(0)
		scraper.get_db().add_log("begin_scrape_new",scraper.scraper_id)
		try:
			scraper.scrape_new_entries()
		except:
			pass
		scraper.get_db().add_log("end_scrape_new",scraper.scraper_id)
	elif sys.argv[1] == 'freetext2items':
		scraper = get_scraper_from_id(sys.argv[2])
		scraper.text2item_heuristic()
	elif sys.argv[1] == 'clear_scraper_history':
		scraper = get_scraper_from_id(sys.argv[2])
		scraper.get_db().add_log("clear_old_revisions",scraper.scraper_id)
		scraper.clear_old_revisions()
	elif sys.argv[1] == 'update_from_wikidata':
		try:
			scraper = get_scraper_from_id(sys.argv[2])
			scraper.update_from_wikidata()
		except Exception as err:
			print(f"Error: {err}")
	elif sys.argv[1] == 'fill_missing_revision_items':
		db = ToolDatabase()
		with db.connection.cursor() as cursor:
			sql = "SELECT id,scraper_id,current_revision_id FROM entry WHERE current_revision_id NOT IN (SELECT revision_id FROM revision_item)"
			cursor.execute(sql, [])
			rows = cursor.fetchall()
		for row in rows:
			scraper_id = row[1]
			revision_id = row[2]
			entry = Entry(scraper_id)
			entry.id = row[0]
			entry.load_from_revision(db,revision_id)
			json = entry.as_json(True)
			db.set_revision_item(revision_id,json)
	elif sys.argv[1] == 'test123':
		import pymysql
		db = ToolDatabase()
		# s = """{"offset":0,"scraper_id":1,"links":[["P31","Q5"]]}"""
		# j = json.loads(s)
		#ret = db.query_entries(j)
		#ret = db.query_scrapers()
		#ret = db.get_entities([123,456])
		#print(json.dumps(ret, indent=4, sort_keys=True))
		scraper_id = 3
		start = 0
		limit = 50
		sql = f"SELECT * FROM vw_entry_wide WHERE scraper_id={scraper_id} LIMIT {limit} OFFSET {start}"
		db = ToolDatabase()
		with db.connection.cursor(pymysql.cursors.DictCursor) as cursor:
		    cursor.execute(sql, [])
		    db.connection.commit()
		    field_names = list([i[0] for i in cursor.description])
		    rows = []
		    for result in cursor.fetchall():
		        row = []
		        for col in field_names:
		            v = db.column_value_pretty(result[col])
		            row.append(v)
		        rows.append(row)


""" Last scraper runtimes
SELECT
scraper.*,
@start := (SELECT `timestamp` FROM event_log WHERE event_type LIKE "begin_scrape%" AND relevant_id=scraper.id ORDER BY `timestamp` DESC LIMIT 1) AS start_time,
@end :=(SELECT `timestamp` FROM event_log WHERE event_type LIKE "end_scrape%" AND relevant_id=scraper.id ORDER BY `timestamp` DESC LIMIT 1) AS end_time,
TIMESTAMPDIFF(SECOND,@start,@end) AS runtime_seconds
FROM scraper
"""