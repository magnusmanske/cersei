#!/usr/bin/env python3

import sys
import importlib
from src.scraper_base import DummyScraper
from src.artworks import Artworks
import json # For test123
import toolforge  # For oneoff TESTING FIXME
from src.tooldatabase import ToolDatabase # For oneoff TESTING FIXME
from src.entry import Entry # For oneoff TESTING FIXME

def get_scraper_from_id(scraper_id, allow_dummy=False):
	try:
		module_name = "src.scrapers.scraper_"+str(scraper_id)
		class_name = "Scraper"+str(scraper_id)
		module = importlib.import_module(module_name)
		class_ = getattr(module, class_name)
		scraper = class_()
		return scraper
	except:
		if allow_dummy:
			scraper = DummyScraper(scraper_id)
			return scraper
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
		scraper = get_scraper_from_id(sys.argv[2],allow_dummy=True)
		scraper.text2item_heuristic()
	elif sys.argv[1] == 'clear_scraper_history':
		scraper = get_scraper_from_id(sys.argv[2],allow_dummy=True)
		scraper.get_db().add_log("clear_old_revisions",scraper.scraper_id)
		scraper.clear_old_revisions()
	elif sys.argv[1] == 'update_from_wikidata':
		try:
			scraper = get_scraper_from_id(sys.argv[2],allow_dummy=True)
			scraper.update_from_wikidata()
		except Exception as err:
			print(f"Error: {err}")
	elif sys.argv[1] == 'artwork2qs':
		artworks = Artworks()
		artworks.verbose = False
		qs_commands = artworks.generate_qs()
		print (qs_commands)
		#artworks.run_qs(qs_commands)
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
		db = ToolDatabase()
		entry = Entry(12)
		entry.load_from_revision(db, 647083)
		print (entry)
	else:
		print (f"Unknown action {sys.argv[1]}")



""" Last scraper runtimes
SELECT
scraper.*,
@start := (SELECT `timestamp` FROM event_log WHERE event_type LIKE "begin_scrape%" AND relevant_id=scraper.id ORDER BY `timestamp` DESC LIMIT 1) AS start_time,
@end :=(SELECT `timestamp` FROM event_log WHERE event_type LIKE "end_scrape%" AND relevant_id=scraper.id ORDER BY `timestamp` DESC LIMIT 1) AS end_time,
TIMESTAMPDIFF(SECOND,@start,@end) AS runtime_seconds
FROM scraper
"""