#!/usr/bin/env python3

import sys
import importlib
import toolforge  # For oneoff TESTING FIXME
from src.tooldatabase import ToolDatabase # For oneoff TESTING FIXME
from src.entry import Entry # For oneoff TESTING FIXME

def get_scraper_from_id(scraper_id):
	module_name = "src.scrapers.scraper_"+str(scraper_id)
	class_name = "Scraper"+str(scraper_id)
	module = importlib.import_module(module_name)
	class_ = getattr(module, class_name)
	scraper = class_()
	return scraper


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
		scraper = get_scraper_from_id(sys.argv[2])
		scraper.update_from_wikidata()
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


