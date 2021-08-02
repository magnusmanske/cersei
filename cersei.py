#!/usr/bin/env python3

import sys
import importlib

def get_scraper_from_id(scraper_id):
	module_name = "src.scraper_"+str(scraper_id)
	class_name = "Scraper"+str(scraper_id)
	module = importlib.import_module(module_name)
	class_ = getattr(module, class_name)
	scraper = class_()
	return scraper


if __name__ == "__main__":
	if sys.argv[1] == 'run':
		scraper = get_scraper_from_id(sys.argv[2])
		scraper.scrape_everything_via_index()
	elif sys.argv[1] == 'freetext2items':
		scraper = get_scraper_from_id(sys.argv[2])
		scraper.text2item_heuristic()
	elif sys.argv[1] == 'clear_scraper_history':
		scraper = get_scraper_from_id(sys.argv[2])
		scraper.clear_old_revisions()
