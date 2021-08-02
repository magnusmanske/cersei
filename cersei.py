#!/usr/bin/env python3

import sys
import importlib

if __name__ == "__main__":
	if sys.argv[1] == 'run':
		module_name = "src.scraper_"+str(sys.argv[2])
		class_name = "Scraper"+str(sys.argv[2])
		module = importlib.import_module(module_name)
		class_ = getattr(module, class_name)
		scraper = class_()
		for html in scraper.paginate_index():
			for o in scraper.parse_index_page(html):
				o.create_or_update_in_database(scraper.get_db())
			exit(0)