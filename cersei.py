#!/usr/bin/env python3

from src.scraper_1 import Scraper1

if __name__ == "__main__":
	scraper = Scraper1()
	for html in scraper.paginate_index():
		for o in scraper.parse_index_page(html):
			print ("---")
			print (str(o))
			o.create_or_update_in_database(scraper.get_db())
		exit(0)
