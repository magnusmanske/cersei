from src.scraper_base import ScraperBase
from bs4 import BeautifulSoup
import requests
import re
import string
import json
from src.values import TimeValue
from src.entry import Entry

class Scraper18(ScraperBase):

	"""Theapolis
	"""
	def __init__(self):
		super().__init__(18)

	def scrape_everything(self):
		self.scrape_everything_via_index()

	def paginate_index(self):
		pageNr = 1
		perPage = 1000
		while True:
			try:
				url = f"https://www.theapolis.de/api/de/profiles?page={pageNr}&items={perPage}&search=&status=1&commercial=&organization="
				print (url)
				page = requests.get(url)
				j = json.loads(page.text)
				yield j
				if (j['data']['currentPageNumber']+1)*perPage-1 > j['data']['totalCount']:
					break
			except Exception as err:
				print(f"Unexpected {err}")
			pageNr += 1

	def parse_index_page(self,j):
		for item in j['data']['items']:
			try:
				for entry in self.process_author(item):
					yield entry
			except Exception as err:
				print(f"parse_index_page: {err}")

	def process_author(self,item):
		entry = Entry(self.scraper_id)
		entry.id = item['slug']
		entry.add_item("P31","Q5")

		url = f"https://www.theapolis.de/de/profil/{entry.id}"
		entry.add_label_etc(url,"url","en")

		entry.add_label_etc(item['name'],"original_label",self.language)
		entry.add_label_etc(item['name'],"label",self.language)
		entry.add_label_etc(item['undertitle'],"description",self.language)

		if 'birthDate' in item and item['birthDate']!='':
			m = re.match(r'^(\d{4})-(\d{2})-(\d{2})',item['birthDate'])
			if m is not None:
				tv = TimeValue(ymd=(int(m.group(1)), int(m.group(2)), int(m.group(3))), precision=11)
				entry.add_time("P569",tv)

		entry.add_freetext(19,item['birthPlace'])
		if item['nationality']!='EU' and item['nationality']!='nicht EU':
			entry.add_freetext(27,item['nationality'])

		yield entry
