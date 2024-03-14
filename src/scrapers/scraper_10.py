from bs4 import BeautifulSoup
from src.scraper_base import ScraperBase
import requests
import subprocess
import re
import json
import string
from src.values import TimeValue,QuantityValue
from src.entry import Entry

# ATTENTION this also updates scraper 11 (artist)

class Scraper10(ScraperBase):

	"""National Gallery of Art
	"""
	def __init__(self):
		super().__init__(10)
		self.person_scraper_id = 11
		self.person_entry_cache = []

	def scrape_everything(self):
		self.scrape_everything_via_index()

	def entry_url_relative2full(self,url):
		return f"https://www.nga.gov{url}"

	def paginate_index(self):
		for letter in string.ascii_lowercase:
			pageNr = 0
			pageSize = 90
			so_far = 0
			totalcount = pageSize # dummy, will be replaced on first URL load
			while so_far<totalcount:
				try:
					url = f"https://www.nga.gov/bin/ngaweb/collection-search-result/search.pageSize__{pageSize}.pageNumber__{pageNr}.json?artist={letter}"
					print(url)
					page = requests.get(url, timeout=60)
					j = json.loads(page.text)
					yield j
					totalcount = j['totalcount']
					so_far += pageSize
				except Exception as err:
					print(f"Unexpected {err}")
				pageNr += 1

	def parse_index_page(self,j):
		for result in j['results']:
			for entry in self.process_artwork(result):
				yield entry

	def process_artwork(self,artwork):
		entry = Entry(self.scraper_id)
		entry.id = artwork['id']
		url = self.construct_entry_url_from_id(entry.id)
		entry.add_label_etc(url,"url","en")
		entry.add_label_etc(artwork['title'],"label","en")
		entry.add_label_etc(artwork['title'],"original_label","en")
		entry.add_item("P31","Q838948") # artwork
		entry.add_freetext(186,artwork['medium']) # Material
		entry.add_freetext(31,artwork['classification'])
		self.add_dimensions(entry,artwork['dimensions1'])
		self.add_dimensions(entry,artwork['dimensions2'])
		entry.add_freetext(793,artwork['creditline'])
		entry.add_string(217,artwork['accessionnumber'])
		for artist in artwork['artists']:
			try:
				url = artist['url']
				m = re.match(r"^/content/ngaweb/collection/artist-info\.(\d+)\.html$",url)
				if m is None:
					continue
				artist_id = m.group(1).strip()
				if artist['role'] is None:
					entry.add_scraper_item(170,self.person_scraper_id,artist_id)
				for entry_artist in self.process_artist(artist_id,artist):
					yield entry_artist
			except:
				print (f"Problem with {artist}")
		print (entry)
		yield entry

	def add_dimensions(self,entry,s):
		if s is None or s=='':
			return
		m = re.match(r"^.*?([0-9.]+)\s*x\s*([0-9.]+)\s*(mm|cm|m)\b.*$",s)
		if m is None:
			return
		
		width = self.parse_quantity(f"{m.group(1)} {m.group(3)}")
		if width is not None:
			entry.add_quantity(2049,width.amount,width.unit)

		height = self.parse_quantity(f"{m.group(2)} {m.group(3)}")
		if height is not None:
			entry.add_quantity(2048,height.amount,height.unit)


	def process_artist(self,artist_id,artist):
		if artist_id is None or artist_id in self.person_entry_cache:
			return
		self.person_entry_cache.append(artist_id)

		entry = Entry(self.person_scraper_id)
		entry.id = artist_id
		entry.add_label_etc(f"https://www.nga.gov{artist['url']}","url","en")
		if artist['role'] is None:
			entry.add_item("P31","Q5") # human
		entry.add_label_etc(artist['name'],"original_label","en")
		entry.add_label_etc(artist['forwardName'],"label","en")
		entry.add_label_etc(artist['displaydatecons'],"description","en")
		entry.add_freetext(27,artist['nationality'])

		# Lifespan
		if artist['lifespan'] is not None:
			m = re.match(r"^(\d+)\s*-.*$",artist['lifespan'])
			if m is not None:
				entry.add_time(569,TimeValue(ymd=(int(m.group(1)), 1, 1), precision=9))
			m = re.match(r"^.*?-\s*(\d+)$",artist['lifespan'])
			if m is not None:
				entry.add_time(570,TimeValue(ymd=(int(m.group(1)), 1, 1), precision=9))
		yield entry
