from bs4 import BeautifulSoup
from src.scraper_base import ScraperBase
import requests
import subprocess
import re
import json
import string
from src.values import TimeValue,QuantityValue
from src.entry import Entry

# ATTENTION this also updates scraper 13 (artist)

class Scraper12(ScraperBase):

	"""FNAC
	"""
	def __init__(self):
		super().__init__(12)
		self.person_scraper_id = 13
		self.person_entry_cache = []

	def scrape_everything(self):
		self.scrape_everything_via_index()

	def paginate_index(self):
		pageSize = 1000
		so_far = 0
		totalcount = pageSize # dummy, will be replaced on first URL load
		while so_far<totalcount:
			try:
				url = f"https://api.navigart.fr/14/artworks?&size={pageSize}&from={so_far}"
				print(url)
				page = requests.get(url)
				j = json.loads(page.text)
				yield j
				totalcount = j['totalCount']
				so_far += pageSize
			except Exception as err:
				print(f"Unexpected {err}")

	def parse_index_page(self,j):
		for result in j['results']:
			try:
				if "_source" in result and "ua" in result["_source"]:
					for entry in self.process_ua(result["_source"]["ua"]):
						yield entry
			except Exception as err:
				print(f"Unexpected {err}")

	def process_ua(self,ua):
		artist_ids = []
		if "authors" in ua:
			for artist in ua["authors"]:
				for entry in self.process_artist(artist):
					artist_ids.append(entry.id)
					yield entry
		if "artwork" in ua:
			for entry in self.process_artwork(ua["artwork"]):
				for artist_id in artist_ids:
					entry.add_scraper_item(170,self.person_scraper_id,artist_id)
				yield entry

	def process_artwork(self,artwork):
		entry = Entry(self.scraper_id)
		entry.id = artwork['_id']
		url = self.construct_entry_url_from_id(entry.id)
		entry.add_label_etc(url,"url","en")
		entry.add_label_etc(artwork['title_list'],"original_label",self.language)
		entry.add_label_etc(artwork['title_list'],"label",self.language)
		entry.add_label_etc(artwork['title_list'],"label","en")
		entry.add_item("P31","Q838948") # artwork

		if "date_creation" in artwork:
			m = re.match(r"^(\d{3,}).*$",artwork["date_creation"])
			if m is not None:
				entry.add_time(571,TimeValue(ymd=(int(m.group(1)), 1, 1), precision=9))

		self.add_dimensions(entry,artwork['dimensions'])
		if 'inventory' in artwork:
			entry.add_string(217,artwork['inventory'])

		if "medias" in artwork and len(artwork["medias"])>0:
			medium = artwork["medias"][0]
			url = f"https://images.navigart.fr/{medium['max_width']}/{medium['file_name']}"
			entry.add_string(6500,url)

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


	def process_artist(self,artist):
		if "_id" not in artist:
			return
		artist_id = artist["_id"]
		if artist_id is None or artist_id in self.person_entry_cache:
			return
		self.person_entry_cache.append(artist_id)

		entry = Entry(self.person_scraper_id)
		entry.id = artist_id

		if "name" in artist:
			if "list" in artist["name"]:
				list_name = artist["name"]["list"]
				entry.add_label_etc(f"https://www.navigart.fr/fnac/artworks/authors/{list_name}%E2%86%B9{list_name}","url","en")
				entry.add_label_etc(list_name,"original_label","en")
				nice_name = re.sub(r"^(\S+) (.+)",r"\2 \1",list_name)
				entry.add_label_etc(nice_name,"label","en")

		if "gender" in artist:
			if artist["gender"]=="masculin":
				entry.add_item("P21","Q6581097")
			elif artist["gender"]=="feminin":
				entry.add_item("P21","Q6581072")
			else:
				entry.add_freetext(21,artist['gender'])
		yield entry
