from src.scraper_base import ScraperBase
from bs4 import BeautifulSoup
import requests
import re
import datetime
from src.values import TimeValue
from src.entry import Entry

class Scraper2(ScraperBase):

	"""Columbia Encyclopedia
	"""
	def __init__(self):
		super().__init__(2)

	def scrape_everything(self):
		db = self.get_db()
		for url,html,soup in self.scrape_everything_via_follow([self.url]):
			try:
				entry = self.extract_from_entry_page(url,html,soup)
				if entry is not None:
					entry.create_or_update_in_database(db)
			except Exception as err:
				print(f"Unexpected {err}")
		self.text2item_heuristic()


	def links_to_follow(self,soup):
		return soup.find_all('a')

	def is_entry_page(self,url,html=None,soup=None):
		if re.match(r"^https://www.infoplease.com/encyclopedia/.+$",url):
			return True
		return False

	def get_entry_id_from_href(self,href):
		m = re.match(r"^.*\/encyclopedia\/(.+)$",href)
		if m:
			return m.group(1)

	def extract_from_entry_page(self,url,html,soup):
		m = re.match(r"^https://www.infoplease.com/encyclopedia/(.+)$",url)
		if not m: # No ID posible
			return
		entry = Entry(self.scraper_id)
		entry.id = m.group(1)
		entry.add_label_etc(url,"url",self.language)
		is_human = False
		is_article = False
		if entry.id.startswith("people/"):
			entry.add_item("P31","Q5")
			is_human = True
		for h1 in soup.find_all('h1', class_="page-title"):
			original_label = h1.get_text().strip()
			entry.add_label_etc(original_label,"original_label",self.language)
			label = original_label
			if is_human:
				label = re.sub(r'^(.+?), (.+)$',r'\2 \1',label)
			entry.add_label_etc(label,"label",self.language)
		for div in soup.find_all('div', class_="article-detail"):
			description = div.get_text().strip()
			if is_human and description.startswith(original_label):
				description = description[len(original_label):].lstrip(", ")
			if is_human:
				self.description2dates(description,entry)
			entry.add_label_etc(description,"description",self.language)
			is_article = True
		if is_article and entry.is_valid():
			return entry

	def description2dates(self,description,entry):
		m = re.match(r"^(\d{3,4})–",description)
		if m:
			year = int(m.group(1))
			tv = TimeValue(ymd=(year, 1, 1), precision=9)
			entry.add_time("P569",tv)
		m = re.match(r"^\d*–(\d{3,4})",description)
		if m:
			year = int(m.group(1))
			tv = TimeValue(ymd=(year, 1, 1), precision=9)
			entry.add_time("P570",tv)
