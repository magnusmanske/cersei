from src.scraper_base import ScraperBase
from bs4 import BeautifulSoup
import requests
import re
import string
from src.values import TimeValue
from src.entry import Entry

class Scraper16(ScraperBase):

	"""Standard Ebooks
	"""
	def __init__(self):
		super().__init__(16)
		self.person_scraper_id = 17
		self.person_entry_cache = []

	def scrape_everything(self):
		self.scrape_everything_via_index()

	def paginate_index(self):
		pageNr = 1
		has_next = True
		while has_next:
			try:
				url = f"https://standardebooks.org/ebooks?page={pageNr}&per-page=48"
				print (url)
				page = requests.get(url)
				html = page.text
				yield html
				soup = BeautifulSoup(html,features="html.parser")
				has_next = False
				navs = soup.find_all('nav')
				for nav in navs:
					if nav.find('a',{"rel":"next"}) is not None:
						has_next = True
			except Exception as err:
				print(f"Unexpected {err}")
			pageNr += 1

	def entry_url_relative2full(self,url):
		return f"https://standardebooks.org{url}"

	def parse_index_page(self,html):
		soup = BeautifulSoup(html,features="html.parser")
		for li in soup.find_all('li', {"typeof":"schema:Book"}):
			entry = Entry(self.scraper_id)
			entry.id = re.sub(r'^/ebooks/','',li.get('about'))
			url = self.entry_url_relative2full(f"/ebooks/{entry.id}")
			entry.add_label_etc(url,"url","en")

			title = li.find('span',{'property':'schema:name'}).get_text()
			entry.add_label_etc(title,"original_label",self.language)
			entry.add_label_etc(title,"label",self.language)

			for author in li.find_all('p', class_='author'):
				for author_id,author_entry in self.process_author(author):
					if author_id is not None:
						entry.add_scraper_item(50,self.person_scraper_id,author_id)
					if author_entry is not None:
						yield author_entry

			yield entry

	def process_author(self,author):
		author_id = re.sub(r'^/ebooks/','',author.get('resource'))
		if author_id in self.person_entry_cache:
			yield author_id,None
			return
		self.person_entry_cache.append(author_id)

		entry = Entry(self.person_scraper_id)
		entry.id = author_id
		entry.add_item("P31","Q5")

		url = self.entry_url_relative2full(f"/ebooks/{entry.id}")
		entry.add_label_etc(url,"url","en")

		title = author.get_text()
		entry.add_label_etc(title,"original_label",self.language)
		entry.add_label_etc(title,"label",self.language)

		yield author_id,entry
