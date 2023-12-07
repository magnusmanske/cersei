from bs4 import BeautifulSoup
from src.scraper_base import ScraperBase
import requests
import subprocess
import re
import json
import gzip
from src.values import TimeValue
from src.entry import Entry

# ATTENTION this also updates scraper 8 (artist) and 9 (museum)

class Scraper7(ScraperBase):

	"""Kunstdatenbank
	"""
	def __init__(self):
		super().__init__(7)
		self.person_scraper_id = 8
		self.museum_scraper_id = 9
		self.person_entry_cache = ['unknown'] # Skip some "generic" ones
		self.museum_entry_cache = []

	def scrape_everything(self):
		self.scrape_everything_via_index()

	def entry_url_relative2full(self,url):
		return f"https://www.kunstdatenbank.at/{url}"

	def paginate_index(self):
		pageNr = 1
		while True:
			try:
				url = f"https://www.kunstdatenbank.at/search-for-objects?page={pageNr}"
				print(url)
				page = requests.get(url)
				html = page.text
				yield html
				soup = BeautifulSoup(html,features="html.parser")
				if len(soup.find_all('a', class_='next'))==0:
					break # No "next" button, quit
			except Exception as err:
				print(f"Unexpected {err}")
			pageNr += 1

	def parse_index_page(self,html):
		soup = BeautifulSoup(html,features="html.parser")
		for table in soup.find_all('table', class_="resulttable"):
			for tbody in table.find_all('tbody'):
				for tr in tbody.find_all('tr'):
					tds = tr.find_all('td')
					if len(tds)>=2:
						for a in tds[1].find_all('a'):
							m = re.match(r"^.*detail-view-object.(\d+)$",a['href'])
							if m is not None:
								try:
									artwork_id = m.group(1)
									for entry in self.process_artwork(artwork_id):
										yield entry
								except Exception as e:
									print(f"Huh: {e}")
									pass

	def process_artwork(self,artwork_id):
		entry = Entry(self.scraper_id)
		entry.id = artwork_id
		url = self.construct_entry_url_from_id(entry.id)
		entry.add_label_etc(url,"url","en")
		page = requests.get(url)
		html = page.text
		soup = BeautifulSoup(html,features="html.parser")

		# Label
		for h1 in soup.find_all('h1'):
			entry.add_label_etc(h1.text,"label","de")

		# Non-free image(s)
		for div in soup.find_all('div', class_='div_picture'):
			for a in div.find_all('a'):
				entry.add_string(6500,self.entry_url_relative2full(a['href']))

		for h3 in soup.find_all('h3'):
			value = h3.find_parent('div').find_next_sibling('div')
			if h3.string=='Artist':
				for a in value.find_all('a'):
					m = re.match(r"^search-page/artist/(.+)$",a['href'])
					if m is not None:
						artist_id = self.unquote(m.group(1))
						entry.add_scraper_item(170,self.person_scraper_id,artist_id)
						artist_entry = self.process_artist(artist_id,a.text)
						if artist_entry is not None:
							yield artist_entry
			elif h3.string=='Museum':
				for a in value.find_all('a'):
					m = re.match(r"^/detail-view-museum/(.+)$",a['href'])
					if m is not None:
						museum_id = self.unquote(m.group(1))
						entry.add_scraper_item(276,self.museum_scraper_id,museum_id)
						museum_entry = self.process_museum(museum_id)
						if museum_entry is not None:
							yield museum_entry
			elif h3.string=='Inventory Number':
				entry.add_string(217,value.string)
			elif h3.string=='Description':
				entry.add_label_etc(value.text.strip(),"description","en")
			elif h3.string=='Material':
				entry.add_freetext(186,value.text.strip())
		yield entry

	def process_museum(self,museum_id):
		if museum_id in self.museum_entry_cache:
			return
		self.museum_entry_cache.append(museum_id)
		entry = Entry(self.museum_scraper_id)
		entry.id = museum_id
		url = f"https://www.kunstdatenbank.at/detail-view-museum/{entry.id}"
		entry.add_label_etc(url,"url","en")
		entry.add_item("P31","Q33506") # museum

		page = requests.get(url)
		html = page.text
		soup = BeautifulSoup(html,features="html.parser")

		# Label
		for h1 in soup.find_all('h1'):
			entry.add_label_etc(h1.text,"label","en")

		# Misc
		for h3 in soup.find_all('h3'):
			value = h3.find_parent('div').find_next_sibling('div')
			if h3.string=='Telephone':
				entry.add_string(1329,value.text.strip())
			elif h3.string=='Website':
				entry.add_string(856,value.text.strip())
			elif h3.string=='E-Mail':
				entry.add_string(968,value.text.strip())
			elif h3.string=='Address':
				entry.add_string(6375,value.text.strip().replace("\n",", ").replace("\r",''))

		return entry

	def process_artist(self,artist_id,name):
		if artist_id in self.person_entry_cache:
			return
		self.person_entry_cache.append(artist_id)
		entry = Entry(self.person_scraper_id)
		entry.id = artist_id
		url = f"https://www.kunstdatenbank.at/search-page/artist/{entry.id}"
		entry.add_label_etc(url,"url","en")
		entry.add_item("P31","Q5") # human
		entry.add_label_etc(name,"original_label","en")

		desc = ''
		m = re.match(r"^(.+?) *(\(.*)$",name)
		if m is not None:
			desc = m.group(2).strip()
			name = m.group(1).strip()
		m = re.match(r"^(.+?), (.+)$",name)
		if m is not None:
			name = m.group(2).strip()+' '+m.group(1).strip()
		if desc!='':
			entry.add_label_etc(desc,"description","en")
		entry.add_label_etc(name,"label","en")

		m = re.match(r"^\((\d{4})\s*-\s*(\d{4})\).*$",desc)
		if m is not None:
			entry.add_time(569,TimeValue(ymd=(int(m.group(1)), 1, 1), precision=9))
			entry.add_time(570,TimeValue(ymd=(int(m.group(2)), 1, 1), precision=9))
		return entry
