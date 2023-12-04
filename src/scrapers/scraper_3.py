from src.scraper_base import ScraperBase
from bs4 import BeautifulSoup
import requests
import re
import datetime
from src.values import TimeValue
from src.entry import Entry

class Scraper3(ScraperBase):

	"""Great Ukrainian Encyclopedia Online
	"""
	def __init__(self):
		super().__init__(3)
		self.date_patterns = [
			("%d.%m.%Y",11),
			("%Y",9),
		]

	def scrape_everything(self):
		db = self.get_db()
		for title,url in self.scrape_mediawiki_all(self.url.rstrip("/")+"/api.php"):
			try:
				entry = self.extract_from_entry_page(title,url)
				if entry is not None:
					entry.create_or_update_in_database(db)
			except Exception as err:
				print(f"Unexpected {err}")
		self.text2item_heuristic()

	def scrape_new_entries(self):
		db = self.get_db()
		for title,url in self.scrape_mediawiki_new(self.url.rstrip("/")+"/api.php"):
			try:
				entry = self.extract_from_entry_page(title,url)
				if entry is not None:
					print (str(entry))
					entry.create_or_update_in_database(db)
			except Exception as err:
				print(f"Unexpected {err}")
		self.text2item_heuristic()

	def extract_from_entry_page(self,title,url):
		if re.match(r"^[Jj]{3}:.*$",url): # Hardcoded BS filter
			return
		entry = Entry(self.scraper_id)
		entry.id = title.replace(" ","_")
		entry.add_label_etc(url,"url",self.language)
		entry.add_label_etc(title,"original_label",self.language)
		is_human = False
		if re.match(r"^\S+, .+$",title): # Hackish heuritic
			is_human = True

		# Load page
		page = requests.get(url)
		html = page.text
		soup = BeautifulSoup(html,features="html.parser")

		# Description
		for div in soup.find_all(id='mw-content-text'):
			description = div.get_text().replace("\n"," ").strip()[0:254]
			entry.add_label_etc(description,"description",self.language)

		# Human infobox
		for tr in soup.find_all("tr"):
			tds = []
			for td in tr.find_all("td"):
				tds.append(td.get_text().strip())
			if len(tds)!=2:
				continue
			if tds[0]=="Народження":
				is_human = True
				self.add_date_or_freetext("P569",tds[1],entry)
			if tds[0]=="Смерть":
				is_human = True
				self.add_date_or_freetext("P570",tds[1],entry)
			if tds[0]=="Місце народження":
				self.add_to_item_or_freetext("P19",tds[1],entry)
			if tds[0]=="Місце смерті":
				self.add_to_item_or_freetext("P20",tds[1],entry)

		label = title
		if is_human:
			m = re.match(r"^(.+?) \(.+$",label)
			if m:
				label = m.group(1)
			label = re.sub(r'^(.+?), (.+)$',r'\2 \1',label)
			entry.add_item("P31","Q5")
		entry.add_label_etc(label,"label",self.language)

		if entry.is_valid():
			return entry

	def normalize_source_id(self,source_id):
		return source_id.replace(" ","_")
