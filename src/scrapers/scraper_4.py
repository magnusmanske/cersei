from src.scraper_base import ScraperBase
from bs4 import BeautifulSoup
import requests
import re
from src.values import TimeValue
from src.entry import Entry
from selenium import webdriver

class Scraper4(ScraperBase):

	"""Croatian family encyclopedia
	"""
	def __init__(self):
		super().__init__(4)

	def scrape_everything(self):
		self.scrape_everything_via_index()

	def entry_url_relative2full(self,url):
		return "http://hol.lzmk.hr/"+url

	def paginate_index(self):
		driver = webdriver.PhantomJS("/data/project/cersei/scripts/cersei/phantomjs-2.1.1-linux-x86_64/bin/phantomjs")
		driver.get(self.url)
		index_page = 1
		while True:
			#print ("Now at index page "+str(index_page))
			html = driver.page_source
			yield html
			index_page += 1
			try:
				link = driver.find_element_by_link_text(str(index_page))
			except:
				link = None
			if link is None:
				try:
					link = driver.find_elements_by_partial_link_text("...")
					if isinstance(link,list):
						if index_page>20 and len(link)==1:
							print ("Finished [0] at index "+str(index_page))
							break
						link = link[-1]
				except:
					print ("Finished [1] at index "+str(index_page))
					break
			if link is None:
				print ("Finished [2] at index "+str(index_page))
				break
			link.click()

	def parse_index_page(self,html):
		soup = BeautifulSoup(html,features="html.parser")
		for td in soup.find_all('td'):
			entry = Entry(self.scraper_id)
			is_human = False
			original_label = ""
			description = ""
			found = 0
			for link in td.find_all('a'):
				href = link.get('href')

				m = re.match(r"^clanak\.aspx\?id=(\d+)$",href)
				if not m: # No ID, no point
					continue
				entry.id = m.group(1)
				found += 1

				url = self.entry_url_relative2full(href)
				entry.add_label_etc(url,"url",self.language)

				original_label = link.get_text().strip()
				entry.add_label_etc(original_label,"original_label",self.language)

				pretty_label = original_label
				pretty_label = re.sub(r'^(.+?), (.+)$',r'\2 \1',pretty_label)
				if pretty_label != original_label:
					is_human = True
				entry.add_label_etc(pretty_label,"label",self.language)
			if found != 1:
				continue
			for span in td.find_all('span', class_="opis"):
				description = span.get_text().strip()
				if description.startswith(original_label):
					description = description[len(original_label):].strip()
					description = description.rstrip(" .\n")
				entry.add_label_etc(description,"description",self.language)
			if is_human:
				entry.add_item("P31","Q5")
				m = re.match(r"^.*\((\d{3,4})[–-].*$",description)
				if m:
					tv = TimeValue(ymd=(int(m.group(1)), 1, 1), precision=9)
					entry.add_time("P569",tv)
				m = re.match(r"^.*[–-](\d{3,4})\).*$",description)
				if m:
					tv = TimeValue(ymd=(int(m.group(1)), 1, 1), precision=9)
					entry.add_time("P570",tv)

			if entry.is_valid():
				yield entry
