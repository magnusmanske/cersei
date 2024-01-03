from random import shuffle
from src.scraper_base import ScraperBase
from bs4 import BeautifulSoup
import requests
import re
import string
from src.values import TimeValue
from src.entry import Entry

class Scraper14(ScraperBase):

	"""BUGZ
	"""
	def __init__(self):
		super().__init__(14)

	def scrape_everything(self):
		self.scrape_everything_via_index()

	def paginate_index(self):
		letters1 = [str(letter) for letter in range(0,9)]
		letters2 = [letter for letter in string.ascii_lowercase]
		letters = letters1+letters2
		shuffle(letters)
		for letter in letters:
			pageNr = 0
			has_next = True
			while has_next:
				try:
					url = f"https://bugz.ento.org.nz/search?q={letter}&title=&author=&taxonomicName=&minDate=&maxDate=&sortBy=relevance&sortOrder=desc&page={pageNr}"
					print (url)
					page = requests.get(url)
					html = page.text
					yield html
					soup = BeautifulSoup(html,features="html.parser")
					page_status = soup.find('p', class_="page-status")
					if page_status is None:
						has_next = False
					else:
						a_s = soup.find_all('a', class_="disabled")
						for a in a_s:
							if str(a['aria-label'])=='next page':
								has_next = False
				except Exception as err:
					print(f"Unexpected {err}")
				pageNr += 1

	def entry_url_relative2full(self,url):
		return f"https://bugz.ento.org.nz{url}"

	def parse_index_page(self,html):
		soup = BeautifulSoup(html,features="html.parser")
		for li in soup.find_all('li', class_="search-result"):
			entry = Entry(self.scraper_id)
			h5 = li.find('h5')
			title = h5.get_text().strip()
			for em in h5.find_all('em'):
				self.add_to_item_or_freetext("P921",em.get_text(),entry)
			entry.add_label_etc(title,"original_label",self.language)
			entry.add_label_etc(title,"label",self.language)
			entry.add_item("P31","Q13442814") # scholarly article
			entry.add_monolingual_text("P1476",self.language,title);

			p = li.find('p')
			if p is not None:
				em = p.find_all('em')[-1]
				if em is not None:
					journal_name = em.get_text().strip()
					self.add_to_item_or_freetext("P1433",journal_name,entry)
					if len(p.contents)>=3:
						last_element = p.contents.pop()
						ignore = p.contents.pop()
						text = str(p.get_text())
						m = re.match(r"^(.+?)(\d{4}):(.+)$",text)
						if m is not None:
							author = m.group(1).strip()
							year = int(m.group(2).strip())
							title = m.group(3).strip()
							tv = TimeValue(ymd=[year,1,1],precision=9)
							entry.add_time("P577",tv)
							entry.add_string("P2093",author)
						m = re.match(r"^\s*:\s*(.+?):(.+)$",last_element)
						if m is not None:
							volume = m.group(1).strip()
							pages = m.group(2).strip()
							entry.add_string("P478",volume)
							entry.add_string("P304",pages)

			for a in li.find_all('a', class_="btn-outline-dark"):
				if a.get_text().strip()!='View Detail':
					continue
				url = self.entry_url_relative2full(a['href'])
				entry.add_label_etc(url,"url",self.language)
				entry.id = url.rsplit('/',1)[1]
				entry.add_string("P12224",entry.id)
				break # Only one required

			if entry.is_valid():
				yield entry
