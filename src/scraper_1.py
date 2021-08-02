from src.scraper_base import ScraperBase
from bs4 import BeautifulSoup
import requests
import re
from src.values import TimeValue
from src.entry import Entry

class Scraper1(ScraperBase):
	HARDCODED = {
		"places":{
			"Praha":"Q1085"
		}
	}

	"""Slovnikceske Literatury CZ
	"""
	def __init__(self):
		super().__init__()
		self.scraper_id = 1
		self.url_pattern = "http://www.slovnikceskeliteratury.cz/showContent.jsp?docId=$1"
		self.language = 'cs'

	def paginate_index(self):
		pageNr = 1
		while True:
			url = "http://www.slovnikceskeliteratury.cz/list.jsp?show=-&order=title&ascDesc=asc&startswith="
			page = requests.post(url, data = {"page":1,"pageNr":pageNr})
			html = page.text
			yield html
			soup = BeautifulSoup(html,features="html.parser")
			if len(soup.find_all('input', attrs={"class": "submitButton", "name": "next"}))==0:
				break # No "next" button, quit
			pageNr += 1

	def construct_entry_url_from_id(self,id):
		return self.url_pattern.replace('$1',id)

	def entry_url_relative2full(self,url):
		return re.sub(r'^\./showContent\.jsp','http://www.slovnikceskeliteratury.cz/showContent.jsp',url)

	def parse_index_page(self,html):
		soup = BeautifulSoup(html,features="html.parser")
		for div in soup.find_all('div', class_="result"):
			o = Entry(self.scraper_id)
			descs = []
			for link in div.find_all('a', class_="result_name"):
				original_label = link.get_text().strip()
				pretty_label = original_label
				pretty_label = re.sub(r'^.+? - +','',pretty_label)
				pretty_label = re.sub(r' *\(.*$','',pretty_label)
				pretty_label = re.sub(r'^(.+?), (.+)$',r'\2 \1',pretty_label)
				o.add_label_etc(original_label,"original_label",self.language)
				o.add_label_etc(pretty_label,"label",self.language)
				href = link.get('href')
				m = re.match(r"^.*docId=(\d+).*$",href)
				if m:
					o.id = m.group(1)
					url = self.construct_entry_url_from_id(o.id)
					o.add_label_etc(url,"url",self.language)
			for span in div.find_all('span', class_="datumnarozeni"):
				s = re.sub(r"\s+"," ",span.get_text()).strip()
				self.parse_date_prop(o,s,"P569")
				descs.append(s)
			for span in div.find_all('span', class_="mistonarozeni"):
				s = re.sub(r"\s+"," ",span.get_text()).strip()
				if s=="":
					continue
				prop = "P19"
				item = self.string2item(prop,s)
				if item is None:
					o.add_freetext(prop,s)
				else:
					o.add_item(prop,item)
				descs.append(s)
			for span in div.find_all('span', class_="datumumrti"):
				s = re.sub(r"\s+"," ",span.get_text()).strip()
				self.parse_date_prop(o,s,"P570")
				descs.append(s)
			for span in div.find_all('span', class_="mistoumrti"):
				s = re.sub(r"\s+"," ",span.get_text()).strip()
				if s=="":
					continue
				prop = "P20"
				item = self.string2item(prop,s)
				if item is None:
					o.add_freetext(prop,s)
				else:
					o.add_item(prop,item)
				descs.append(s)
			descs_no_empty = filter(lambda d: d.strip()!="", descs)
			o.short_description = "; ".join(descs_no_empty)
			if o.is_valid():
				yield o

	def parse_date_prop(self,o,date_string,prop):
		date_string = date_string.strip()
		if date_string=="":
			return
		try:
			m = re.match(r"^[*†]\D*(\d{1,2})\. *(\d{1,2})\. *(\d{3,4})",date_string)
			if m:
				day = int(m.group(1))
				month = int(m.group(2))
				year = int(m.group(3))
				tv = TimeValue(ymd=(year, month, day), precision=11)
				o.add_time(prop,tv)
			else: # Couldn't parse date
				o.add_freetext(prop,date_string)
		except: # Couldn't parse date
			o.add_freetext(prop,date_string)

	def string2item(self,prop,s):
		if prop in ["P19","P20"]:
			if s in self.HARDCODED["places"]:
				return self.HARDCODED["places"][s]
