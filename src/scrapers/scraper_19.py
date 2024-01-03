from src.scraper_base import ScraperBase
from bs4 import BeautifulSoup
import requests
import re
import string
import json
from src.values import TimeValue
from src.entry import Entry

class Scraper19(ScraperBase):

	"""EDIT16 catalogue printer (publisher)
	"""
	def __init__(self):
		super().__init__(19)

	def scrape_everything(self):
		self.scrape_everything_via_index()

	def paginate_index(self):
		pageNr = 1
		perPage = 50
		while True:
			try:
				url = f"https://edit16.iccu.sbn.it/o/edit16-api/titles-search?fieldstruct%5B1%5D=ricerca.parole_tutte%3A4%3D6&fieldop%5B8%5D=AND%3A%40and%40&fieldaccess%5B1%5D=NomeEdizione%3A1019%3Anocheck&fieldop%5B6%5D=AND%3A%40and%40&fieldop%5B4%5D=AND%3A%40and%40&fieldstruct%5B7%5D=ricerca.parole_tutte%3A4%3D6&sort_access=Editore%3Amin%203003&fieldaccess%5B5%5D=Indirizzo%3A4003%3Anocheck&fieldop%5B2%5D=AND%3A%40and%40&fieldstruct%5B9%5D=ricerca.parole_tutte%3A4%3D6&fieldaccess%5B3%5D=LuogoAttivita%3A59%3Anocheck&fieldop%5B0%5D=AND%3A%40and%40&fieldstruct%5B3%5D=ricerca.parole_tutte%3A4%3D6&fieldaccess%5B9%5D=item%3A1032%3ACNCT&fieldstruct%5B5%5D=ricerca.parole_tutte%3A4%3D6&fieldaccess%5B7%5D=DescrizioneMarca%3A3087%3Anocheck&item_nocheck%3A9003%3Atipo=T&_cacheid=1704297185695&fieldstruct%5B0%5D=ricerca.parole_tutte%3A4%3D6&fieldaccess%5B2%5D=Note%3A63&fieldop%5B9%5D=AND%3A%40and%40&fieldaccess%5B0%5D=NomeEditore%3A1018%3Anocheck&fieldop%5B7%5D=AND%3A%40and%40&fieldaccess%5B6%5D=Keywords%3A1016&fieldop%5B5%5D=AND%3A%40and%40&fieldstruct%5B6%5D=ricerca.parole_tutte%3A4%3D6&fieldaccess%5B4%5D=Insegna%3A9002%3Anocheck&fieldop%5B3%5D=AND%3A%40and%40&fieldstruct%5B8%5D=ricerca.parole_tutte%3A4%3D6&fieldop%5B1%5D=AND%3A%40and%40&fieldstruct%5B2%5D=ricerca.parole_tutte%3A4%3D6&fieldaccess%5B8%5D=CitStandardMarca%3A4087%3Anocheck&fieldstruct%5B4%5D=ricerca.parole_tutte%3A4%3D6&core=editori&page-size={perPage}&page={pageNr}"
				# print (url)
				page = requests.get(url)
				j = json.loads(page.text)
				yield j
				if j['data']['pagination']['current'] >= j['data']['pagination']['total']:
					break
			except Exception as err:
				print(f"Unexpected {err}")
			pageNr += 1

	def parse_index_page(self,j):
		for item in j['data']['results']:
			try:
				for entry in self.process_publisher(item):
					yield entry
			except Exception as err:
				print(f"parse_index_page: {err}")

	def process_publisher(self,item):
		m = re.match(r'^CNCT0*(\d+)$',item['bid'])
		if m is None:
			return
		entry = Entry(self.scraper_id)
		entry.id = int(m.group(1))
		entry.add_string(5493,str(entry.id))

		url = f"https://edit16.iccu.sbn.it/resultset-editori/-/editori/detail/CNCT{entry.id}"
		entry.add_label_etc(url,"url","en")

		entry.add_label_etc(item['title']['text'],"original_label",self.language)
		entry.add_label_etc(item['title']['text'],"label",self.language)

		for alias in item['infos']:
			if ':' not in alias: # "Varianti del nome:"
				entry.add_label_etc(alias,"alias",self.language)
		
		yield entry
