from bs4 import BeautifulSoup
from src.scraper_base import ScraperBase
import requests
import subprocess
import re
import sys
import json
import string
from src.values import TimeValue,QuantityValue,ItemValue
from src.entry import Entry,PropertyValue

# ATTENTION this also updates scraper 13 (artist)

class Scraper12(ScraperBase):
	"""FNAC
	"""

	DOMAIN_DENOMINATION = {
		"photographie": "Q125191",
		"peinture": "Q3305213",
		"oeuvre en 3 dimensions": "Q350268",
		"sculpture": "Q860861",
		"livre": "Q571",
		"vase": "Q191851",
		"dessin": "Q93184",
		"oeuvre textile": "Q22075301",
		"estampe": "Q11835431",
		"ustensile de cuisine": "Q3773693"
	}

	def __init__(self):
		super().__init__(12)
		self.person_scraper_id = 13
		self.institution_scraper_id = 15
		self.person_entry_cache = []
		self.institution_entry_cache = []
		self.date_patterns = [ ("%Y",9) ]

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
				print(f"paginate_index: Unexpected {err}", file=sys.stderr)

	def parse_index_page(self,j):
		for result in j['results']:
			try:
				if "_source" in result and "ua" in result["_source"]:
					for entry in self.process_ua(result["_source"]["ua"]):
						yield entry
			except Exception as err:
				print(f"parse_index_page: {err}", file=sys.stderr)

	def process_ua(self,ua):
		if "artwork" not in ua:
			return
		artwork_entry,authors_birth_death,institution_ids = self.process_artwork(ua["artwork"])

		try:
			for institution_id in institution_ids:
				artwork_entry.add_scraper_item(195,self.institution_scraper_id,institution_id)
				for institution_entry in self.process_institution(institution_id):
					yield institution_entry
		except Exception as err:
			print(f"process_ua institution: {err}", file=sys.stderr)
		
		if "authors" in ua:
			if len(ua["authors"])!=1:
				authors_birth_death = '' # To make sure not add dates to wrong authors
			for artist in ua["authors"]:
				try:
					artist_id,artist_entry = self.process_artist(artist,authors_birth_death)
					if artist_id is not None:
						artwork_entry.add_scraper_item(170,self.person_scraper_id,artist_id)
					if artist_entry is not None:
						yield artist_entry
				except Exception as err:
					print(f"process_ua artist: {err}", file=sys.stderr)

		yield artwork_entry

	def process_artwork(self,artwork):
		entry = Entry(self.scraper_id)
		entry.id = artwork['_id']
		url = self.construct_entry_url_from_id(entry.id)
		entry.add_label_etc(url,"url","en")
		entry.add_label_etc(artwork['title_list'],"original_label",self.language)
		entry.add_label_etc(artwork['title_list'],"label",self.language)

		domain_denomination = []
		if "domain_denomination" in artwork:
			domain_denomination = artwork["domain_denomination"].split(",")
			domain_denomination = [ dd.strip().lower() for dd in domain_denomination ]
			domain_denomination = list(filter(lambda dd: dd!='', domain_denomination))
		found_p31 = False
		for dd in domain_denomination:
			if dd in self.DOMAIN_DENOMINATION:
				entry.add_item("P31",self.DOMAIN_DENOMINATION[dd])
				found_p31 = True
			else:
				entry.add_freetext(31,dd)
		if not found_p31: # Fallback
			entry.add_item("P31","Q838948") # artwork

		institution_ids = []
		if "localisation_if_deposit" in artwork:
			location = artwork['localisation_if_deposit']
			institution = location.split(':').pop().strip()
			institution_ids.append(institution)
			institution_id,location = self.parse_institution(institution)
			if institution_id!='':
				entry.add_scraper_item(276,self.institution_scraper_id,institution_id)

		collection = None
		if "collection" in artwork:
			institution_ids.append(artwork['collection'])
			if artwork['collection']=="Centre national des arts plastiques":
				collection = "Q2072647"
				entry.add_item("P195",collection)
			else:
				entry.add_freetext(195,artwork['collection'])

		if "date_creation" in artwork:
			m = re.match(r"^(\d{3,}).*$",artwork["date_creation"])
			if m is not None:
				entry.add_time(571,TimeValue(ymd=(int(m.group(1)), 1, 1), precision=9))

		self.add_dimensions(entry,artwork['dimensions'])
		if 'inventory' in artwork:
			qualifier = PropertyValue("P195",ItemValue("Q2072647"))
			entry.add_string(217,artwork['inventory'],qualifiers=[qualifier])

		if "medias" in artwork and len(artwork["medias"])>0:
			medium = artwork["medias"][0]
			url = f"https://images.navigart.fr/{medium['max_width']}/{medium['file_name']}"
			if "copyright" in artwork and artwork["copyright"]=="Domaine public":
				entry.add_string(4765,url)
			else:
				entry.add_string(6500,url)
		
		authors_birth_death = ''
		if "authors_birth_death" in artwork:
			authors_birth_death = artwork['authors_birth_death'].strip()

		institution_ids = list(set(institution_ids))
		return (entry,authors_birth_death,institution_ids)

	def parse_institution(self,institution_full):
		institution_full = re.sub(r'^\s*\[.*?\]\s*','',institution_full)
		# print (f">> {institution_full}")
		institution_id = institution_full.strip().lower().replace(' ','_')
		location = ''
		m = re.match(r'^(.+?) *\((.+)\).*$',institution_full)
		if m is not None:
			institution_id = m.group(1).strip().lower().replace(' ','_')
			location = m.group(2).strip()
		# print (f":: {institution_id} / {location}")
		return institution_id,location

	def process_institution(self,institution_full):
		institution_id,location = self.parse_institution(institution_full)
		if institution_id=='' or institution_id in self.institution_entry_cache:
			return
		self.institution_entry_cache.append(institution_id)

		entry = Entry(self.institution_scraper_id)
		entry.id = institution_id
		entry.add_item("P31","Q43229")
		entry.add_label_etc(institution_full,"original_label",self.language)
		entry.add_label_etc(institution_id,"label",self.language)

		if location!='':
			entry.add_freetext(131,location)

		yield entry



	def add_dimensions(self,entry,s):
		if s is None or s=='':
			return
		m = re.match(r"^.*?([0-9,.]+)\s*x\s([0-9,.]+)\s*x\s*([0-9,.]+)\s*(mm|cm|m)\b.*$",s)
		if m is None:
			m = re.match(r"^.*?([0-9,.]+)\s*x\s*([0-9,.]+)\s*(mm|cm|m)\b.*$",s)
			if m is None:
				return
			unit_group_id = 3
		else:
			unit_group_id = 4

		height = self.parse_quantity(f"{m.group(1)} {m.group(unit_group_id)}")
		if height is not None:
			entry.add_quantity(2048,height.amount,height.unit)

		width = self.parse_quantity(f"{m.group(2)} {m.group(unit_group_id)}")
		if width is not None:
			entry.add_quantity(2049,width.amount,width.unit)
		
		if unit_group_id==4:
			depth = self.parse_quantity(f"{m.group(3)} {m.group(unit_group_id)}")
			if depth is not None:
				entry.add_quantity(4511,depth.amount,depth.unit)


	def process_artist(self,artist,authors_birth_death):
		if "_id" not in artist:
			return None,None
		artist_id = artist["_id"]
		if artist_id is None or artist_id in self.person_entry_cache:
			return artist_id,None
		self.person_entry_cache.append(artist_id)

		entry = Entry(self.person_scraper_id)
		entry.id = artist_id
		entry.add_item("P31","Q5")

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
			elif artist["gender"]=="féminin":
				entry.add_item("P21","Q6581072")
			else:
				entry.add_freetext(21,artist['gender'])

		m = re.match(r"^(.*? )-( .*?)$",authors_birth_death)
		if m is not None:
			birth_date,birth_place = self.parse_place_date(m.group(1))
			death_date,death_place = self.parse_place_date(m.group(2))
			self.add_date_or_freetext("P569",birth_date,entry)
			entry.add_freetext(19,birth_place)
			self.add_date_or_freetext("P570",death_date,entry)
			entry.add_freetext(20,death_place)

		return artist_id,entry

	def parse_place_date(self, s):
		m = re.match(r"^(.+?),\s*(.+)$",s.strip())
		if m is None:
			return s,'' # assuming whole string is a date
		return str(m.group(1)).strip(),str(m.group(2))
