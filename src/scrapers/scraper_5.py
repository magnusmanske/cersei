from src.scraper_base import ScraperBase
import requests
import subprocess
import re
import json
import gzip
from src.values import TimeValue
from src.entry import Entry

# ATTENTION this also updates scraper 6 (artist)

class Scraper5(ScraperBase):

	"""Finnish National Gallery artwork&artist
	"""
	def __init__(self):
		super().__init__(5)
		self.person_scraper_id = 6
		self.person_entry_cache = []

	def scrape_everything(self):
		db = self.get_db()
		for entry in self.scrape_everything_via_datafile():
			if entry is not None:
				entry.create_or_update_in_database(db)

	def scrape_everything_via_datafile(self):
		# Download data file
		filename = self.get_data_file_path("kansallisgalleria.objects.json.gz")
		download_command = f"curl -s -X 'GET' 'https://www.kansallisgalleria.fi/api/v1/objects' -H 'accept: application/json' -H 'x-api-key: 03956358af58842eaf03798ae707be7b' > '{filename}'"
		subprocess.run(download_command, shell=True, check=True)

		# Parse all entries
		proc = subprocess.Popen(f"gunzip -c '{filename}' | jq -cn --stream 'fromstream(1|truncate_stream(inputs ))'", shell=True,stdout=subprocess.PIPE)
		while True:
			line = proc.stdout.readline()
			if not line:
				break
			line = line.rstrip().decode('utf-8')
			obj = json.loads(line)
			try:
				for entry in self.process_object(obj):
					yield entry
			except Exception as err:
				print(f"Unexpected {err}")

	def process_object(self,obj):
		#print (json.dumps(obj))
		entry = Entry(self.scraper_id)
		entry.id = str(obj["objectId"])
		entry.add_item("P31","Q838948") # artwork
		url = self.construct_entry_url_from_id(entry.id)
		entry.add_label_etc(url,"url","en")
		if "title" in obj:
			for language,text in obj["title"].items():
				entry.add_label_etc(text,"label",language)
		if "collection" in obj and "en" in obj["collection"]:
			entry.add_freetext(195,obj["collection"]["en"])
		# TODO height,width
		if "materials" in obj and "en" in obj["materials"]:
			entry.add_freetext(186,obj["materials"]["en"])
		if "classifications" in obj and "en" in obj["classifications"]:
			entry.add_freetext(31,obj["classifications"]["en"])
		if "inventoryNumber" in obj:
			entry.add_string(217,obj["inventoryNumber"])
		if "people" in obj:
			for person in obj["people"]:
				if "role" not in person or "id" not in person["role"]:
					continue
				if person["role"]["id"]!=98595: # Artist
					continue
				if "id" in person:
					entry.add_scraper_item(170,self.person_scraper_id,person["id"])
					if person["id"] not in self.person_entry_cache:
						self.person_entry_cache.append(person["id"])
						person_entry = self.process_person(person)
						yield person_entry
		yield entry

	def process_person(self,person):
		entry = Entry(self.person_scraper_id)
		entry.id = str(person["id"])
		entry.add_item("P31","Q5") # human
		entry.add_freetext(735,person["firstName"])
		entry.add_freetext(734,person["familyName"])
		text = (person["firstName"] or '')+' '+(person["familyName"] or '')
		entry.add_label_etc(text.strip(),"label","en")

		url = f"https://www.kansallisgalleria.fi/en/search?authors[]={text}"
		entry.add_label_etc(url,"url","en")

		entry.add_freetext(19,person["birthPlace"])
		entry.add_freetext(20,person["deathPlace"])

		m = re.match(r"^(\d{3,})-(\d{2})-(\d{2})$",(person["birthDate"] or ''))
		if m:
			tv = TimeValue(ymd=(int(m.group(1)), int(m.group(2)), int(m.group(3))), precision=11)
			entry.add_time("P569",tv)
		elif person["birthYear"] is not None:
			tv = TimeValue(ymd=(int(person["birthYear"]), 1, 1), precision=9)
			entry.add_time("P569",tv)

		m = re.match(r"^(\d{3,})-(\d{2})-(\d{2})$",(person["deathDate"] or ''))
		if m:
			tv = TimeValue(ymd=(int(m.group(1)), int(m.group(2)), int(m.group(3))), precision=11)
			entry.add_time("P570",tv)
		elif person["deathYear"] is not None:
			tv = TimeValue(ymd=(int(person["deathYear"]), 1, 1), precision=9)
			entry.add_time("P570",tv)
		return entry
