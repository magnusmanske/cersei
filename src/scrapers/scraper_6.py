from src.scraper_base import ScraperBase
import requests
import re
import json
import gzip
from src.values import TimeValue
from src.entry import Entry

class Scraper6(ScraperBase):

	"""Finnish National Gallery artwork
	"""
	def __init__(self):
		super().__init__(6)

	def scrape_everything(self):
		db = self.get_db()
		for entry in self.scrape_everything_via_datafile():
			if entry is not None:
				entry.create_or_update_in_database(db)

	def scrape_everything_via_datafile(self):
		# https://codereview.stackexchange.com/questions/203842/read-gzipped-json-file-from-url
		# curl -s -X 'GET' 'https://www.kansallisgalleria.fi/api/v1/objects' -H 'accept: application/json' -H 'x-api-key: 03956358af58842eaf03798ae707be7b' > /data/project/cersei/data_files/kansallisgalleria.objects.json.gz
		filename = "/data/project/cersei/data_files/kansallisgalleria.objects.json.gz"
		proc = subprocess.Popen(f"gunzip -c '{filename}' | jq -cn --stream 'fromstream(1|truncate_stream(inputs ))'", shell=True,stdout=subprocess.PIPE)
		while True:
			line = proc.stdout.readline()
			if not line:
				break
			line = line.rstrip().decode('utf-8')
			obj = json.loads(line)
			for entry in self.process_object(obj):
				yield entry

	def process_object(self,obj):
		if "people" not in obj:
			return
		for person in obj["people"]:
			if "id" not in person:
				continue
			entry = Entry(self.scraper_id)
			entry.id = str(person["id"])
			entry.add_item("P31","Q5") # human
			entry.add_freetext(735,person["firstName"])
			entry.add_freetext(734,person["familyName"])
			text = (person["firstName"] or '')+' '+(person["familyName"] or '')
			entry.add_label_etc(text.strip(),"label","en")
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
			yield entry
