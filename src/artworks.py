import subprocess
import requests
import json
from datetime import datetime
from src.tooldatabase import ToolDatabase
# from quickstatements_client import DateQualifier, EntityQualifier, TextQualifier, EntityLine # REQUIRES python >= 3.8

class Artworks:
	def __init__(self):
		self.check_work_label = True
		self.check_creator = True
		self.require_matched_creator = False
		self.verbose = True

	def generate_qs(self):
		db = ToolDatabase()
		ret = []
		batch = []
		qs_commands = []
		with db.connection.cursor() as cursor:
			sql = (
				"SELECT vw_artworks.*,scraper.property AS wd_prop,scraper.property_item AS wd_prop_item"
				" FROM vw_artworks ,entry,scraper"
				" WHERE work_wd_item IS NULL AND candidate_qs IS NOT NULL AND entry.id=work_entry_id"
				" AND scraper.id=scraper_id AND scraper.property IS NOT NULL AND scraper.property_item IS NOT NULL"
#				" LIMIT 10" # TODO TESTING FIXME
				)
			cursor.execute(sql, [])
			fields = dict( (i[0],idx) for idx,i in enumerate(cursor.description))
			for row in cursor:
				batch.append(row)
				if len(batch)>=25: # (less than 50 because potentially more than one candidate item per row)
					for qs_command in self.process_batch(batch,fields):
						qs_commands.append(qs_command)
					batch = []
		for qs_command in self.process_batch(batch,fields):
			qs_commands.append(qs_command)
		return qs_commands

	def process_batch(self,batch,fields):
		if len(batch)==0:
			return

		items = []
		for row in batch:
			for candidate_qs in row[fields["candidate_qs"]].split(','):
				(candidate_q,institution_item) = candidate_qs.split('|')
				items.append(f"Q{candidate_q}")
		items = list(set(items)) # Deduplicate
		if len(items)==0:
			return # Nothing to do
		url = "https://www.wikidata.org/w/api.php?action=wbgetentities&format=json&ids="+"|".join(items)
		r = requests.get(url)
		j = json.loads(r.text)

		today = datetime.utcnow().strftime("+%Y-%m-%dT00:00:00Z/11")
		for row in batch:
			for candidate_qs in row[fields["candidate_qs"]].split(','):
				(candidate_q,institution_item) = candidate_qs.split('|')

				# Check for same institution qualifier in the WD statement
				if institution_item!=str(row[fields["wd_prop_item"]]):
					self.log (f"Skipping, wrong institution")
					continue

				q = f"Q{candidate_q}"
				if ("entities" not in j) or (q not in j["entities"]):
					self.log("Work {q} not found")
					continue
				entity = j["entities"][q]

				if self.check_work_label:
					if "labels" not in entity:
						self.log("Label match required, but {q} has no labels")
						continue
					has_correct_label = False
					work_label = row[fields['work_label']].strip().lower()
					for language in entity["labels"]:
						if work_label==entity["labels"][language]["value"].strip().lower():
							has_correct_label = True
					if not has_correct_label:
						self.log(f"https://www.wikidata.org/wiki/{q} has the correct institution and item ID, but no matching label ({work_label})")
						continue


				if self.check_creator:
					has_correct_creator = False
					if row[fields['creator_wd_item']] is None:
						if self.require_matched_creator:
							self.log (f"Matched creator required but not found")
							continue
						else:
							has_correct_creator = True
					creator_q = f"Q{row[fields['creator_wd_item']]}"
					if self.entity_has_item_statement(entity,"P170",creator_q):
						has_correct_creator = True
					if not has_correct_creator:
						self.log (f"Skipping {q}, creator {creator_q} not found in {q}")
						continue

				wd_prop = f"P{row[fields['wd_prop']]}"
				source_id = row[fields['work_source_id']]
				if self.entity_has_string_statement(entity,wd_prop,source_id):
					self.log(f"{q} already has {wd_prop}:{source_id}")
					continue

				# retrieved = DateQualifier.retrieved(datetime.utcnow())
				# source = EntityQualifier(predicate="S248", target=f"Q{row[fields['wd_prop_item']]}")
				# determination_method = EntityQualifier(predicate="S459", target="Q123778298")
				# qs_command = TextLine(
				# 	subject = q,
				# 	predicate = wd_prop,
				# 	target = source_id,
				# 	qualifiers = [determination_method,source,retrieved]
				# 	)

				# print (f"{candidate_q} / {institution_item} / {row[fields['wd_prop_item']]}")
				qs_command = [ q , wd_prop , f"\"{source_id}\"" , 
					"S459" , "Q123778298", # determination method: CERSEI
					"S248" , f"Q{row[fields['wd_prop_item']]}" ,
					"S813" , today]
				yield qs_command;

	def entity_has_item_statement(self,entity,prop,target_item):
		if "claims" in entity and prop in entity["claims"]:
			for claim in entity["claims"][prop]:
				if "mainsnak" in claim and "datavalue" in claim["mainsnak"] and "value" in claim["mainsnak"]["datavalue"] and "id" in claim["mainsnak"]["datavalue"]["value"]:
					if target_item==claim["mainsnak"]["datavalue"]["value"]["id"]:
						return True
		return False

	def entity_has_string_statement(self,entity,prop,value):
		if "claims" in entity and prop in entity["claims"]:
			for claim in entity["claims"][prop]:
				if "mainsnak" in claim and "datavalue" in claim["mainsnak"] and "value" in claim["mainsnak"]["datavalue"]:
					if value==claim["mainsnak"]["datavalue"]["value"]:
						return True
		return False

	def log(self,msg):
		if self.verbose:
			print (msg)

	def run_qs(self,qs_commands):
		with open("/data/project/cersei/quickstatements_token", 'r') as file:
			qs_token = file.read().strip()
		print (qs_token)
		tmp_file = "/tmp/cersei_artworks.qs"
		with open(tmp_file,"w") as f:
			for qsc in qs_commands:
				f.write ("\t".join(qsc)+"\n")

		now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
		command = f"""curl https://quickstatements.toolforge.org/api.php \
			-d action=import \
			-d submit=1 \
			-d format=v1 \
			-d username=Magnus_Manske \
			-d "batchname=CERSEI artwork sync {now}" \
			--data-raw 'token={qs_token}' \
			--data-urlencode data@{tmp_file}
		"""
		res = subprocess.Popen(command, shell=True)
		# j = json.loads(res)
		# if "status" in j and j["status"]=="OK":
		# 	return
		# print (f"ERROR: {res}")

