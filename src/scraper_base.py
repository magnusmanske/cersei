import abc
import toolforge
from src.tooldatabase import ToolDatabase
from src.entry import Entry
from src.values import TimeValue
import datetime
import requests

class ScraperBase(metaclass=abc.ABCMeta):
	PROP2GROUP = {
		"P19":"place",
		"P20":"place",
		"P106":"occupation",
	}

	def __init__(self,scraper_id=None):
		self.enforce_unique_ids = True
		self.name = None
		self.url = None
		self.url_pattern = None
		self.scraper_id = scraper_id
		self.language = None
		self.db = None
		self.dbwd = None
		self.date_patterns = []
		if scraper_id is not None:
			self.load_scraper_from_db(scraper_id)

	"""Returns the HTML of every index page"""
	def paginate_index(self):
		raise Exception ("ScraperBase::paginate_index should be overloaded")

	"""Extends the given relative URL to a full, valid URL"""
	def entry_url_relative2full(self,url):
		raise Exception ("ScraperBase::entry_url_relative2full should be overloaded")

	"""Parses the index page HTML into individual entries"""
	def parse_index_page(self,html):
		raise Exception ("ScraperBase::parse_index_page should be overloaded")

	@abc.abstractmethod
	def scrape_everything(self):
		"""Scrapes the entire website"""

	def is_entry_page(self,url,html=None,soup=None):
		raise Exception ("ScraperBase::is_entry_page should be overloaded")

	def get_entry_id_from_href(self,href):
		raise Exception ("ScraperBase::get_entry_id_from_href should be overloaded")

	def links_to_follow(self,soup):
		raise Exception ("ScraperBase::links_to_follow should be overloaded")

	"""Constructs the URL of the entry based on its ID"""
	def construct_entry_url_from_id(self,id):
		if self.url_pattern is None:
			return ""
		return self.url_pattern.replace('$1',id)

	def load_scraper_from_db(self,scraper_id):
		self.scraper_id = scraper_id
		db = self.get_db()
		row = db.get_single_row_for_id("scraper",scraper_id)
		if row is None:
			raise Exception("There is no scraper with ID "+str(scraper_id))
		self.name = row["name"]
		self.url = row["url"]
		self.url_pattern = row["url_pattern"]
		self.language = row["language"]

	def get_db(self):
		if self.db:
			return self.db
		self.db = ToolDatabase()
		return self.db

	def get_group_for_property(self,prop):
		if prop not in self.PROP2GROUP:
			return
		return self.PROP2GROUP[prop]

	def add_description_list(self,descs,entry):
		descs_no_empty = filter(lambda d: d.strip()!="", descs)
		short_description = "; ".join(descs_no_empty)
		entry.add_label_etc(short_description,"description",self.language)

	def string2item(self,prop,text):
		db = self.get_db()
		group = self.get_group_for_property(prop)
		q = db.find_text_item_match(text,group,["",self.language])
		return q

	def add_to_item_or_freetext(self,prop: str,text: str, entry: Entry):
		text = text.strip()
		if text=="":
			return
		item = self.string2item(prop,text)
		if item is None:
			entry.add_freetext(prop,text)
		else:
			entry.add_item(prop,item)

	def scrape_everything_via_index(self):
		db = self.get_db()
		for html in self.paginate_index():
			for entry in self.parse_index_page(html):
				entry.create_or_update_in_database(db)
		self.text2item_heuristic()

	def place_heuristic(self,text):
		# Big city, City, Capital city, capital of region, district town
		hints = ["Q1549591","Q515","Q5119","Q12031379","Q8452914"]
		return self.run_heuristic(text,hints,"P19")

	def occupation_heuristic(self,text):
		# Occupation
		hints = ["Q12737077"]
		return self.run_heuristic(text,hints,"P106")

	def run_heuristic(self,text,hints,example_property):
		s2i = self.string2item(example_property,text)
		if s2i is not None:
			return s2i # We already have an item for that
		if self.dbwd is None:
			self.dbwd = toolforge.connect('wikidatawiki')
		with self.dbwd.cursor() as cursor:
			sql = """
				SELECT DISTINCT page_title
				FROM wbt_text_in_lang,wbt_text,wbt_term_in_lang,wbt_item_terms,page,pagelinks
				WHERE wbx_text=%s
				AND wbx_id=wbxl_text_id
				AND wbxl_language=%s
				AND wbxl_id=wbtl_text_in_lang_id
				AND wbtl_type_id IN (1,3)
				AND wbtl_id=wbit_term_in_lang_id
				AND page_title=concat("Q",wbit_item_id)
				AND page_namespace=0
				AND pl_from=page_id
				AND pl_namespace=0
				AND pl_title IN (
				""".replace("\n"," ").strip()
			placeholders = ["%s" for hint in hints]
			sql += ",".join(placeholders) + ")"
			params = [text,self.language]
			params += hints
			cursor.execute(sql, params)
			rows = cursor.fetchall()
			if len(rows)!=1:
				return
			item = rows[0][0].decode('utf8')
			group = self.get_group_for_property(example_property)
			if group is None: # Paranoia
				return item
			db = self.get_db()
			db.add_text2item(self.language,group,text,item)
			return item

	"""Finds freetext rows that can be converted to items,
	creates the item rows and deletes the freetext ones.
	It does *not* create new revisions.
	"""
	def convert_freetext_to_item(self):
		groups = {}
		for prop,group in self.PROP2GROUP.items():
			if group not in groups:
				groups[group] = []
			groups[group].append(int(prop[1:]))
		db = self.get_db()
		for group,props in groups.items():
			rows = db.get_freetext2item(self.scraper_id,group,props,self.language)
			if rows is None or len(rows)==0:
				continue
			freetext_counter = {}
			for row in rows:
				fid = row["freetext_id"]
				if fid not in freetext_counter:
					freetext_counter[fid] = 0
				freetext_counter[fid] += 1
			columns = ["property","item_id","item_type","revision_id"]
			rows2db = []
			freetext2delete = []
			for row in rows:
				fid = row["freetext_id"]
				if freetext_counter[fid]>1:
					print ("Freetext row "+str(fid)+" has multiple replacements, skipping")
					continue
				new_row = [row["property"],row["item_id"],"item",row["revision_id"]]
				rows2db.append(new_row)
				freetext2delete.append(fid)
			if len(freetext2delete)+len(rows2db)==0:
				continue
			print ("Moving "+str(len(rows2db))+" freetext rows to items")
			db.insert_group("item",columns,rows2db)
			db.delete_rows_by_id("freetext",freetext2delete)

	def text2item_heuristic(self):
		if self.scraper_id is None:
			return
		db = self.get_db()
		rows = db.get_item2text_candidates(self.scraper_id)
		if rows is None or len(rows)==0:
			return
		for row in rows:
			prop = "P"+str(row["property"])
			if prop not in self.PROP2GROUP:
				continue
			group = self.PROP2GROUP[prop]
			text = row["value"].decode('utf8')
			if group=="place":
				self.place_heuristic(text)
			if group=="occupation":
				self.occupation_heuristic(text)
		self.convert_freetext_to_item()

	def clear_old_revisions(self):
		db = self.get_db()
		for table in Entry.VALUE_TABLES:
			db.clear_old_revisions_in_table(self.scraper_id,table)
		db.clear_old_revisions(self.scraper_id)


	def scrape_everything_via_follow(self,seed_urls):
		running = seed_urls
		url_cache = running[:]
		while len(running)>0:
			next_up = []
			for url in running:
				page = requests.get(url)
				html = page.text
				soup = BeautifulSoup(html,features="html.parser")

				# Process this as an entry?
				if self.is_entry_page(url,html,soup):
					yield url,html,soup

				# Find links to other encyclopedia pages
				for link in self.links_to_follow(soup):
					href = link.get('href')
					if href is None:
						continue
					entry_id = self.get_entry_id_from_href(href)
					url = self.construct_entry_url_from_id(entry_id)
					if url in url_cache:
						continue
					next_up.append(url)
					url_cache.append(url)
			running = next_up

	def scrape_mediawiki(self,api_url,batch_size=500):
		# Get page URL pattern
		url = api_url+"?action=query&meta=siteinfo&siprop=general&format=json"
		response = requests.get(url)
		j = response.json()
		article_pattern = j["query"]["general"]["server"]+j["query"]["general"]["articlepath"]
		if article_pattern.startswith("/"):
			if api_url.startswith("https:"):
				article_pattern = "https:"+article_pattern
			else:
				article_pattern = "http:"+article_pattern

		# Retrieve all namespace 0 pages
		apcontinue = None
		while True:
			url = api_url+"?action=query&list=allpages&apnamespace=0&format=json&apfilterredir=nonredirects&aplimit="+str(batch_size)
			if apcontinue is not None:
				url += "&apcontinue="+str(apcontinue)
			try:
				response = requests.get(url)
				j = response.json()
			except:
				break # Some issue, can't continue
			for pageinfo in j["query"]["allpages"]:
				page_title = pageinfo["title"]
				page_url = article_pattern.replace("$1",page_title.replace(" ","_"))
				yield page_title,page_url
			if "continue" in j and "apcontinue" in j["continue"]:
				apcontinue = j["continue"]["apcontinue"]
			else:
				break # Can't continue

	def add_date_or_freetext(self,prop,date_string,entry):
		if len(self.date_patterns)==0:
			raise Exception("add_date_or_freetext is used but no date_patterns are set")
		date_string = str(date_string).strip()
		for date_pattern,precision in self.date_patterns:
			try:
				dt = datetime.datetime.strptime(date_string,date_pattern)
			except:
				continue
			if dt is None:
				continue
			if precision==11:
				tv = TimeValue(ymd=(dt.year, dt.month, dt.day), precision=precision)
			elif precision==10:
				tv = TimeValue(ymd=(dt.year, dt.month, 1), precision=precision)
			elif precision==9:
				tv = TimeValue(ymd=(dt.year, 1, 1), precision=precision)
			else:
				continue
			entry.add_time(prop,tv)
			return True
		entry.add_freetext(prop,date_string)
		return False
