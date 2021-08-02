import abc
import toolforge
from src.tooldatabase import ToolDatabase
from src.entry import Entry

class ScraperBase(metaclass=abc.ABCMeta):
	PROP2GROUP = {
		"P19":"place",
		"P20":"place",
		"P106":"occupation",
	}

	def __init__(self):
		self.enforce_unique_ids = True
		self.url_pattern = ''
		self.scraper_id = None
		self.db = None
		self.dbwd = None

	@abc.abstractmethod
	def paginate_index(self):
		"""Returns the HTML of every index page"""

	@abc.abstractmethod
	def entry_url_relative2full(self,url):
		"""Extends the given relative URL to a full, valid URL"""

	@abc.abstractmethod
	def parse_index_page(self,html):
		"""Parses the index page HTML into individual entries"""

	@abc.abstractmethod
	def construct_entry_url_from_id(self,id):
		"""Constructs the URL of the entry based on its ID"""

	def get_db(self):
		if self.db:
			return self.db
		self.db = ToolDatabase()
		return self.db

	def get_group_for_property(self,prop):
		if prop not in self.PROP2GROUP:
			return
		return self.PROP2GROUP[prop]


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
