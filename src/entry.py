from src.values import *


class Entry:
	def __init__(self, scraper_id):
		self.scraper_id = scraper_id
		self.id = None
		self.entry_id = None
		self.current_revision_id = None
		self.values = {
			"string":[],
			"item":[],
			"time":[],
			"freetext":[],
			"monolingual_string":[],
			"labels_etc":[]
		}

	def __str__(self):
		if self.id is None:
			return "<UNINITIALIZED ENTRY>"
		ret = ""
		for key in ["id","current_revision_id"]:
			ret += key+": "+str(self.__dict__[key])+"\n"
		for (key,values) in self.values.items():
			if len(values)==0:
				continue
			ret += key+":\n"
			for item in values:
				ret += "\t"+str(item[0])+": "+str(item[1])+"\n"
		return ret

	def has_revision_changed(self):
		return False

	def create_or_update_in_database(self,db):
		self.check_valid()
		if self.entry_id is None:
			self.entry_id = db.get_entry_id_for_scraper_and_id(self.scraper_id, self.id)
		self.current_revision_id = db.get_current_revision_id(self.entry_id)
		if self.current_revision_id!=0 and not self.has_revision_changed():
			print ("Revision unchanged")
			return
		self.current_revision_id = db.create_new_revision(self.entry_id)
		db.set_current_revision(self.entry_id,self.current_revision_id)

		# Generate INSERT statements
		for table,values in self.values.items():
			if len(values) == 0:
				continue
			columns = ["revision_id"]
			has_property = (table!="labels_etc")
			if has_property:
				columns.append("property")
			columns += values[0][1].db_fields()
			rows = []
			for value in values:
				row = [ self.current_revision_id ]
				if has_property:
					row.append(value[0])
				row += value[1].db_values(db)
				rows.append(row)
			db.insert_group(table,columns,rows)


	def sanitize_property(self,prop) -> int:
		prop = str(prop).strip().upper()
		if prop[0]=="P":
			prop = prop[1:]
		return int(prop)


	def add_label_etc(self,value,type_name,language):
		v = LabelsEtcValue(value,type_name,language)
		self.values["labels_etc"].append((0,v))

	def add_string(self,prop,string: str):
		prop = self.sanitize_property(prop)
		value = StringValue(string)
		self.values["string"].append((prop,value))

	def add_item(self,prop,q: str):
		prop = self.sanitize_property(prop)
		item = ItemValue(q)
		self.values["item"].append((prop,item))

	def add_time(self,prop,tv: TimeValue):
		prop = self.sanitize_property(prop)
		self.values["time"].append((prop,tv))

	def add_freetext(self,prop,string: str):
		prop = self.sanitize_property(prop)
		value = FreetextValue(string)
		self.values["freetext"].append((prop,value))

	def add_monolingual_text(self, prop, language: str, value: str):
		prop = self.sanitize_property(prop)
		value = MonolingualStringValue(language, value)
		self.values["monolingual_string"].append((prop,value))

	def is_valid(self):
		return self.id is not None

	def check_valid(self):
		if not self.is_valid():
			raise Exception("Entry not valid: "+str(self))

