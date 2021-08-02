from src.values import *

class PropertyValue:
	def __init__(self, prop, value):
		self.prop = prop
		self.value = value

	def __str__(self):
		return "P"+str(self.prop)+":"+str(self.value)

	def __lt__(self,other):
		if self.prop < other.prop:
			return self.prop < other.prop
		return self.value < other.value

	def __gt__(self,other):
		return (other<self)

	def __eq__(self,other):
		return not (self<other) and not (other<self)

	def __ne__(self,other):
		return not (self==other)

class Entry:
	VALUE_TABLES = ["string","item","time","location","freetext","monolingual_string","labels_etc"]

	def __init__(self, scraper_id):
		self.scraper_id = scraper_id
		self.id = None
		self.entry_id = None
		self.revision_id = None
		self.values = {}
		for table in self.VALUE_TABLES:
			self.values[table]=[]

	def __str__(self):
		if self.id is None:
			return "<UNINITIALIZED ENTRY>"
		ret = ""
		for key in ["id","revision_id"]:
			ret += key+": "+str(self.__dict__[key])+"\n"
		for (key,values) in self.values.items():
			if len(values)==0:
				continue
			ret += key+":\n"
			for item in values:
				ret += str(item)
		return ret

	def load_from_revision(self,db,revision_id):
		self.revision_id = revision_id
		for table,value in self.values.items():
			if table in ["time","location"]:
				query_table=table
			else:
				query_table="vw_"+table
			rows = db.load_table_for_revision(self.revision_id,query_table)
			if rows is None:
				continue
			for row in rows:
				if table=="string":
					o = StringValue(row["value"].decode('utf8'))
				elif table=="item":
					o = ItemValue(row["q"])
				elif table=="time":
					o = TimeValue(tv=row["value"],precision=row["precision"])
				elif table=="location":
					o = LocationValue(latitude=row["longitude"],longitude=row["longitude"])
				elif table=="freetext":
					o = FreetextValue(row["value"].decode('utf8'))
				elif table=="monolingual_string":
					o = MonolingualStringValue(row["language"].decode('utf8'),row["value"].decode('utf8'))
				elif table=="labels_etc":
					o = LabelsEtcValue(row["value"].decode('utf8'),row["type_name"],row["language"].decode('utf8'))
				prop = 0
				if "property" in row:
					prop = int(row["property"])
				self.values[table].append(PropertyValue(prop,o))

	def has_revision_changed(self,db):
		other = Entry(self.scraper_id)
		other.load_from_revision(db,self.revision_id)
		for table,value in self.values.items():
			own_table = sorted(value)
			other_table = sorted(other.values[table])
			if len(own_table)!=len(other_table):
				return True
			for row_id,row in enumerate(own_table):
				if row!=other_table[row_id]:
					return True
		return False

	def create_or_update_in_database(self,db):
		self.check_valid()
		if self.entry_id is None:
			self.entry_id = db.get_entry_id_for_scraper_and_id(self.scraper_id, self.id)
		self.revision_id = db.get_current_revision_id(self.entry_id)
		if self.revision_id!=0 and not self.has_revision_changed(db):
			return
		self.revision_id = db.create_new_revision(self.entry_id)
		db.set_current_revision(self.entry_id,self.revision_id)

		# Generate INSERT statements
		for table,prop_values in self.values.items():
			if len(prop_values) == 0:
				continue
			columns = ["revision_id"]
			has_property = (table!="labels_etc")
			if has_property:
				columns.append("property")
			columns += prop_values[0].value.db_fields()
			rows = []
			for prop_value in prop_values:
				row = [ self.revision_id ]
				if has_property:
					row.append(prop_value.prop)
				row += prop_value.value.db_values(db)
				rows.append(row)
			db.insert_group(table,columns,rows)

	def sanitize_property(self,prop) -> int:
		prop = str(prop).strip().upper()
		if prop[0]=="P":
			prop = prop[1:]
		return int(prop)

	def add_label_etc(self,value,type_name,language):
		if value is None or value.strip()=="":
			return
		if type_name is None or type_name.strip()=="":
			return
		if language is None or language.strip()=="":
			return
		v = LabelsEtcValue(value,type_name,language)
		self.values["labels_etc"].append(PropertyValue(0,v))

	def add_string(self,prop,string: str):
		if string is None or string.strip()=="":
			return
		prop = self.sanitize_property(prop)
		value = StringValue(string)
		self.values["string"].append(PropertyValue(prop,value))

	def add_item(self,prop,q: str):
		if q is None or q.strip()=="":
			return
		prop = self.sanitize_property(prop)
		item = ItemValue(q)
		self.values["item"].append(PropertyValue(prop,item))

	def add_time(self,prop,tv: TimeValue):
		prop = self.sanitize_property(prop)
		self.values["time"].append(PropertyValue(prop,tv))

	def add_location(self, prop, latitude: float, longitude: float):
		prop = self.sanitize_property(prop)
		value = LocationValue(latitude, longitude)
		self.values["location"].append(PropertyValue(prop,value))

	def add_freetext(self,prop,string: str):
		if string is None or string.strip()=="":
			return
		prop = self.sanitize_property(prop)
		value = FreetextValue(string)
		self.values["freetext"].append(PropertyValue(prop,value))

	def add_monolingual_text(self, prop, language: str, value: str):
		prop = self.sanitize_property(prop)
		value = MonolingualStringValue(language, value)
		self.values["monolingual_string"].append(PropertyValue(prop,value))

	def is_valid(self):
		return self.id is not None

	def check_valid(self):
		if not self.is_valid():
			raise Exception("Entry not valid: "+str(self))

