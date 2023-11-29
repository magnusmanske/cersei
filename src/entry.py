from collections import OrderedDict
from src.values import *
from src.wikidata_entity import *
import json

class PropertyValue:
	def __init__(self, prop, value):
		self.prop = prop
		self.value = value

	def as_wikidata_claim(self):
		return self.value.as_wikidata_claim(self.property_text())

	def property_text(self)->str:
		return "P"+str(self.prop)

	def __str__(self):
		return self.property_text()+":"+str(self.value)

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
				ret += "  "+str(item)+"\n"
		return ret

	def decode(self,s):
		return s.decode(encoding='utf8',errors='ignore')

	def load_from_revision(self,db,revision_id):
		self.revision_id = int(revision_id)
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
					o = StringValue(self.decode(row["value"]))
				elif table=="item":
					o = ItemValue(row["q"])
				elif table=="time":
					o = TimeValue(tv=row["value"],precision=row["precision"])
				elif table=="location":
					o = LocationValue(latitude=row["longitude"],longitude=row["longitude"])
				elif table=="freetext":
					o = FreetextValue(self.decode(row["value"]))
				elif table=="monolingual_string":
					o = MonolingualStringValue(self.decode(row["language"]),self.decode(row["value"]))
				elif table=="labels_etc":
					o = LabelsEtcValue(self.decode(row["value"]),row["type_name"],self.decode(row["language"]))
				prop = 0
				if "property" in row:
					prop = int(row["property"])
				self.values[table].append(PropertyValue(prop,o))

	def dict_deep_sort(self,obj):
	    if isinstance(obj, dict):
	        obj = OrderedDict(sorted(obj.items()))
	        for k, v in obj.items():
	            if isinstance(v, dict) or isinstance(v, list):
	                obj[k] = self.dict_deep_sort(v)

	    if isinstance(obj, list):
	        for i, v in enumerate(obj):
	            if isinstance(v, dict) or isinstance(v, list):
	                obj[i] = self.dict_deep_sort(v)
	        obj = sorted(obj, key=lambda x: json.dumps(x))

	    return obj

	def as_json(self, internal_use: bool):
		entity = WikidataEntity()
		entity.entity_type = WikidataEntityType.ITEM

		for table,values in self.values.items():
			if table in ["labels_etc","freetext"]:
				continue
			for o in values:
				entity.claims.append(o.as_wikidata_claim())

		for property_value in self.values["labels_etc"]:
			value = property_value.value
			label = WikidataLabel(value.language,value.value)
			if value.type_name=="label":
				entity.labels.append(label)
			elif value.type_name=="description":
				entity.descriptions.append(label)
			elif value.type_name=="original_label":
				entity.aliases.append(label)
			elif value.type_name=="alias":
				entity.aliases.append(label)

		j = entity.as_dict()
		if internal_use:
			# TODO add other stuff
			j["freetext"] = []
			for freetext in self.values["freetext"]:
				label = {"property":freetext.property_text(),"value":str(freetext.value)}
				j["freetext"].append(label)

		j = self.dict_deep_sort(j)
		return json.dumps(j)

	def has_revision_changed(self,db):
		json = entry.as_json(True)
		json_other = db.get_revision_item(self.revision_id)
		return (json!=json_other) # TODO test
		"""
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
		"""

	def create_or_update_in_database(self,db):
		self.check_valid()
		if self.entry_id is None:
			self.entry_id = db.get_entry_id_for_scraper_and_id(self.scraper_id, self.id)
		self.revision_id = db.get_current_revision_id(self.entry_id)
		if self.revision_id!=0 and not self.has_revision_changed(db):
			return
		self.revision_id = db.create_new_revision(self.entry_id)
		db.set_revision_item(self.revision_id,self.as_json(True))
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
		if len(value)>250:
			value = value[0:250]
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

