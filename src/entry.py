import json
from collections import OrderedDict

from src.values import *
from src.wikidata_entity import *


class PropertyValue:
    def __init__(self, prop, value):
        self.prop = self.sanitize_property(prop)
        self.value = value

    def as_wikidata_claim(self):
        return self.value.as_wikidata_claim(self.property_text())

    def property_text(self) -> str:
        return "P" + str(self.prop)

    def sanitize_property(self, prop) -> int:
        prop = str(prop).strip().upper()
        if prop[0] == "P":
            prop = prop[1:]
        return int(prop)

    def __str__(self):
        return self.property_text() + ":" + str(self.value)

    def __repr__(self):
        return self.__str__()

    def __lt__(self, other):
        if self.prop < other.prop:
            return self.prop < other.prop
        return self.value < other.value

    def __gt__(self, other):
        return other < self

    def __eq__(self, other):
        return not (self < other) and not (other < self)

    def __ne__(self, other):
        return not (self == other)


class Entry:
<<<<<<< HEAD
	VALUE_TABLES = ["string","item","time","location","quantity","freetext","monolingual_string","labels_etc","scraper_item"]

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
				elif table=="quantity":
					o = QuantityValue(amount=row["amount"],unit=row["unit"])
				elif table=="freetext":
					o = FreetextValue(self.decode(row["value"]))
				elif table=="scraper_item":
					o = ScraperItemValue(row["scraper_id"],self.decode(row["ext_id"]))
				elif table=="monolingual_string":
					o = MonolingualStringValue(self.decode(row["language"]),self.decode(row["value"]))
				elif table=="labels_etc":
					o = LabelsEtcValue(self.decode(row["value"]),row["type_name"],self.decode(row["language"]))
				if 'qualifiers' in row and row['qualifiers'] is not None:
					try:
						o.qualifiers = []
						for qual in json.loads(self.decode(row["qualifiers"])):
							print (f"{qual}")
							prop = qual["prop"]
							qv = qual["value"]
							if "classname" not in qv:
								print (f"load_from_revision: No classname {qual}", file=sys.stderr)
								exit(0)
							if qv["classname"]=='ItemValue':
								value = ItemValue(f"{qv['item_type']}{qv['item_id']}")
							else:
								print (f"load_from_revision: Unknown pv {qual}", file=sys.stderr)
								exit(0)
							nq = PropertyValue(prop,value)
							o.qualifiers.append(nq)
					except Exception as err:
						print(f"Entry::load_from_revision: qualifier load error {row}", file=sys.stderr)
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
			if table in ["labels_etc","freetext","scraper_item"]:
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
				value = {"property":freetext.property_text(),"value":str(freetext.value)}
				j["freetext"].append(value)
			j["scraper_item"] = []
			for scraper_item in (self.values["scraper_item"] or []):
				value = {"property":scraper_item.property_text(),"scraper_id":scraper_item.value.scraper_id,"ext_id":scraper_item.value.ext_id}
				j["scraper_item"].append(value)
			for property_value in self.values["labels_etc"]:
				value = property_value.value
				if value.type_name=="url":
					j["external_url"] = value.value

		j = self.dict_deep_sort(j)
		return json.dumps(j)

	def has_revision_changed(self,db):
		json = self.as_json(True)
		json_other = db.get_revision_item(self.revision_id)
		return (json!=json_other) # TODO test
		"""
=======
    VALUE_TABLES = [
        "string",
        "item",
        "time",
        "location",
        "quantity",
        "freetext",
        "monolingual_string",
        "labels_etc",
        "scraper_item",
    ]

    def __init__(self, scraper_id):
        self.scraper_id = scraper_id
        self.id = None
        self.entry_id = None
        self.revision_id = None
        self.values = {}
        for table in self.VALUE_TABLES:
            self.values[table] = []

    def __str__(self):
        if self.id is None:
            return "<UNINITIALIZED ENTRY>"
        ret = ""
        for key in ["id", "revision_id"]:
            ret += key + ": " + str(self.__dict__[key]) + "\n"
        for key, values in self.values.items():
            if len(values) == 0:
                continue
            ret += key + ":\n"
            for item in values:
                ret += "  " + str(item) + "\n"
        return ret

    def decode(self, s):
        return s.decode(encoding="utf8", errors="ignore")

    def load_from_revision(self, db, revision_id):
        self.revision_id = int(revision_id)
        for table, value in self.values.items():
            query_table = "vw_" + table
            rows = db.load_table_for_revision(self.revision_id, query_table)
            if rows is None:
                continue
            for row in rows:
                if table == "string":
                    o = StringValue(self.decode(row["value"]))
                elif table == "item":
                    o = ItemValue(row["q"])
                elif table == "time":
                    o = TimeValue(tv=row["value"], precision=row["precision"])
                elif table == "location":
                    o = LocationValue(
                        latitude=row["longitude"], longitude=row["longitude"]
                    )
                elif table == "quantity":
                    o = QuantityValue(amount=row["amount"], unit=row["unit"])
                elif table == "freetext":
                    o = FreetextValue(self.decode(row["value"]))
                elif table == "scraper_item":
                    o = ScraperItemValue(row["scraper_id"], self.decode(row["ext_id"]))
                elif table == "monolingual_string":
                    o = MonolingualStringValue(
                        self.decode(row["language"]), self.decode(row["value"])
                    )
                elif table == "labels_etc":
                    o = LabelsEtcValue(
                        self.decode(row["value"]),
                        row["type_name"],
                        self.decode(row["language"]),
                    )
                if "qualifiers" in row and row["qualifiers"] is not None:
                    try:
                        o.qualifiers = []
                        for qual in json.loads(self.decode(row["qualifiers"])):
                            print(f"{qual}")
                            prop = qual["prop"]
                            qv = qual["value"]
                            if "classname" not in qv:
                                print(
                                    f"load_from_revision: No classname {qual}",
                                    file=sys.stderr,
                                )
                                exit(0)
                            if qv["classname"] == "ItemValue":
                                value = ItemValue(f"{qv['item_type']}{qv['item_id']}")
                            else:
                                print(
                                    f"load_from_revision: Unknown pv {qual}",
                                    file=sys.stderr,
                                )
                                exit(0)
                            nq = PropertyValue(prop, value)
                            o.qualifiers.append(nq)
                    except Exception as err:
                        print(
                            f"Entry::load_from_revision: qualifier load error {row}",
                            file=sys.stderr,
                        )
                prop = 0
                if "property" in row:
                    prop = int(row["property"])
                self.values[table].append(PropertyValue(prop, o))

    def dict_deep_sort(self, obj):
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

        for table, values in self.values.items():
            if table in ["labels_etc", "freetext", "scraper_item"]:
                continue
            for o in values:
                entity.claims.append(o.as_wikidata_claim())

        for property_value in self.values["labels_etc"]:
            value = property_value.value
            label = WikidataLabel(value.language, value.value)
            if value.type_name == "label":
                entity.labels.append(label)
            elif value.type_name == "description":
                entity.descriptions.append(label)
            elif value.type_name == "original_label":
                entity.aliases.append(label)
            elif value.type_name == "alias":
                entity.aliases.append(label)

        j = entity.as_dict()
        if internal_use:
            # TODO add other stuff
            j["freetext"] = []
            for freetext in self.values["freetext"]:
                value = {
                    "property": freetext.property_text(),
                    "value": str(freetext.value),
                }
                j["freetext"].append(value)
            j["scraper_item"] = []
            for scraper_item in self.values["scraper_item"] or []:
                value = {
                    "property": scraper_item.property_text(),
                    "scraper_id": scraper_item.value.scraper_id,
                    "ext_id": scraper_item.value.ext_id,
                }
                j["scraper_item"].append(value)

        j = self.dict_deep_sort(j)
        return json.dumps(j)

    def has_revision_changed(self, db):
        json = self.as_json(True)
        json_other = db.get_revision_item(self.revision_id)
        return json != json_other  # TODO test
        """
>>>>>>> d1300fc92cf01dad7209654770354febc4de859c
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

    def create_or_update_in_database(self, db):
        self.check_valid()
        if self.entry_id is None:
            self.entry_id = db.get_entry_id_for_scraper_and_id(self.scraper_id, self.id)
        self.revision_id = db.get_current_revision_id(self.entry_id)
        rev_changed = self.has_revision_changed(db)
        if self.revision_id != 0 and not rev_changed:
            return
        self.revision_id = db.create_new_revision(self.entry_id)
        j = self.as_json(True)
        db.set_revision_item(self.revision_id, j)
        db.set_current_revision(self.entry_id, self.revision_id)

        # Generate INSERT statements
        for table, prop_values in self.values.items():
            if len(prop_values) == 0:
                continue
            columns = ["revision_id"]
            has_property = table != "labels_etc"
            has_qualifiers = has_property
            if has_property:
                columns.append("property")
            if has_qualifiers:
                columns.append("qualifiers_text_id")
            columns += prop_values[0].value.db_fields()
            rows = []
            for prop_value in prop_values:
                row = [self.revision_id]
                if has_property:
                    row.append(prop_value.prop)
                if has_qualifiers:
                    qualifiers = prop_value.value.get_qualifiers_text_id(db)
                    row.append(qualifiers)
                row += prop_value.value.db_values(db)
                rows.append(row)
            db.insert_group(table, columns, rows)

    def sanitize_property(self, prop) -> int:
        prop = str(prop).strip().upper()
        if prop[0] == "P":
            prop = prop[1:]
        return int(prop)

    def add_label_etc(self, value, type_name, language):
        if value is None or value.strip() == "":
            return
        if type_name is None or type_name.strip() == "":
            return
        if language is None or language.strip() == "":
            return
        if len(value) > 250:
            value = value[0:250]
        v = LabelsEtcValue(value, type_name, language)
        self.values["labels_etc"].append(PropertyValue(0, v))

    def add_string(self, prop, string: str, references=[], qualifiers=[]):
        if string is None or string.strip() == "":
            return
        prop = self.sanitize_property(prop)
        value = StringValue(string)
        value.references = references
        value.qualifiers = qualifiers
        self.values["string"].append(PropertyValue(prop, value))

    def add_item(self, prop, q: str, references=[], qualifiers=[]):
        if q is None or q.strip() == "":
            return
        prop = self.sanitize_property(prop)
        item = ItemValue(q)
        item.references = references
        item.qualifiers = qualifiers
        self.values["item"].append(PropertyValue(prop, item))

    def add_time(self, prop, tv: TimeValue, references=[], qualifiers=[]):
        prop = self.sanitize_property(prop)
        if len(tv.references) == 0:
            tv.references = references
        if len(tv.qualifiers) == 0:
            tv.qualifiers = qualifiers
        self.values["time"].append(PropertyValue(prop, tv))

    def add_location(
        self, prop, latitude: float, longitude: float, references=[], qualifiers=[]
    ):
        prop = self.sanitize_property(prop)
        value = LocationValue(latitude, longitude)
        value.references = references
        value.qualifiers = qualifiers
        self.values["location"].append(PropertyValue(prop, value))

    def add_quantity(
        self, prop, amount: float, unit=None, references=[], qualifiers=[]
    ):
        prop = self.sanitize_property(prop)
        value = QuantityValue(amount, unit)
        value.references = references
        value.qualifiers = qualifiers
        self.values["quantity"].append(PropertyValue(prop, value))

    def add_freetext(self, prop, string: str, references=[], qualifiers=[]):
        if string is None or string.strip() == "" or string.strip() == "?":
            return
        prop = self.sanitize_property(prop)
        value = FreetextValue(string)
        value.references = references
        value.qualifiers = qualifiers
        self.values["freetext"].append(PropertyValue(prop, value))

    def add_scraper_item(
        self, prop, scraper_id: int, ext_id: str, references=[], qualifiers=[]
    ):
        if prop is None or prop <= 0:
            return
        if scraper_id is None or scraper_id <= 0:
            return
        ext_id = str(ext_id)
        if ext_id is None or ext_id.strip() == "":
            return
        prop = self.sanitize_property(prop)
        scraper_id = int(scraper_id)
        value = ScraperItemValue(scraper_id, ext_id)
        value.references = references
        value.qualifiers = qualifiers
        self.values["scraper_item"].append(PropertyValue(prop, value))

    def add_monolingual_text(
        self, prop, language: str, value: str, references=[], qualifiers=[]
    ):
        prop = self.sanitize_property(prop)
        value = MonolingualStringValue(language, value)
        value.references = references
        value.qualifiers = qualifiers
        self.values["monolingual_string"].append(PropertyValue(prop, value))

    def is_valid(self):
        return self.id is not None

    def check_valid(self):
        if not self.is_valid():
            raise Exception("Entry not valid: " + str(self))
