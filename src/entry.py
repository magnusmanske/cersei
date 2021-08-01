class Entry:
	def __init__(self):
		self.id = ''
		self.revision_id = None
		self.values = {
			"string":[],
			"item":[],
			"time":[],
			"freetext":[],
			"labels_etc":[]
		}

	def __str__(self):
		ret = ""
		for key in ["id","revision_id"]:
			ret += key+": "+self.__dict__[key]+"\n"
		for (key,values) in self.values.items():
			if len(values)==0:
				continue
			ret += key+":\n"
			for item in values:
				ret += "\t"+item[0]+": "+str(item[1])+"\n"
		return ret
		#return str(self.__class__) + '\n' + '\n'.join(('{} = {}'.format(item, self.__dict__[item]) for item in self.__dict__))

	def create_or_update_in_database(self,db):
		pass

	def sanitize_property(self,prop):
		return prop.strip().upper()

	def add_string(self,prop,s):
		prop = self.sanitize_property(prop)
		s = s.strip()
		if s!="":
			self.values["string"].append((prop,s))

	def add_item(self,prop,q):
		prop = self.sanitize_property(prop)
		self.values["item"].append((prop,q))

	def add_time(self,prop,tv):
		prop = self.sanitize_property(prop)
		self.values["time"].append((prop,tv))

	def add_freetext(self,prop,s):
		prop = self.sanitize_property(prop)
		s = s.strip()
		if s!="":
			self.values["freetext"].append((prop,s))

	def is_valid(self):
		return self.original_label!='' and self.id!=''
