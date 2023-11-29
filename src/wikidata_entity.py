import enum
import abc

class WikidataEntityType(str, enum.Enum):
	ITEM = "Q"
	PROPERTY = "P"
	LEXEME = "L"

# For label, description, alias
class WikidataLabel:
	def __init__(self, language:str, value:str):
		self.language = language
		self.value = value

class WikidataSitelink:
	def __init__(self, wiki:str, title:str, badges = []):
		self.wiki = wiki
		self.title = title
		self.badges = badges # TODO

class WikidataClaim(metaclass=abc.ABCMeta):
	def __init__(self, prop, id = None , rank = "normal"):
		self.property = prop
		self.id = id
		self.rank = rank
		self.references = []
		self.qualifiers = []

	@abc.abstractmethod
	def as_dict(self):
		"""Returns the claim as a dict for JSON"""

	def get_base_dict(self):
		d = {"mainsnak":{"snaktype":"value","property":self.property},"type":"statement","rank":self.rank}
		if self.id is not None:
			d["id"] = self.id
		return d

class WikidataClaimString(WikidataClaim):
	def __init__(self, prop: str, text: str, rank = "normal", id = None):
		super().__init__(prop, id, rank)
		self.text = text

	def as_dict(self):
		d = self.get_base_dict()
		d["mainsnak"]["datavalue"] = {"value":self.text,"type":"string"}
		return d

class WikidataClaimMonolingualText(WikidataClaim):
	def __init__(self, prop: str, language: str, text: str, rank = "normal", id = None):
		super().__init__(prop, id, rank)
		self.language = language
		self.text = text

	def as_dict(self):
		d = self.get_base_dict()
		d["mainsnak"]["datavalue"] = {"value":{"text":self.text,"language":self.language},"type":"monolingualtext"}
		return d

class WikidataClaimEntity(WikidataClaim):
	def __init__(self, prop: str, entity: str, rank = "normal", id = None):
		super().__init__(prop, id, rank)
		self.entity = entity.strip().upper()
		self.entity_type = self.determine_entity_type()

	# TODO duplicated from WikidataEntity
	def determine_entity_type(self):
		if self.entity is None:
			return None
		for typename in WikidataEntityType:
			if self.entity.startswith(typename.value):
				return typename
		return None

	def as_dict(self):
		d = self.get_base_dict()
		d["mainsnak"]["datavalue"] = {"value":{"entity-type":self.entity_type.name.lower(),"id":self.entity},"type":"wikibase-entityid"}
		return d

class WikidataClaimTime(WikidataClaim):
	def __init__(self, prop: str, time: str, precision: int, rank = "normal", id = None, calendarmodel = "http://www.wikidata.org/entity/Q1985727"):
		super().__init__(prop, id, rank)
		self.time = time
		self.precision = precision
		self.calendarmodel = calendarmodel

	def as_dict(self):
		d = self.get_base_dict()
		d["mainsnak"]["datavalue"] = {"value":{"time":self.time,"precision":self.precision,"timezone":0,"before":0,"after":0,"calendarmodel":self.calendarmodel},"type":"time"}
		return d

class WikidataClaimLocation(WikidataClaim):
	def __init__(self, prop: str, latitude: float, longitude: float, rank = "normal", id = None, globe = "http://www.wikidata.org/entity/Q2"):
		super().__init__(prop, id, rank)
		self.latitude = latitude
		self.longitude = longitude
		self.precision = 0.01
		self.globe = globe

	def as_dict(self):
		d = self.get_base_dict()
		d["mainsnak"]["datavalue"] = {"value":{"latitude":self.latitude,"longitude":self.longitude,"precision":self.precision,"globe":self.globe},"type":"time"}
		return d



class WikidataEntity:

	def __init__(self, id = None):
		self.id = id
		self.entity_type = self.determine_entity_type()
		self.claims = []
		self.labels = []
		self.descriptions = []
		self.aliases = []
		self.sitelinks = []

	def determine_entity_type(self):
		if self.id is None:
			return None
		for typename in WikidataEntityType:
			if self.id.startswith(typename.value):
				return typename
		return None

	def as_dict(self):
		j = {"labels":{},"descriptions":{},"aliases":{},"claims":{},"sitelinks":{}}
		if self.id is not None:
			j["title"] = self.id
		if self.entity_type is not None:
			j["type"] = self.entity_type.name.lower()
		for label in self.labels:
			j["labels"][label.language] = {"language":label.language,"value":label.value}
		for alias in self.aliases:
			if alias.language not in j["aliases"]:
				j["aliases"][alias.language] = []
			j["aliases"][alias.language].append({"language":alias.language,"value":alias.value})
		for description in self.descriptions:
			j["descriptions"][description.language] = {"language":description.language,"value":description.value}
		for sitelink in self.sitelinks:
			j["sitelinks"][sitelink.wiki] = {"wiki":sitelink.wiki,"title":sitelink.title,"badges":[]} # TODO
		for claim in self.claims:
			c = claim.as_dict()
			if claim.property not in j["claims"]:
				j["claims"][claim.property] = []
			j["claims"][claim.property].append(c)

		return j
