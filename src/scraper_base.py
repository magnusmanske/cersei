import abc
from src.ToolDatabase import ToolDatabase

class ScraperBase(metaclass=abc.ABCMeta):
	def __init__(self):
		self.enforce_unique_ids = True
		self.url_pattern = ''
		self.scraper_id = 1
		self.db = None

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
