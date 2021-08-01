import datetime

class Value(metaclass=abc.ABCMeta):
	@abc.abstractmethod
	def db_table(self):
		"""Returns the name of the database table coreesponding to the type."""

	@abc.abstractmethod
	def db_fields(self):
		"""Returns the fields in the table(), except for id and revision_id"""

	@abc.abstractmethod
	def db_values(self, db):
		"""Returns the values for the database table in the same order as "fields()".
		Requires db to query/create text values.
		"""

class TimeValue(Value):
	def __init__(self, dt=None, ymd=None, precision=9, tv=None ):
		if dt is None and ymd is not None:
			dt = datetime.datetime(int(ymd[0]),int(ymd[1]),int(ymd[2]))
		self.time_value = tv
		self.precision = int(precision)
		if tv is None and dt is not None and precision is not None:
			self.set_from_datetime(dt)

	def set_from_datetime(self, dt):
		self.time_value = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
		if not self.time_value.startswith("-"):
			self.time_value = "+" + self.time_value

	def __str__(self):
		return self.time_value+"/"+str(self.precision)

	def db_table(self):
		return "time"

	def db_fields(self):
		return ["value","precision"]

	def db_values(self, db):
		return [str(self.time_value),str(self.precision)]
