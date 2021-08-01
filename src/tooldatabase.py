import pymysql
import toolforge

class ToolDatabase :
	def __init__(self):
		self.connection = toolforge.toolsdb("s54821__cersei_p")

	def get_entry_id_for_scraper_and_id(self, scraper_id, source_id) -> int:
		source_text_id = self.get_or_create_text(source_id)
		return self.__get_or_create("entry",("scraper_id","source_text_id"),(scraper_id,source_text_id))

	def get_current_revision_id(self, entry_id) -> int:
		with self.connection.cursor() as cursor:
			sql = "SELECT `current_revision_id` FROM `entry` WHERE `id`=%s"
			cursor.execute(sql, (entry_id, ))
			result = cursor.fetchone()
			self.connection.commit()
			if result is None:
				raise Exception("Expected entry ID '"+str(entry_id)+"' is not in database")
			return result[0]

	def create_new_revision(self, entry_id) -> int:
		with self.connection.cursor() as cursor:
			sql = "INSERT INTO `revision` (`entry_id`) VALUES (%s)"
			cursor.execute(sql, (entry_id, ))
			self.connection.commit()
			return cursor.lastrowid

	def set_current_revision(self,entry_id,revision_id):
		with self.connection.cursor() as cursor:
			sql = "UPDATE `entry` SET `current_revision_id`=%s WHERE `id`=%s"
			cursor.execute(sql, (revision_id, entry_id))
			self.connection.commit()

	def insert_group(self, table: str, columns: list,rows: list):
		if len(rows) == 0:
			return
		with self.connection.cursor() as cursor:
			values = []
			sql = "INSERT IGNORE INTO `"+table+"` (`"+"`,`".join(columns)+"`) VALUES "
			for rownum,row in enumerate(rows):
				if rownum>0:
					sql += ","
				sql += "("
				for valnum,value in enumerate(row):
					if valnum>0:
						sql += ","
					sql += "%s"
					values.append(value)
				sql += ")"
			print (sql)
			print (values)
			cursor.execute(sql, values)
			self.connection.commit()

	def load_table_for_revision(self,revision_id,table):
		with self.connection.cursor(pymysql.cursors.DictCursor) as cursor:
			sql = "SELECT * FROM `"+table+"` WHERE `revision_id`=%s"
			cursor.execute(sql, (revision_id,))
			return cursor.fetchall()


	"""Returns the ID of the text, if it is in the `text` table.
	Creates a new row if not, and returns the new ID.
	"""
	def get_or_create_text(self,value) -> int:
		value = str(value).strip()
		if value == "":
			return None
		return self.__get_or_create("text",("value",),(value,))

	def __get_or_create(self,table: str, keys: list, values: list) -> int:
		with self.connection.cursor() as cursor:
			sql = "SELECT `id` FROM `"+table+"` WHERE "
			for num,key in enumerate(keys):
				if num>0:
					sql += " AND "
				sql += "`"+key+"`=%s"
			cursor.execute(sql, values)
			result = cursor.fetchone()
			self.connection.commit()
			if result is not None:
				return result[0]

			sql = "INSERT IGNORE INTO `"+table+"` (`"+"`,`".join(keys)+"`) VALUES ("
			for num,value in enumerate(values):
				if num>0:
					sql += ","
				sql += "%s"
			sql += ")"
			cursor.execute(sql, values)
			self.connection.commit()
			return cursor.lastrowid
