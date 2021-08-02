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
			cursor.execute(sql, values)
			self.connection.commit()

	def delete_rows_by_id(self,table,ids):
		ids = list(map(lambda id:str(id), ids))
		ids = list(filter(lambda id: id.isnumeric(), ids))
		if len(ids)==0:
			return
		sql = "DELETE FROM `"+table+"` WHERE `id` IN (" + ",".join(ids) + ")"
		with self.connection.cursor() as cursor:
			cursor.execute(sql, ())
			self.connection.commit()

	def load_table_for_revision(self,revision_id,table):
		with self.connection.cursor(pymysql.cursors.DictCursor) as cursor:
			sql = "SELECT * FROM `"+table+"` WHERE `revision_id`=%s"
			cursor.execute(sql, (revision_id,))
			return cursor.fetchall()

	def find_text_item_match(self, text: str, group: str, languages: list):
		text_id = self.get_or_create_text(text)
		placeholders = ["%s" for language in languages]
		params = [text_id,group]
		params += languages
		with self.connection.cursor(pymysql.cursors.DictCursor) as cursor:
			sql = "SELECT DISTINCT `item_id` FROM `text2item` WHERE `text_id`=%s AND `group`=%s AND `language` IN ("
			sql += ",".join(placeholders) + ")"
			cursor.execute(sql, params)
			rows = cursor.fetchall()
			if len(rows)!=1: # This includes multiple candidates
				return
			return "Q"+str(rows[0]["item_id"])


	def get_item2text_candidates(self, scraper_id, min_count=3):
		with self.connection.cursor(pymysql.cursors.DictCursor) as cursor:
			sql = "SELECT * FROM vw_item_candidates WHERE scraper_id=%s AND cnt>=%s"
			cursor.execute(sql, (scraper_id,min_count))
			return cursor.fetchall()

	def add_text2item(self,language,group,text,item):
		item_id = int(str(item)[1:])
		text_id = self.get_or_create_text(text)
		with self.connection.cursor() as cursor:
			sql = "INSERT IGNORE INTO `text2item` (`language`,`group`,`text_id`,`item_id`) VALUES (%s,%s,%s,%s)"
			cursor.execute(sql, (language,group,text_id,item_id))
			return cursor.lastrowid

	def get_freetext2item(self,scraper_id,group,props,language):
		with self.connection.cursor(pymysql.cursors.DictCursor) as cursor:
			sql = """
				SELECT property,text2item.item_id,revision_id,freetext.id AS freetext_id FROM text2item,freetext,revision,entry
				WHERE text2item.text_id=freetext.text_id
				AND text2item.group=%s
				AND text2item.language IN ("",%s)
				AND revision_id=revision.id
				AND revision.entry_id=entry.id
				AND entry.scraper_id=%s
				AND freetext.property in (
			""".replace("\n"," ").strip()
			placeholders = ["%s" for props in props]
			sql += ",".join(placeholders) + ")"
			params = [group,language,scraper_id]+props
			cursor.execute(sql, params)
			return cursor.fetchall()

	def get_old_revision_id_query(self,scraper_id: int):
		sql = "SELECT revision.id FROM revision,entry WHERE entry.scraper_id="+str(scraper_id)+" AND revision.entry_id=entry.id AND revision.id!=entry.current_revision_id"
		return sql

	def clear_old_revisions_in_table(self,scraper_id: int,table: str):
		sql = "DELETE FROM `"+table+"` WHERE `revision_id` IN ("+self.get_old_revision_id_query(scraper_id)+")"
		with self.connection.cursor() as cursor:
			cursor.execute(sql, ())
			self.connection.commit()

	def clear_old_revisions(self,scraper_id: int):
		sql = self.get_old_revision_id_query(scraper_id)
		ids = []
		with self.connection.cursor() as cursor:
			cursor.execute(sql, ())
			rows = cursor.fetchall()
			for row in rows:
				ids.append(str(row[0]))
		if len(ids)==0:
			return
		sql = "DELETE FROM `revision` WHERE `id` IN ("+",".join(ids)+")"
		with self.connection.cursor() as cursor:
			cursor.execute(sql, ())
			self.connection.commit()

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
