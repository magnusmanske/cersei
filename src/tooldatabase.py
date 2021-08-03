import pymysql
import toolforge

class ToolDatabase :
	LETTER2TYPE = {
		"Q":"item",
		"P":"property",
		"L":"lexeme"
	}

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

	def get_single_row_for_id(self,table,row_id):
		with self.connection.cursor(pymysql.cursors.DictCursor) as cursor:
			sql = "SELECT * FROM `"+table+"` WHERE `id`=%s"
			cursor.execute(sql, (row_id,))
			rows = cursor.fetchall()
			if rows is not None and len(rows)>0:
				return rows[0]

	"""Finds all source IDs in a list that are not in the database for this scraper.
	Takes a dict (source_id,something).
	Returns a list of all source IDs not in the database for this scraper.
	"""
	def source_ids_in_wikidata_but_not_here(self,scraper_id: int,source2wiki: dict)->list:
		with self.connection.cursor(pymysql.cursors.DictCursor) as cursor:
			all_source_ids = {}
			for source_id in source2wiki.keys():
				all_source_ids[str(source_id)] = True

			placeholders = ["%s" for x in source2wiki.keys()]
			sql = "SELECT `source_id` FROM `vw_entry` WHERE `scraper_id`=%s AND source_id IN ("+",".join(placeholders)+")"
			params = [scraper_id]+list(source2wiki.keys())
			cursor.execute(sql, params)
			rows = cursor.fetchall()

			for row in rows:
				source_id = row["source_id"].decode('utf8')
				if source_id in all_source_ids:
					all_source_ids.pop(source_id)

			return list(all_source_ids.keys())

	def get_wikidata_mappings_for_source_ids(self,scraper_id,source_ids):
		with self.connection.cursor(pymysql.cursors.DictCursor) as cursor:
			placeholders = ["%s" for x in source_ids]
			sql = """
				SELECT `entry`.`id` AS `entry_id`,`text`.`value` AS `source_id`,`wikidata_mapping`.* FROM `entry`,`text`,`wikidata_mapping`
				WHERE `entry`.`scraper_id`=%s
				AND `entry`.`source_text_id`=`text`.`id`
				AND `wikidata_mapping`.`entry_id`=`entry`.`id`
				AND `text`.`value` IN (
				""".replace("\n"," ").strip()+",".join(placeholders)+")"
			params = [scraper_id]+list(source_ids)
			cursor.execute(sql, params)
			rows = cursor.fetchall()
			ret = {}
			for row in rows:
				row["source_id"] = row["source_id"].decode('utf8')
				row["item"] = self.construct_item(row["item_type"],row["item_id"])
				ret[row["source_id"]] = row
			return ret

	def construct_item(self,item_type,item_id):
		for letter,key in self.LETTER2TYPE.items():
			if item_type==key:
				return letter+str(item_id)
		raise Exception("ToolDatabase::construct_item: Unknown item type '"+item_type+"'")

	"""Splits an item into ajn item type string and a numeric value.
	Eg: Q12345 => ("item",12345)
	"""
	def split_item(self,item):
		item = str(item).strip().upper()
		letter = item[0]
		if letter not in self.LETTER2TYPE:
			raise Exception("ToolDatabase::split_item: '"+item+"' has unknown letter")
		item_id = item[1:]
		if not item_id.isnumeric():
			raise Exception("ToolDatabase::split_item: '"+item+"' has non-numeric part")
		return (self.LETTER2TYPE[letter],int(item_id))

	def get_entry_ids_for_source_ids(self,scraper_id,source_ids):
		with self.connection.cursor(pymysql.cursors.DictCursor) as cursor:
			placeholders = ["%s" for x in source_ids]
			sql = """
				SELECT `entry`.`id` AS `entry_id`,`text`.`value` AS `source_id` FROM `entry`,`text`
				WHERE `entry`.`scraper_id`=%s
				AND `entry`.`source_text_id`=`text`.`id`
				AND `text`.`value` IN (
				""".replace("\n"," ").strip()+",".join(placeholders)+")"
			params = [scraper_id]+list(source_ids)
			cursor.execute(sql, params)
			rows = cursor.fetchall()
			ret = {}
			for row in rows:
				ret[row["source_id"].decode('utf8')] = int(row["entry_id"])
			return ret

	def add_wikidata_mapping(self,entry_id,item,method):
		item_type,item_id = self.split_item(item)
		with self.connection.cursor() as cursor:
			sql = "REPLACE INTO `wikidata_mapping` (`entry_id`,`item_type`,`item_id`,`method`) VALUES (%s,%s,%s,%s)"
			cursor.execute(sql, (entry_id,item_type,item_id,method))
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
