import abc
import datetime
import enum
import json

from src.wikidata_entity import *


class Value(metaclass=abc.ABCMeta):
    def __init__(self):
        self.qualifiers = []
        self.references = []

    @abc.abstractmethod
    def db_table(self):
        """Returns the name of the database table coreesponding to the type."""

    @abc.abstractmethod
    def db_fields(self):
        """Returns the fields in the table(), except for id, revision_id, and property"""

    @abc.abstractmethod
    def db_values(self, db):
        """Returns the values for the database table in the same order as "fields()".
        Requires db to query/create text values.
        """

    @abc.abstractmethod
    def as_wikidata_claim(self, prop):
        """Returns the value as a Wikidata claim"""

    def str_ref_qual(self):
        """Returns a string representation of the references and qualifiers."""
        if len(self.references) + len(self.qualifiers) == 0:
            return ""
        ret = ""
        if len(self.qualifiers) > 0:
            parts = []
            for qual in self.qualifiers:
                parts.append(f"{qual.prop}:{qual.value}")
            ret += " {" + " | ".join(parts) + "}"
        if len(self.references) > 0:
            ret += " /TODO REFERENCES/"
        return ret

    def add_references_qualifiers(self, claim):
        return claim  # TODO

    def json_compatible(self):
        """Returns a JSON-compatible representation of the value."""
        ret = self.__dict__
        ret["classname"] = type(self).__name__
        return ret

    def get_qualifiers_text_id(self, db):
        if len(self.qualifiers) == 0:
            return None
        qualifiers = []
        for qual in self.qualifiers:
            qual_new = {}
            qual_new["prop"] = qual.prop
            qual_new["value"] = qual.value.json_compatible()
            qualifiers.append(qual_new)
        qualifiers_text = json.dumps(qualifiers, sort_keys=True)
        qualifiers_text_id = db.get_or_create_text(qualifiers_text)
        return qualifiers_text_id


class StringValue(Value):
    def __init__(self, value):
        super().__init__()
        self.value = value

    def __str__(self):
        return self.value + self.str_ref_qual()

    def __lt__(self, other):
        return self.value < other.value

    def db_table(self):
        return "string"

    def db_fields(self):
        return ["text_id"]

    def db_values(self, db):
        text_id = db.get_or_create_text(self.value)
        return [text_id]

    def as_wikidata_claim(self, prop):
        return self.add_references_qualifiers(WikidataClaimString(prop, self.value))


class FreetextValue(Value):
    def __init__(self, value):
        super().__init__()
        self.value = value[:240]  # column in VARBINARY(255)

    def __str__(self):
        return self.value + self.str_ref_qual()

    def __lt__(self, other):
        return self.value < other.value

    def db_table(self):
        return "freetext"

    def db_fields(self):
        return ["text_id"]

    def db_values(self, db):
        text_id = db.get_or_create_text(self.value)
        return [text_id]

    def as_wikidata_claim(self, prop):
        return self.add_references_qualifiers(WikidataClaimString(prop, self.value))


class ScraperItemValue(Value):
    def __init__(self, scraper_id: int, ext_id: str):
        super().__init__()
        self.scraper_id = scraper_id
        self.ext_id = ext_id

    def __str__(self):
        return f"{self.scraper_id}:{self.ext_id}" + self.str_ref_qual()

    def __lt__(self, other):
        if self.scraper_id == other.scraper_id:
            return self.ext_id < other.ext_id
        return self.scraper_id < other.scraper_id

    def db_table(self):
        return "scraper_item"

    def db_fields(self):
        return ["scraper_id", "ext_id"]

    def db_values(self, db):
        text_id = db.get_or_create_text(self.ext_id)
        return [self.scraper_id, text_id]

    def as_wikidata_claim(self, prop):
        return self.add_references_qualifiers(WikidataClaimString(prop, self.__str__))


class EntityLetterToType(str, enum.Enum):
    P = "property"
    Q = "item"
    L = "lexeme"


class ItemValue(Value):
    def __init__(self, q):
        super().__init__()
        q = q.strip().upper()
        if len(q) < 2:
            raise Exception("ItemValue: " + q + " is too short")
        letter = str(q[0])
        try:
            self.item_type = EntityLetterToType[letter]
        except:
            raise Exception("ItemValue: " + q + " has unknown letter '" + letter + "'")
        number_string = str(q[1:])
        if not number_string.isnumeric():
            raise Exception("ItemValue: " + number_string + " is not numeric")
        self.item_id = int(number_string)

    def __str__(self):
        return str(self.item_id) + " (" + self.item_type + ")" + self.str_ref_qual()

    def __lt__(self, other):
        if self.item_type != other.item_type:
            return self.item_type < other.item_type
        return self.item_id < other.item_id

    def json_compatible(self):
        ret = self.__dict__
        ret["classname"] = type(self).__name__
        ret["item_type"] = self.item_type.name
        if len(ret["references"]) == 0:
            ret.pop("references", None)
        if len(ret["qualifiers"]) == 0:
            ret.pop("qualifiers", None)
        return ret

    def db_table(self):
        return "item"

    def db_fields(self):
        return ["item_id", "item_type"]

    def db_values(self, db):
        return [self.item_id, self.item_type]

    def as_wikidata_claim(self, prop):
        q = str(self.item_type.name) + str(self.item_id)
        return self.add_references_qualifiers(WikidataClaimEntity(prop, q))


class LabelsEtcValue(Value):
    ALLOWED_TYPES = ("original_label", "label", "alias", "description", "url")

    def __init__(self, value, type_name, language):
        super().__init__()
        self.type_name = str(type_name).strip().lower()
        if self.type_name not in self.ALLOWED_TYPES:
            raise Exception(
                "LabelsEtcValue: Type name '"
                + type_name
                + "' is not valid; allowed: "
                + str(self.ALLOWED_TYPES)
            )
        self.value = str(value).strip()
        self.language = str(language).strip()

    def __str__(self):
        return (
            self.type_name.upper()
            + " "
            + self.language
            + ":"
            + self.value
            + self.str_ref_qual()
        )

    def __lt__(self, other):
        if self.type_name != other.type_name:
            return self.type_name < other.type_name
        if self.language != other.language:
            return self.language < other.language
        return self.value < other.value

    def db_table(self):
        return "labels_etc"

    def db_fields(self):
        return ["type_name", "language_id", "text_id"]

    def db_values(self, db):
        language_id = db.get_or_create_text(self.language)
        text_id = db.get_or_create_text(self.value)
        return [self.type_name, language_id, text_id]

    def as_wikidata_claim(self, prop):
        pass


class MonolingualStringValue(Value):
    def __init__(self, language, value):
        super().__init__()
        self.value = str(value).strip()
        self.language = str(language).strip()

    def __str__(self):
        return self.language + ":" + self.value + self.str_ref_qual()

    def __lt__(self, other):
        if self.language != other.language:
            return self.language < other.language
        return self.value < other.value

    def db_table(self):
        return "monolingual_string"

    def db_fields(self):
        return ["language_id", "text_id"]

    def db_values(self, db):
        language_id = db.get_or_create_text(self.language)
        text_id = db.get_or_create_text(self.value)
        return [language_id, text_id]

    def as_wikidata_claim(self, prop):
        return self.add_references_qualifiers(
            WikidataClaimMonolingualText(prop, self.language, self.value)
        )


class TimeValue(Value):
    def __init__(self, dt=None, ymd=None, precision=9, tv=None):
        super().__init__()
        if dt is None and ymd is not None:
            dt = datetime.datetime(int(ymd[0]), int(ymd[1]), int(ymd[2]))
        self.time_value = tv
        self.precision = int(precision)
        if tv is None and dt is not None and precision is not None:
            self.set_from_datetime(dt)

    def set_from_datetime(self, dt):
        self.time_value = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        if not self.time_value.startswith("-"):
            self.time_value = "+" + self.time_value

    def __str__(self):
        return self.time_value + "/" + str(self.precision) + self.str_ref_qual()

    def __lt__(self, other):
        if self.precision != other.precision:
            return self.precision < other.precision
        return self.time_value < other.time_value

    def db_table(self):
        return "time"

    def db_fields(self):
        return ["value", "precision"]

    def db_values(self, db):
        return [str(self.time_value), self.precision]

    def as_wikidata_claim(self, prop):
        return self.add_references_qualifiers(
            WikidataClaimTime(prop, self.time_value, self.precision)
        )


class LocationValue(Value):
    def __init__(self, latitude: float, longitude: float):
        super().__init__()
        self.latitude = latitude
        self.longitude = longitude

    def __str__(self):
        return str(self.latitude) + ", " + str(self.longitude) + self.str_ref_qual()

    def __lt__(self, other):
        if self.latitude != other.latitude:
            return self.latitude < other.latitude
        return self.longitude < other.longitude

    def db_table(self):
        return "location"

    def db_fields(self):
        return ["latitude", "longitude"]

    def db_values(self, db):
        return [self.latitude, self.longitude]

    def as_wikidata_claim(self, prop):
        return self.add_references_qualifiers(
            WikidataClaimLocation(prop, self.latitude, self.longitude)
        )


class QuantityValue(Value):
    def __init__(self, amount: float, unit=None):
        super().__init__()
        self.amount = amount
        self.unit = unit

    def __str__(self):
        ret = f"{self.amount}"
        if self.unit is not None:
            ret += f" [{self.unit}]"
        return ret + self.str_ref_qual()

    def __lt__(self, other):
        # NOTE: This is not numerical, normalized order, but /some/ order
        if self.amount != other.amount:
            return self.latitude < amount.amount
        if self.unit is None:
            return True
        if other.unit is None:
            return False
        return self.unit < other.unit

    def db_table(self):
        return "quantity"

    def db_fields(self):
        return ["amount", "unit"]

    def db_values(self, db):
        return [self.amount, self.unit]

    def as_wikidata_claim(self, prop):
        return self.add_references_qualifiers(
            WikidataClaimQuantity(prop, self.amount, self.unit)
        )
