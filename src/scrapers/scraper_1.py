import re

import requests
from bs4 import BeautifulSoup

from src.entry import Entry
from src.scraper_base import ScraperBase
from src.values import TimeValue


class Scraper1(ScraperBase):
    """Slovnikceske Literatury CZ"""

    def __init__(self):
        super().__init__(1)

    def scrape_everything(self):
        self.scrape_everything_via_index()

    def paginate_index(self):
        pageNr = 0
        while True:
            try:
                url = "http://slovnikceskeliteratury.cz/list.jsp?show=-&order=title&ascDesc=asc&startswith="
                page = requests.post(
                    url, data={"page": 1, "pageNr": pageNr}, timeout=60
                )
                html = page.text
                yield html
                soup = BeautifulSoup(html, features="html.parser")
                if (
                    len(
                        soup.find_all(
                            "input", attrs={"class": "submitButton", "name": "next"}
                        )
                    )
                    == 0
                ):
                    break  # No "next" button, quit
            except Exception as err:
                print(f"Unexpected {err}")
            pageNr += 1

    def entry_url_relative2full(self, url):
        return re.sub(
            r"^\./showContent\.jsp",
            "http://www.slovnikceskeliteratury.cz/showContent.jsp",
            url,
        )

    def parse_index_page(self, html):
        soup = BeautifulSoup(html, features="html.parser")
        for div in soup.find_all("div", class_="result"):
            entry = Entry(self.scraper_id)
            descs = []
            is_human = False
            for link in div.find_all("a", class_="result_name"):
                original_label = link.get_text().strip()
                pretty_label = original_label
                pretty_label = re.sub(r"^.+? - +", "", pretty_label)
                pretty_label = re.sub(r"\s*\(.*$", "", pretty_label)
                pretty_label = re.sub(r"^(.+?), (.+)$", r"\2 \1", pretty_label)
                entry.add_label_etc(original_label, "original_label", self.language)
                entry.add_label_etc(pretty_label, "label", self.language)
                href = link.get("href")
                if href is not None:
                    m = re.match(r"^.*docId=(\d+).*$", href)
                    if m:
                        entry.id = m.group(1)
                        url = self.construct_entry_url_from_id(entry.id)
                        entry.add_label_etc(url, "url", self.language)
            for span in div.find_all("span", class_="datumnarozeni"):
                s = re.sub(r"\s+", " ", span.get_text()).strip()
                if self.parse_date_prop(entry, s, "P569"):
                    is_human = True
                descs.append(s)
            for span in div.find_all("span", class_="mistonarozeni"):
                s = re.sub(r"\s+", " ", span.get_text()).strip()
                self.add_to_item_or_freetext("P19", s, entry)
                descs.append(s)
            for span in div.find_all("span", class_="datumumrti"):
                s = re.sub(r"\s+", " ", span.get_text()).strip()
                if self.parse_date_prop(entry, s, "P570"):
                    is_human = True
                descs.append(s)
            for span in div.find_all("span", class_="mistoumrti"):
                s = re.sub(r"\s+", " ", span.get_text()).strip()
                self.add_to_item_or_freetext("P20", s, entry)
                descs.append(s)
            self.add_description_list(descs, entry)
            if is_human:
                entry.add_item("P31", "Q5")
            if entry.is_valid():
                yield entry

    def parse_date_prop(self, entry, date_string, prop):
        date_string = date_string.strip()
        if date_string == "":
            return False
        try:
            m = re.match(r"^[*†]\D*(\d{1,2})\. *(\d{1,2})\. *(\d{3,4})", date_string)
            if m:
                day = int(m.group(1))
                month = int(m.group(2))
                year = int(m.group(3))
                tv = TimeValue(ymd=(year, month, day), precision=11)
                entry.add_time(prop, tv)
                return True
            else:  # Couldn't parse date
                entry.add_freetext(prop, date_string)
                return False
        except:  # Couldn't parse date
            entry.add_freetext(prop, date_string)
            return False
