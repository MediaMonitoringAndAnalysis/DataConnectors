"""
Low-level ReliefWeb scraping utilities.

Contains:
- :class:`ReliefArticleAPI`  – fetch article metadata via the ReliefWeb REST API.
- :class:`ReliefArticle`     – parse article detail pages via HTML scraping.
- Module-level helpers used by the scraping pipeline.
"""

from __future__ import annotations

import json
import os
import random
import re
from datetime import datetime
from importlib import resources
from itertools import groupby
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup as bs
from bs4 import NavigableString
from langdetect import detect
from nltk.tokenize import word_tokenize
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Source metadata (bundled with the package)
# ---------------------------------------------------------------------------

def _load_sources_metadata() -> dict:
    try:
        with resources.open_text(
            "data_connectors.reliefweb", "sources_metadata.json"
        ) as f:
            return json.load(f)
    except Exception:
        return {}


_SOURCES_METADATA: dict = _load_sources_metadata()

# ---------------------------------------------------------------------------
# ReliefWeb API client
# ---------------------------------------------------------------------------

class ReliefArticleAPI:
    """
    Fetch article metadata from the ReliefWeb REST API.

    Parameters
    ----------
    url:
        A ReliefWeb article URL, e.g.
        ``https://reliefweb.int/report/ukraine/situation-report-2024``.
    """

    _API_BASE = "https://api.reliefweb.int/v1"

    def __init__(self, url: str) -> None:
        self.url = url
        node_id = url.split("/")[-1]
        self.api_url = f"{self._API_BASE}/reports/{node_id}"

        response = requests.get(self.api_url)
        if response.status_code == 200:
            self.content = response.json()
        else:
            raise RuntimeError(
                f"ReliefWeb API returned status {response.status_code} for {self.api_url}"
            )

    def get_info(self) -> Dict:
        """Return a normalised dictionary of article metadata."""
        data0 = self.content["data"][0]
        fields = data0.get("fields", {})
        id_ = data0.get("id", "-")
        title = fields.get("title", "-")

        date_posted = "-"
        date_created = "-"
        if "date" in fields:
            date_posted = fields["date"].get("created", "-")
            if date_posted != "-":
                date_posted = date_posted.split("T")[0]
            date_created = fields["date"].get("original", "-")
            if date_created != "-":
                date_created = date_created.split("T")[0]

        urls = [x["url"] for x in fields.get("file", [])]
        text = fields.get("body", "-")
        doc_url = fields.get("url", "-")
        language = [x["name"] for x in fields.get("language", [])]
        primary_country = fields.get("primary_country", {}).get("name", "-")
        disaster = [x["name"] for x in fields.get("disaster", [])]
        disaster_type = [x["name"] for x in fields.get("disaster_type", [])]
        source = [x["name"] for x in fields.get("source", [])]
        format_ = [x["name"] for x in fields.get("format", [])]
        themes = [x["name"] for x in fields.get("theme", [])]
        affected_countries = [x["name"] for x in fields.get("country", [])]
        disaster_title = disaster
        disaster_time = (
            "-".join(disaster_title[0].split(" - ")[1:]).strip()
            if disaster_title
            else "-"
        )

        return {
            "node": id_,
            "Document Title": title,
            "Document URL": doc_url,
            "text": text,
            "attachments": urls,
            "Document Posting Date": date_posted,
            "Document Publishing Date": date_created,
            "primary_country": primary_country,
            "source": source,
            "disaster": disaster,
            "format": format_,
            "themes": themes,
            "disaster_type": disaster_type,
            "language": language,
            "similar": [],
            "Affected Countries": affected_countries,
            "Disaster Title": disaster_title,
            "Disaster Time": disaster_time,
            "Multiple Affected Countries": len(affected_countries) > 1,
            "Disaster Type": disaster_type,
        }


# ---------------------------------------------------------------------------
# HTML scraper
# ---------------------------------------------------------------------------

class ReliefArticle:
    """
    Scrape a ReliefWeb page (list page *or* article detail page) via HTML parsing.

    Parameters
    ----------
    url:
        A ReliefWeb URL.  For a list/river page pass a formatted URL string;
        for an article detail page pass the canonical article URL.
    """

    def __init__(self, url: str) -> None:
        self.url = url
        response = requests.post(url, data=json.dumps({"limit": 6}))
        self.content = bs(response.text, "html.parser")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _footer_values(self, keys: List[str], footer) -> Dict:
        footer_dds = footer.find_all("dd")
        total = []
        for dd in footer_dds:
            info = []
            ul = dd.find("ul")
            if ul:
                for li in ul.find_all("li"):
                    a = li.find("a")
                    if a:
                        info.append(a.text)
            total.append(info)

        if len(keys) != len(total):
            return {}
        return dict(zip(keys, total))

    def _parse_article_detail(self) -> Dict:
        """Parse an article detail page and return a metadata dict."""
        _id_tag = self.content.find("link", {"rel": "shortlink"})
        node_id = int(_id_tag["href"].split("/")[-1]) if _id_tag else -1

        date_posted = None
        dp_tag = self.content.find("dd", {"class": "rw-entity-meta__tag-value--posted"})
        if dp_tag:
            t = dp_tag.find("time")
            if t:
                date_posted = datetime.fromisoformat(t["datetime"]).date()

        date_pub = None
        dpub_tag = self.content.find(
            "dd", {"class": "rw-entity-meta__tag-value--published"}
        )
        if dpub_tag:
            t = dpub_tag.find("time")
            if t:
                date_pub = datetime.fromisoformat(t["datetime"]).date()

        title_tag = self.content.find("h1", {"class": "rw-article__title"})
        title = title_tag.text if title_tag else "-"

        text_parts: List[str] = []
        content_div = self.content.find("div", {"class": "rw-report__content"})
        if content_div:
            for el in content_div.find_all("p"):
                for child in el.children:
                    if isinstance(child, NavigableString) or (
                        hasattr(child, "name") and child.name not in ["a", "strong"]
                    ):
                        cleaned = (
                            str(child)
                            .strip()
                            .replace("\n", " ")
                            .replace("<em>", "")
                            .replace("</em>", "")
                        )
                        text_parts.append(cleaned)
        text_parts = [t for t in text_parts if len(word_tokenize(t)) > 3]

        foot = None
        footer_tag = self.content.find("footer", {"class": "rw-article__footer"})
        if footer_tag:
            foot = footer_tag.find("dl", {"class": "rw-meta"}) or footer_tag.find(
                "dl", {"class": "rw-entity-meta"}
            )

        values: Dict = {}
        if foot:
            keys = [c.text.replace(" ", "_").lower() for c in foot.find_all("dt")]
            values = self._footer_values(keys, foot)

        atts: List[str] = []
        att_section = self.content.find("section", {"class": "rw-attachment"})
        if att_section:
            for li in att_section.find_all("li"):
                a = li.find("a")
                if a:
                    atts.append("https://reliefweb.int" + a.get("href", ""))

        similar_urls: List[str] = []
        rel_section = self.content.find("section", {"id": "related"})
        if rel_section:
            for ar in rel_section.find_all(
                "article", {"class": "rw-river-article--report"}
            ):
                h3 = ar.find("h3", {"class": "rw-river-article__title"})
                if h3:
                    a = h3.find("a")
                    if a:
                        similar_urls.append(a.get("href", ""))

        info: Dict = {
            "node": node_id,
            "Document Title": title,
            "Document URL": self.url,
            "text": " ".join(text_parts),
            "similar": similar_urls,
            "Document Posting Date": date_posted,
            "Document Publishing Date": str(date_pub).split(" ")[0] if date_pub else "-",
            "attachments": atts,
        }
        info.update(values)
        return info

    def get_info(self) -> Dict:
        """Return parsed article metadata."""
        return self._parse_article_detail()

    def save_atts(self, main_dir: str = "./") -> None:
        """Download and save attachment files to *main_dir*."""
        info = self.get_info()
        for i, url in enumerate(info.get("attachments", [])):
            ext = url.split(".")[-1]
            dest = os.path.join(main_dir, f"{info['node']}_{i+1}.{ext}")
            try:
                with open(dest, "wb") as f:
                    f.write(requests.get(url).content)
            except Exception as exc:
                print(f"ERROR saving attachment: {exc}")


# ---------------------------------------------------------------------------
# Page-level helpers
# ---------------------------------------------------------------------------

def get_total_article_count(content: bs) -> int:
    """Parse the total result count from a ReliefWeb river/list page."""
    container = content.find(
        "div",
        {"class": "rw-river-results--with-advanced-search rw-river-results"},
    )
    if not container:
        raise ValueError("Could not find result count element on the page.")
    match = re.search(r"of ([\d,]+) results", container.text)
    if not match:
        raise ValueError("Could not parse result count from page text.")
    return int(match.group(1).replace(",", ""))


def scrape_article_urls_from_page(page_content: bs) -> List[str]:
    """Return all article URLs found on a ReliefWeb river page."""
    main_list = page_content.find(
        "div", {"class": "[ cd-flow ] rw-river__articles"}
    )
    if not main_list:
        return []

    urls: List[str] = []
    for cls in ("rw-river-article--with-summary", "rw-river-article--with-preview"):
        for article in main_list.find_all("article", {"class": cls}):
            h3 = article.find("h3", {"class": "rw-river-article__title"})
            if h3:
                a = h3.find("a")
                if a:
                    urls.append(a.get("href", ""))
    return list(set(urls))


# ---------------------------------------------------------------------------
# Text & source helpers
# ---------------------------------------------------------------------------

def detect_language(text: str) -> str:
    """Return ISO 639-1 language code, or empty string on failure."""
    try:
        return detect(text)
    except Exception:
        return ""


def get_source_types(sources: List[str]) -> List[str]:
    """Map source names to their type using the bundled metadata."""
    source_types: List[str] = []
    for source in sources:
        matches = [k for k in _SOURCES_METADATA if source in k]
        if matches:
            for match in matches:
                source_types.append(_SOURCES_METADATA[match]["type"])
        else:
            print(f"Source '{source}' not found in metadata")
    return source_types
