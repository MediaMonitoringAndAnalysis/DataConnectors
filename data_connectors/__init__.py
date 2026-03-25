"""
data_connectors – pluggable data-ingestion library for Media Monitoring & Analysis.

Available connectors
--------------------
- **ReliefWeb** – scrapes situation reports and updates from reliefweb.int.

Quick start
-----------
>>> from data_connectors import get_reliefweb_leads
>>> leads = get_reliefweb_leads(
...     project_page_starting_url=(
...         "https://reliefweb.int/updates"
...         "?advanced-search=%28PC220%29_%28DO20241109-%29&page={}"
...     ),
...     project_name="sudan_2024",
...     data_folder="data/sudan_2024",
...     extracted_data_path="data/sudan_2024/leads.csv",
...     extract_pdf_text=False,
... )
"""

from .reliefweb import ReliefWebConnector, get_reliefweb_leads

__all__ = [
    "get_reliefweb_leads",
    "ReliefWebConnector",
]
