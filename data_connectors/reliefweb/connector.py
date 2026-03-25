"""
ReliefWeb connector – public API.

The primary entry-point is :func:`get_reliefweb_leads`.
"""

from __future__ import annotations

import ast
import os
import random
from typing import Dict, List, Optional

import pandas as pd
from tqdm import tqdm

from ..base import BaseConnector
from .scraper import (
    ReliefArticle,
    detect_language,
    get_total_article_count,
    scrape_article_urls_from_page,
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _cast_text(value) -> list:
    """Coerce *value* to a list, handling string-encoded lists from CSV round-trips."""
    if value == "-":
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            return ast.literal_eval(value)
        except (ValueError, SyntaxError):
            return []
    return []


def _get_lead_sources(row: pd.Series) -> List[str]:
    sources: List[str] = []
    for col in ("source", "sources"):
        if col in row.index and isinstance(row[col], list):
            sources.extend(row[col])
    return sources


def _clean_text_based_on_url(url, text):
    """
    If the document URL points to a non-PDF file, discard the text.
    For PDF URLs, text extraction is handled separately.
    """
    non_text_exts = {"csv", "xlsx", "doc", "docx", "jpeg", "jpg", "pptx", "ppt", "png"}
    if not isinstance(url, str):
        return text
    ext = url.split(".")[-1].lower()
    if ext in non_text_exts | {"pdf"}:
        return [] if ext != "pdf" else text
    return text


# ---------------------------------------------------------------------------
# Scraping pipeline
# ---------------------------------------------------------------------------

def _scrape_reliefweb_leads(
    starting_url_template: str,
    project_name: str,
    data_folder: str,
    save: bool = True,
    sample: bool = False,
) -> pd.DataFrame:
    """
    Crawl ReliefWeb and return raw leads as a DataFrame.

    Parameters
    ----------
    starting_url_template:
        A URL template with a single ``{}`` placeholder for the page number,
        e.g. ``"https://reliefweb.int/updates?advanced-search=...&page={}"``
    project_name:
        Logical name for this scraping run (used when *save* is True).
    data_folder:
        Root folder where intermediate data is written when *save* is True.
    save:
        If ``True`` (default), create *data_folder* and save intermediate CSVs.
    sample:
        If ``True``, only scrape a small sample of 5 articles (useful for testing).

    Returns
    -------
    pandas.DataFrame
        Raw leads before post-processing.
    """
    if save:
        if not project_name:
            raise ValueError("'project_name' must be provided when save=True.")
        if not data_folder:
            raise ValueError("'data_folder' must be provided when save=True.")
        os.makedirs(data_folder, exist_ok=True)

    initial_page = ReliefArticle(starting_url_template.format(0))

    if sample:
        total_nb_articles = 5
        page_range = [0]
    else:
        total_nb_articles = get_total_article_count(initial_page.content)
        last_page = 1 + total_nb_articles // 20
        page_range = list(range(int(last_page)))

    results: List[Dict] = []
    with tqdm(total=total_nb_articles, desc="Scraping ReliefWeb articles") as pbar:
        for page_nb in page_range:
            page_url = starting_url_template.format(page_nb)
            page = ReliefArticle(page_url)
            article_urls = scrape_article_urls_from_page(page.content)

            if sample:
                article_urls = article_urls[:total_nb_articles]

            random.shuffle(article_urls)
            for url in article_urls:
                article = ReliefArticle(url)
                info = article.get_info()
                pbar.update(1)
                if info:
                    results.append(info)
                else:
                    print(f"WARNING: no data parsed for {url}")

    return _postprocess_leads(
        results,
        project_name=project_name,
        additional_columns=[
            "doc_id",
            "Document Title",
            "Primary Country",
            "Document Format",
            "Document Publishing Date",
            "Document Source",
        ],
    )


def _postprocess_leads(
    results: List[Dict],
    project_name: str,
    additional_columns: List[str],
) -> pd.DataFrame:
    """Normalise raw scraping results into the standard lead schema."""
    df = pd.DataFrame(results)
    df.rename(columns={"node": "doc_id"}, inplace=True)
    df["doc_id"] = df["doc_id"].apply(lambda x: f"reliefweb_{x}")
    df["Primary Country"] = df["primary_country"].apply(
        lambda x: x[0] if isinstance(x, list) and x else "-"
    )
    df["Document Source"] = df.apply(_get_lead_sources, axis=1)
    df["Document Format"] = df["format"].apply(_cast_text)
    df["Entry Type"] = "Reliefweb Website"
    df["entry_fig_path"] = "-"

    final_cols = list(
        dict.fromkeys(
            [
                "doc_id",
                "Entry Type",
                "entry_fig_path",
                "Document Title",
                "Document URL",
                "Primary Country",
                "Document Format",
                "Document Publishing Date",
                "Document Source",
                "attachments",
                "text",
            ]
            + additional_columns
        )
    )
    # Keep only columns that actually exist
    final_cols = [c for c in final_cols if c in df.columns]
    return df[final_cols]


# ---------------------------------------------------------------------------
# Public function
# ---------------------------------------------------------------------------

def get_reliefweb_leads(
    project_page_starting_url: str,
    project_name: str,
    data_folder: str,
    extracted_data_path: os.PathLike,
    openai_api_key: Optional[str] = None,
    extract_pdf_text: bool = True,
    save: bool = True,
    sample: bool = False,
) -> pd.DataFrame:
    """
    Fetch ReliefWeb leads and optionally extract text from PDF attachments.

    The function implements a simple cache: if *extracted_data_path* already
    exists the scraping step is skipped and the CSV is loaded directly.

    Parameters
    ----------
    project_page_starting_url:
        ReliefWeb search/updates URL with a ``{}`` placeholder for the page
        number.  Example::

            "https://reliefweb.int/updates?advanced-search=%28PC220%29&page={}"

    project_name:
        Short identifier for this project (e.g. ``"sudan_2024"``).  Used as a
        sub-folder name when persisting data.
    data_folder:
        Root directory for storing downloaded PDFs and intermediate files.
    extracted_data_path:
        Path to the CSV file used as a cache.  If the file does not exist it
        will be created after scraping.
    openai_api_key:
        OpenAI API key required for PDF text extraction.  Falls back to the
        ``OPENAI_API_KEY`` environment variable when *None*.  If neither is
        set and *extract_pdf_text* is ``True``, a :class:`ValueError` is raised.
    extract_pdf_text:
        Whether to run the PDF extraction step (default ``True``).  Set to
        ``False`` to skip it (no OpenAI key required).
    save:
        Persist intermediate results to disk (default ``True``).
    sample:
        Scrape only a small sample of ~5 articles.  Useful for testing.

    Returns
    -------
    pandas.DataFrame
        Leads with the standard schema described in :class:`~data_connectors.base.BaseConnector`.

    Examples
    --------
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
    >>> leads.head()
    """
    # Resolve OpenAI API key
    if openai_api_key is None:
        openai_api_key = os.getenv("OPENAI_API_KEY") or os.getenv("openai_api_key")

    if extract_pdf_text and openai_api_key is None:
        raise ValueError(
            "An OpenAI API key is required for PDF text extraction.  "
            "Pass it via 'openai_api_key' or set the OPENAI_API_KEY environment variable.  "
            "To skip PDF extraction set extract_pdf_text=False."
        )

    # --- Scraping (with CSV cache) ------------------------------------------
    if not os.path.exists(extracted_data_path):
        new_leads = _scrape_reliefweb_leads(
            project_page_starting_url, project_name, data_folder, save, sample
        )
        if save:
            os.makedirs(os.path.dirname(os.path.abspath(extracted_data_path)), exist_ok=True)
            new_leads.to_csv(extracted_data_path, index=False)
    else:
        new_leads = pd.read_csv(extracted_data_path)
        new_leads["attachments"] = new_leads["attachments"].apply(_cast_text)

    # --- PDF text extraction -------------------------------------------------
    if extract_pdf_text and "PDF Text" not in new_leads.get("Entry Type", pd.Series()).unique():
        from .pdf_extractor import call_pdf_extractor

        print("Extracting text from PDF attachments…")
        pdf_files_path = os.path.join(data_folder, project_name, "pdf_files")
        pdf_leads = call_pdf_extractor(
            df=new_leads,
            pdf_doc_folder_path=pdf_files_path,
            openai_api_key=openai_api_key,
            additional_columns=[],
            figures_saving_path=os.path.join(pdf_files_path, "figures"),
        )
        new_leads = pd.concat([new_leads, pdf_leads], ignore_index=True)
        new_leads = new_leads[new_leads["text"].apply(lambda x: len(str(x).strip()) > 5)]

        if save:
            new_leads.to_csv(extracted_data_path, index=False)

    return new_leads


# ---------------------------------------------------------------------------
# Connector class (for plugin-style usage)
# ---------------------------------------------------------------------------

class ReliefWebConnector(BaseConnector):
    """
    :class:`~data_connectors.base.BaseConnector` implementation for ReliefWeb.

    This class wraps :func:`get_reliefweb_leads` for use in plugin-style
    pipelines where connector instances are passed around.

    Parameters
    ----------
    project_name:
        Short identifier for this project.
    data_folder:
        Root directory for downloaded PDFs and intermediate files.
    extracted_data_path:
        CSV cache file path.
    openai_api_key:
        OpenAI key (optional; falls back to ``OPENAI_API_KEY`` env var).
    extract_pdf_text:
        Whether to extract text from PDF attachments (default ``True``).
    save:
        Persist intermediate results to disk (default ``True``).
    """

    def __init__(
        self,
        project_name: str,
        data_folder: str,
        extracted_data_path: os.PathLike,
        openai_api_key: Optional[str] = None,
        extract_pdf_text: bool = True,
        save: bool = True,
    ) -> None:
        self.project_name = project_name
        self.data_folder = data_folder
        self.extracted_data_path = extracted_data_path
        self.openai_api_key = openai_api_key
        self.extract_pdf_text = extract_pdf_text
        self.save = save

    def get_leads(
        self,
        project_page_starting_url: str,
        sample: bool = False,
    ) -> pd.DataFrame:
        """
        Fetch leads from ReliefWeb.

        Parameters
        ----------
        project_page_starting_url:
            Search URL template (``{}`` is replaced with the page number).
        sample:
            Scrape only a small sample (useful for testing).
        """
        return get_reliefweb_leads(
            project_page_starting_url=project_page_starting_url,
            project_name=self.project_name,
            data_folder=self.data_folder,
            extracted_data_path=self.extracted_data_path,
            openai_api_key=self.openai_api_key,
            extract_pdf_text=self.extract_pdf_text,
            save=self.save,
            sample=sample,
        )
