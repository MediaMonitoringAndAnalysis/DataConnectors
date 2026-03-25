"""
PDF extraction helper for ReliefWeb leads.

Depends on the optional ``documents_processing`` package.  If that package is
not installed the function raises :class:`ImportError` with a helpful message.
"""

from __future__ import annotations

import os
from typing import List

import pandas as pd
from tqdm import tqdm


def _get_first_n_words(s: str, n: int = 7) -> str:
    """Return the first *n* words of *s* as a safe filename (with .pdf suffix)."""
    return " ".join(s.split()[:n]).replace("/", "-") + ".pdf"


def call_pdf_extractor(
    df: pd.DataFrame,
    pdf_doc_folder_path: os.PathLike,
    openai_api_key: str,
    additional_columns: List[str] = None,
) -> pd.DataFrame:
    """
    Extract text from PDF attachments listed in *df*.

    For every row whose ``attachments`` column contains at least one ``.pdf``
    URL the PDF is downloaded, processed with ``DocumentsDataExtractor``, and
    the resulting rows are returned as a new :class:`~pandas.DataFrame`.

    Parameters
    ----------
    df:
        DataFrame produced by :func:`~data_connectors.reliefweb.connector.get_reliefweb_leads`.
        Must have columns ``attachments`` and ``Document Title``.
    pdf_doc_folder_path:
        Local folder where downloaded PDFs are stored.
    openai_api_key:
        API key forwarded to ``DocumentsDataExtractor``.
    additional_columns:
        Extra columns from *df* to copy into the output rows.
        Defaults to the standard lead schema columns.

    Returns
    -------
    pandas.DataFrame
        One row per extracted page/section across all processed PDFs.

    Raises
    ------
    ImportError
        If the ``documents_processing`` package is not installed.
    """
    try:
        from documents_processing import DocumentsDataExtractor  # type: ignore
    except ImportError as exc:
        raise ImportError(
            "PDF extraction requires the 'documents_processing' package.\n"
            "Install it with:\n"
            "  pip install git+https://github.com/MediaMonitoringAndAnalysis/documents_processing"
        ) from exc

    if additional_columns is None:
        additional_columns = [
            "doc_id",
            "Document Title",
            "Primary Country",
            "Document Format",
            "Document Publishing Date",
            "Document Source",
        ]

    extractor = DocumentsDataExtractor(
        inference_pipeline_name="OpenAI",
        model_name="gpt-4o-mini",
        api_key=openai_api_key,
    )

    os.makedirs(pdf_doc_folder_path, exist_ok=True)
    outputs = pd.DataFrame()

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Extracting PDF text"):
        for attachment_url in row.get("attachments", []):
            if not str(attachment_url).endswith(".pdf"):
                continue

            doc_name = _get_first_n_words(row["Document Title"])
            doc_output: pd.DataFrame = extractor(
                file_name=doc_name,
                doc_folder_path=pdf_doc_folder_path,
                extract_figures_bool=True,
                metadata_extraction_type="document",
                relevant_pages_for_metadata_extraction=[0, 1],
                return_original_pages_numbers=True,
                figures_saving_path="figures",
                doc_url=attachment_url,
            )

            doc_output["Document URL"] = attachment_url
            doc_output["attachments"] = [["-"]] * len(doc_output)
            for col in additional_columns:
                if col in row.index:
                    doc_output[col] = row[col]

            outputs = pd.concat([outputs, doc_output], ignore_index=True)

    return outputs
