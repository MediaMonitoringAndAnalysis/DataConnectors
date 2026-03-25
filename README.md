# DataConnectors

Pluggable data-ingestion connectors for **Media Monitoring & Analysis** pipelines.

Each connector scrapes, normalises, and optionally enriches documents from a
specific source into a common `pandas.DataFrame` schema ready for downstream
analysis.

---

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Connectors](#connectors)
  - [ReliefWeb](#reliefweb)
- [Output Schema](#output-schema)
- [Adding a New Connector](#adding-a-new-connector)
- [Environment Variables](#environment-variables)
- [Development](#development)
- [License](#license)

---

## Installation

```bash
pip install git+https://github.com/MediaMonitoringAndAnalysis/DataConnectors
```

### Optional: PDF text extraction

PDF extraction depends on the companion `documents_processing` package and an
OpenAI API key.

```bash
pip install "git+https://github.com/MediaMonitoringAndAnalysis/DataConnectors#egg=data-connectors[pdf]"
pip install git+https://github.com/MediaMonitoringAndAnalysis/documents_processing
```

---

## Quick Start

```python
from data_connectors import get_reliefweb_leads

leads = get_reliefweb_leads(
    project_page_starting_url=(
        "https://reliefweb.int/updates"
        "?advanced-search=%28PC220%29_%28DO20241109-%29&page={}"
    ),
    project_name="sudan_2024",
    data_folder="data/sudan_2024",
    extracted_data_path="data/sudan_2024/leads.csv",
    extract_pdf_text=False,   # set True + provide OPENAI_API_KEY to also extract PDF text
)

print(leads.head())
```

---

## Connectors

### ReliefWeb

Scrapes situation reports and updates from [reliefweb.int](https://reliefweb.int).

#### Function signature

```python
from data_connectors import get_reliefweb_leads

leads: pd.DataFrame = get_reliefweb_leads(
    project_page_starting_url: str,
    project_name: str,
    data_folder: str,
    extracted_data_path: os.PathLike,
    openai_api_key: str | None = None,   # falls back to OPENAI_API_KEY env var
    extract_pdf_text: bool = True,
    save: bool = True,
    sample: bool = False,
)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `project_page_starting_url` | `str` | ReliefWeb search URL with `{}` placeholder for the page number. |
| `project_name` | `str` | Short identifier used as a sub-folder name (e.g. `"sudan_2024"`). |
| `data_folder` | `str` | Root directory for downloaded PDFs and intermediate CSVs. |
| `extracted_data_path` | `PathLike` | CSV cache file.  If it already exists, scraping is skipped. |
| `openai_api_key` | `str \| None` | OpenAI key for PDF extraction.  Falls back to `OPENAI_API_KEY`. |
| `extract_pdf_text` | `bool` | Run PDF extraction step (default `True`). |
| `save` | `bool` | Persist intermediate results to disk (default `True`). |
| `sample` | `bool` | Scrape only ~5 articles for testing (default `False`). |

#### Class-based usage

```python
from data_connectors import ReliefWebConnector

connector = ReliefWebConnector(
    project_name="sudan_2024",
    data_folder="data/sudan_2024",
    extracted_data_path="data/sudan_2024/leads.csv",
    extract_pdf_text=False,
)

leads = connector.get_leads(
    project_page_starting_url=(
        "https://reliefweb.int/updates"
        "?advanced-search=%28PC220%29_%28DO20241109-%29&page={}"
    ),
    sample=True,  # quick test run
)
```

#### How to build a ReliefWeb search URL

1. Go to <https://reliefweb.int/updates> and apply your filters.
2. Copy the URL from the address bar.
3. Replace the `page=N` value (or append `&page={}` if absent).

Example for Sudan updates since November 2024:
```
https://reliefweb.int/updates?advanced-search=%28PC220%29_%28DO20241109-%29&page={}
```

---

## Output Schema

Every connector returns a `pandas.DataFrame` with at least the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `doc_id` | `str` | Unique document identifier (prefixed with source, e.g. `reliefweb_123`). |
| `Entry Type` | `str` | Human-readable source label (e.g. `"Reliefweb Website"`). |
| `Document Title` | `str` | Title of the document. |
| `Document URL` | `str` | Canonical URL. |
| `Primary Country` | `str` | Primary country string. |
| `Document Format` | `list[str]` | Format tags (e.g. `["Situation Report"]`). |
| `Document Publishing Date` | `date` | Publication date. |
| `Document Source` | `list[str]` | List of source organisation names. |
| `attachments` | `list[str]` | List of attachment URLs. |
| `text` | `str` | Extracted body text. |

---

## Adding a New Connector

The library is designed to be extended.  Follow these steps to add a connector
for a new data source (e.g. `acaps`):

### 1. Create the connector sub-package

```
data_connectors/
└── acaps/
    ├── __init__.py
    ├── connector.py    ← public get_acaps_leads() function
    └── scraper.py      ← source-specific scraping logic
```

### 2. Subclass `BaseConnector`

```python
# data_connectors/acaps/connector.py
from data_connectors.base import BaseConnector
import pandas as pd

class AcapsConnector(BaseConnector):
    def get_leads(self, *args, **kwargs) -> pd.DataFrame:
        # ... your scraping logic ...
        return pd.DataFrame(...)

def get_acaps_leads(...) -> pd.DataFrame:
    return AcapsConnector().get_leads(...)
```

### 3. Expose from the sub-package `__init__.py`

```python
# data_connectors/acaps/__init__.py
from .connector import AcapsConnector, get_acaps_leads

__all__ = ["get_acaps_leads", "AcapsConnector"]
```

### 4. Re-export from the top-level `__init__.py`

```python
# data_connectors/__init__.py
from .acaps import AcapsConnector, get_acaps_leads

__all__ = [
    # ... existing exports ...
    "get_acaps_leads",
    "AcapsConnector",
]
```

### 5. Add dependencies to `pyproject.toml`

```toml
[project.dependencies]
# ... existing deps ...
"acaps-specific-lib>=1.0",
```

---

## Environment Variables

| Variable | Used by | Description |
|----------|---------|-------------|
| `OPENAI_API_KEY` | ReliefWeb PDF extractor | OpenAI API key for PDF text extraction. |

Create a `.env` file in your project root (already in `.gitignore`):

```env
OPENAI_API_KEY=sk-...
```

`python-dotenv` will pick it up automatically.

---

## Development

```bash
git clone https://github.com/MediaMonitoringAndAnalysis/DataConnectors
cd DataConnectors
pip install -e ".[dev]"

# run tests
pytest

# lint
ruff check data_connectors/

# type-check
mypy data_connectors/
```

---

## License

AGPL-3.0 — see [LICENSE](LICENSE).
