"""
Abstract base class for all data connectors.

To add a new connector, subclass ``BaseConnector`` and implement
``get_leads``.  Then expose the callable in the connector's
``__init__.py`` and re-export it from the top-level
``data_connectors/__init__.py``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class BaseConnector(ABC):
    """
    Common interface that every data connector must satisfy.

    Subclasses must implement :meth:`get_leads`, which returns a
    :class:`pandas.DataFrame` whose columns follow the shared lead schema:

    +---------------------------------+-------------------------------+
    | Column                          | Description                   |
    +=================================+===============================+
    | ``doc_id``                      | Unique document identifier    |
    +---------------------------------+-------------------------------+
    | ``Entry Type``                  | Human-readable source label   |
    +---------------------------------+-------------------------------+
    | ``Document Title``              | Title of the document         |
    +---------------------------------+-------------------------------+
    | ``Document URL``                | Canonical URL                 |
    +---------------------------------+-------------------------------+
    | ``Primary Country``             | Primary country string        |
    +---------------------------------+-------------------------------+
    | ``Document Format``             | Format list (e.g. ``["PDF"]``)|
    +---------------------------------+-------------------------------+
    | ``Document Publishing Date``    | ``datetime.date``             |
    +---------------------------------+-------------------------------+
    | ``Document Source``             | List of source names          |
    +---------------------------------+-------------------------------+
    | ``attachments``                 | List of attachment URLs       |
    +---------------------------------+-------------------------------+
    | ``text``                        | Extracted body text           |
    +---------------------------------+-------------------------------+
    """

    @abstractmethod
    def get_leads(self, *args, **kwargs) -> pd.DataFrame:
        """Fetch and return leads as a DataFrame."""
        ...
