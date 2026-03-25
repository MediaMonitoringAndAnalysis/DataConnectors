"""
ReliefWeb data connector.

Exposes :func:`get_reliefweb_leads` and :class:`ReliefWebConnector`.
"""

from .connector import ReliefWebConnector, get_reliefweb_leads

__all__ = ["get_reliefweb_leads", "ReliefWebConnector"]
