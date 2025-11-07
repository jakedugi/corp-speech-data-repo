"""CLI commands for the corpus_cleaner module."""

from .normalize import app as normalize_app
from .process_courtlistener import app as courtlistener_app

__all__ = ["normalize_app", "courtlistener_app"]
