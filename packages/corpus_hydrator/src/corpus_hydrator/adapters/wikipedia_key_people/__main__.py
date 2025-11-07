"""
Main entry point for Wikipedia Key People Scraper

This allows the scraper to be run as a module:
    python -m corpus_hydrator.adapters.wikipedia_key_people --help
"""

from .cli.commands import app

if __name__ == "__main__":
    app()
