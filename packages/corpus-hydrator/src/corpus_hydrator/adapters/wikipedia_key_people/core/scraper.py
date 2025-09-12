"""
Wikipedia Key People Scraper - Core Implementation

This module provides the main scraper functionality for extracting key people
information from Wikipedia pages, using proper link extraction and data cleaning.
"""

import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import urljoin
import re
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import warnings
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

from corpus_types.schemas.wikipedia_key_people import (
    WikipediaKeyPeopleConfig,
    WikipediaKeyPerson,
    WikipediaCompany,
    WikipediaExtractionResult,
    get_default_config
)

from .base_extractor import BaseWikipediaPeopleExtractor
from .enhanced_scraper import EnhancedWikipediaKeyPeopleExtractor

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ────────── HTTP Setup (exactly like sandp_scraper) ──────────
HEADERS = {"User-Agent": "jake@jakedugan.com"}

# Robust HTTP Session with Retries & Backoff
session = requests.Session()
session.headers.update(HEADERS)
retry_strategy = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "HEAD"],
)
adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
session.mount("https://data.sec.gov", adapter)
session.mount("https://www.sec.gov", adapter)

# increase pool size for wikipedia
wiki_adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50)
session.mount("https://en.wikipedia.org", wiki_adapter)

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)


class WikipediaLinkExtractor:
    """Extracts actual Wikipedia links from index pages."""

    def __init__(self, config: WikipediaKeyPeopleConfig):
        self.config = config

    def extract_company_links(self, index_name: str) -> List[Dict[str, Any]]:
        """
        Extract actual Wikipedia company links from an index page.

        Returns list of dicts with: ticker, company_name, wikipedia_url
        """
        logger.info(f"Extracting company links from {index_name}")

        try:
            index_config = self.config.get_index_config(index_name)
            r = session.get(index_config.wikipedia_url, timeout=10)
            r.raise_for_status()

            soup = BeautifulSoup(r.text, "lxml")

            # Find the constituents table
            table = soup.find("table", {"id": index_config.table_id})
            if not table:
                table = soup.find("table", {"class": "wikitable sortable"})
                if not table:
                    logger.error(f"Could not find constituents table for {index_name}")
                    return []

            logger.info("Found constituents table, extracting links...")

            rows = table.find_all("tr")[1:]  # Skip header row
            companies = []

            for row in rows:
                # For Dow Jones, look for th elements with scope="row" (company links)
                # For S&P 500, look for td elements as usual
                if index_name == "dow":
                    # Special handling for Dow Jones table structure
                    th_elements = row.find_all("th")
                    td_elements = row.find_all("td")

                    if th_elements and td_elements:
                        # Company link is in the first th element
                        company_th = th_elements[0]
                        company_link = company_th.find("a", href=True)

                        # Ticker is in the second td element (after the exchange td)
                        if len(td_elements) >= 2:
                            ticker_link = td_elements[1].find("a", href=True)  # Second td has the ticker

                            if company_link and ticker_link:
                                company_href = company_link["href"]
                                ticker_text = ticker_link.get_text(" ", strip=True)

                                # Only process if we have a valid company wiki link and ticker
                                if company_href.startswith("/wiki/") and ticker_text:
                                    full_url = urljoin("https://en.wikipedia.org", company_href)
                                    company_name = company_link.get_text(" ", strip=True)

                                    company_data = {
                                        "ticker": ticker_text,
                                        "company_name": company_name,
                                        "wikipedia_url": full_url,
                                        "index_name": index_name
                                    }

                                    companies.append(company_data)
                                    logger.info(f"Extracted: {ticker_text} -> {full_url}")
                                    # Debug: Print first few URLs to see what we're getting
                                    if len(companies) <= 3:
                                        print(f"DEBUG: Extracted URL #{len(companies)}: {ticker_text} -> {full_url}")
                                        print(f"DEBUG: Original href: {company_href}")
                                        print(f"DEBUG: Constructed URL: {urljoin('https://en.wikipedia.org', company_href)}")
                else:
                    # Standard handling for other indices (S&P 500, NASDAQ)
                    cells = row.find_all("td")
                    if len(cells) > max(index_config.ticker_column, index_config.name_column):
                        # Extract ticker
                        ticker_cell = cells[index_config.ticker_column]
                        ticker_link = ticker_cell.find("a", href=True)
                        ticker = ticker_link.get_text(" ", strip=True) if ticker_link else ticker_cell.text.strip()

                        # Extract company link
                        company_cell = cells[index_config.name_column]
                        link_element = company_cell.find("a", href=True)

                        if link_element and link_element.get("href") and ticker:
                            link_href = link_element["href"]

                            # Check if this is a Wikipedia link
                            if link_href.startswith("/wiki/") or "wikipedia.org" in link_href:
                                full_url = urljoin("https://en.wikipedia.org", link_href)
                                company_name = link_element.get_text(" ", strip=True)

                                company_data = {
                                    "ticker": ticker,
                                    "company_name": company_name,
                                    "wikipedia_url": full_url,
                                    "index_name": index_name
                                }

                                companies.append(company_data)
                                logger.info(f"Extracted: {ticker} -> {full_url}")
                                # Debug: Print first few URLs to see what we're getting
                                if len(companies) <= 3:
                                    print(f"DEBUG: Extracted URL #{len(companies)}: {ticker} -> {full_url}")
                                    print(f"DEBUG: Original href: {link_href}")
                                    print(f"DEBUG: Constructed URL: {urljoin('https://en.wikipedia.org', link_href)}")
                            else:
                                logger.debug(f"Skipping non-Wikipedia link: {link_href}")

            logger.info(f"Successfully extracted {len(companies)} company links from {index_name}")
            return companies

        except Exception as e:
            logger.error(f"Failed to extract links from {index_name}: {e}")
            return []


class WikipediaKeyPeopleExtractor(BaseWikipediaPeopleExtractor):
    """Extracts key people information from individual Wikipedia company pages."""

    def __init__(self, config: WikipediaKeyPeopleConfig):
        super().__init__(config)

    def extract_key_people(self, company: Dict[str, Any]) -> List[WikipediaKeyPerson]:
        """
        Extract key people from a company's Wikipedia page.

        Args:
            company: Dict with ticker, company_name, wikipedia_url, index_name

        Returns:
            List of WikipediaKeyPerson objects
        """
        ticker = company["ticker"]
        company_name = company["company_name"]
        wikipedia_url = company["wikipedia_url"]
        index_name = company["index_name"]

        logger.debug(f"Extracting key people for {ticker} from {wikipedia_url}")

        try:
            # Add rate limiting
            time.sleep(1.0 / self.config.scraping.wikipedia_rate_limit)

            r = session.get(wikipedia_url, timeout=self.config.scraping.request_timeout)
            r.raise_for_status()

            soup = BeautifulSoup(r.text, "lxml")
            infobox = soup.select_one("table.infobox.vcard")

            if not infobox:
                logger.warning(f"No infobox found for {ticker}")
                return []

            key_people = []

            # Search for sections containing key people
            for tr in infobox.find_all("tr"):
                th = tr.find("th")
                td = tr.find("td")

                if not th or not td:
                    continue

                heading = th.get_text(" ", strip=True).lower()

                # Check if this section contains key people
                if any(keyword in heading for keyword in self.config.content.role_keywords):
                    logger.debug(f"Found key people section: '{heading}' for {ticker}")

                    # Extract people from this section
                    people_data = self._extract_people_from_section(td, company)
                    key_people.extend(people_data)

            # Remove duplicates based on clean name
            seen_names = set()
            unique_people = []

            for person in key_people:
                name_key = person.clean_name.lower()
                if name_key not in seen_names and len(name_key) > 1:
                    seen_names.add(name_key)
                    unique_people.append(person)

            logger.info(f"Extracted {len(unique_people)} key people for {ticker}")
            return unique_people[:self.config.scraping.max_people_per_company]

        except Exception as e:
            logger.warning(f"Failed to extract key people for {ticker}: {e}")
            return []

    def _extract_people_from_section(self, section_element, company: Dict[str, Any]) -> List[WikipediaKeyPerson]:
        """Extract individual people from a section element."""
        people = []

        # Try list items first (most common format)
        list_items = section_element.find_all("li")
        if list_items:
            for li in list_items:
                text = li.get_text(" ", strip=True)
                person = self._parse_person_text(text, company)
                if person:
                    people.append(person)
        else:
            # Fallback to pipe-separated text
            text = section_element.get_text(separator="|")
            items = [item.strip() for item in text.split("|") if item.strip()]

            for item in items:
                person = self._parse_person_text(item, company)
                if person:
                    people.append(person)

        return people

    def _parse_person_text(self, text: str, company: Dict[str, Any]) -> Optional[WikipediaKeyPerson]:
        """Parse person text into a WikipediaKeyPerson object."""
        if not text or not text.strip():
            return None

        text = text.strip()

        # Skip obviously malformed entries
        malformed_indicators = [
            text in ['(', ')', ',', '&', '[', ']', 'chairman', 'president', 'CEO', 'CFO', 'COO'],
            len(text) < 3,
            text.isdigit(),
            text in ['Executive', 'Director']
        ]

        if any(malformed_indicators):
            return None

        # Try different parsing patterns
        for pattern in self.config.content.name_title_patterns:
            match = re.match(pattern, text, re.IGNORECASE)
            if match:
                name_part = match.group(1).strip()
                title_part = match.group(2).strip() if len(match.groups()) > 1 else ""

                # Clean and normalize
                clean_name = self._clean_name(name_part)
                clean_title = self._clean_title(title_part or "Executive")

                if clean_name and clean_title:
                    return WikipediaKeyPerson(
                        ticker=company["ticker"],
                        company_name=company["company_name"],
                        raw_name=text,
                        clean_name=clean_name,
                        clean_title=clean_title,
                        wikipedia_url=company["wikipedia_url"],
                        extraction_method="infobox_parsing",
                        parse_success=True,
                        confidence_score=0.9 if len(match.groups()) > 1 else 0.7
                    )

        # If no pattern matches but text looks like a name, assume it's just a name
        if 3 <= len(text) <= 100 and not any(char.isdigit() for char in text):
            clean_name = self._clean_name(text)
            if clean_name:
                return WikipediaKeyPerson(
                    ticker=company["ticker"],
                    company_name=company["company_name"],
                    raw_name=text,
                    clean_name=clean_name,
                    clean_title="Executive",
                    wikipedia_url=company["wikipedia_url"],
                    extraction_method="name_only",
                    parse_success=True,
                    confidence_score=0.5
                )

        return None

    def _clean_name(self, name: str) -> str:
        """Clean up executive name."""
        if not name:
            return ""

        # Remove extra whitespace
        name = ' '.join(name.split())

        # Remove common prefixes/suffixes that aren't part of the name
        name = re.sub(r'^(Mr\.|Mrs\.|Ms\.|Dr\.)\s+', '', name, flags=re.IGNORECASE)
        name = re.sub(r'\s*(Jr\.|Sr\.|III|II|IV)$', '', name, flags=re.IGNORECASE)

        return name.strip()

    def _clean_title(self, title: str) -> str:
        """Clean up executive title."""
        if not title:
            return ""

        # Remove extra whitespace
        title = ' '.join(title.split())

        # Apply title normalization patterns
        for pattern, replacement in self.config.content.title_normalization.items():
            title = re.sub(pattern, replacement, title, flags=re.IGNORECASE)

        return title.strip()


class WikipediaKeyPeopleScraper:
    """Main scraper class that orchestrates the entire extraction process."""

    def __init__(self, config: Optional[WikipediaKeyPeopleConfig] = None):
        self.config = config or get_default_config()

        self.link_extractor = WikipediaLinkExtractor(self.config)
        # Use enhanced extractor for better coverage of complex page structures
        self.people_extractor = EnhancedWikipediaKeyPeopleExtractor(self.config)

        if self.config.verbose:
            logging.getLogger().setLevel(logging.DEBUG)

    def scrape_index(self, index_name: str) -> WikipediaExtractionResult:
        """
        Scrape key people for all companies in an index.

        Args:
            index_name: Name of the index to scrape (sp500, dow, nasdaq100)

        Returns:
            WikipediaExtractionResult with all extracted data
        """
        logger.info(f"Starting key people extraction for {index_name}")

        result = WikipediaExtractionResult(
            operation_id=f"{index_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            index_name=index_name
        )

        try:
            # Step 1: Extract company links from index page
            companies_data = self.link_extractor.extract_company_links(index_name)

            if not companies_data:
                result.success = False
                result.error_message = f"No company links found for {index_name}"
                return result

            # Limit companies if specified
            if self.config.scraping.max_companies:
                companies_data = companies_data[:self.config.scraping.max_companies]

            result.companies_processed = len(companies_data)

            # Step 2: Convert to WikipediaCompany objects
            companies = []
            for i, company_data in enumerate(companies_data):
                # Debug: Check URL before creating company
                url = company_data["wikipedia_url"]
                if not url.startswith("https://en.wikipedia.org/"):
                    print(f"DEBUG: Invalid URL found at index {i}:")
                    print(f"  Ticker: {company_data['ticker']}")
                    print(f"  Company: {company_data['company_name']}")
                    print(f"  URL: {url}")
                    print(f"  URL starts with wiki?: {url.startswith('https://en.wikipedia.org/')}")

                try:
                    company = WikipediaCompany(
                        ticker=company_data["ticker"],
                        company_name=company_data["company_name"],
                        wikipedia_url=company_data["wikipedia_url"],
                        index_name=company_data["index_name"]
                    )
                    companies.append(company)
                except Exception as e:
                    print(f"DEBUG: Failed to create company for {company_data['ticker']}: {e}")
                    print(f"  URL: {company_data['wikipedia_url']}")
                    raise  # Re-raise to see the full error

            # Step 3: Extract key people from each company
            logger.info(f"Extracting key people from {len(companies)} companies...")

            all_key_people = []
            successful_companies = 0

            if self.config.dry_run:
                # In dry run mode, create mock data
                for company in companies:
                    mock_person = WikipediaKeyPerson(
                        ticker=company.ticker,
                        company_name=company.company_name,
                        raw_name=f"John Doe (CEO) - Mock Data",
                        clean_name="John Doe",
                        clean_title="Chief Executive Officer",
                        wikipedia_url=company.wikipedia_url,
                        extraction_method="dry_run_mock",
                        parse_success=True,
                        confidence_score=1.0
                    )
                    all_key_people.append(mock_person)
                    successful_companies += 1
            else:
                # Real extraction with parallel processing
                with ThreadPoolExecutor(max_workers=2) as executor:  # Conservative worker count
                    future_to_company = {
                        executor.submit(self.people_extractor.extract_key_people, {
                            "ticker": company.ticker,
                            "company_name": company.company_name,
                            "wikipedia_url": company.wikipedia_url,
                            "index_name": company.index_name
                        }): company
                        for company in companies
                    }

                    for future in future_to_company:
                        company = future_to_company[future]
                        try:
                            key_people = future.result()
                            if key_people:
                                all_key_people.extend(key_people)
                                company.key_people_count = len(key_people)
                                company.processing_success = True
                                successful_companies += 1
                                logger.info(f"✓ {company.ticker}: {len(key_people)} key people")
                            else:
                                company.processing_success = False
                                logger.warning(f"✗ {company.ticker}: No key people found")
                        except Exception as e:
                            company.processing_success = False
                            logger.error(f"✗ {company.ticker}: Error - {e}")

            # Step 4: Update result object
            result.companies = companies
            result.key_people = all_key_people
            result.companies_successful = successful_companies
            result.total_key_people = len(all_key_people)
            result.mark_completed()

            logger.info(f"Completed extraction for {index_name}:")
            logger.info(f"  - Companies processed: {result.companies_processed}")
            logger.info(f"  - Companies successful: {result.companies_successful}")
            logger.info(f"  - Total key people: {result.total_key_people}")
            logger.info(f"   Average key people per company: {result.total_key_people/result.companies_successful:.1f}" if result.companies_successful > 0 else "   No successful companies")
            return result

        except Exception as e:
            logger.error(f"Extraction failed for {index_name}: {e}")
            result.success = False
            result.error_message = str(e)
            result.mark_completed()
            return result

    def scrape_multiple_indices(self, index_names: List[str]) -> Dict[str, WikipediaExtractionResult]:
        """
        Scrape key people for multiple indices.

        Args:
            index_names: List of index names to scrape

        Returns:
            Dictionary mapping index names to their extraction results
        """
        results = {}

        for index_name in index_names:
            if index_name in self.config.indices:
                logger.info(f"Processing index: {index_name}")
                result = self.scrape_index(index_name)
                results[index_name] = result
            else:
                logger.warning(f"Unknown index: {index_name}")

        return results
