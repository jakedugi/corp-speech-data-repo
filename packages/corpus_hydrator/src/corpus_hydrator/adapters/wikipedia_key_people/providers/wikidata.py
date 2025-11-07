"""
Wikidata Provider for Structured People Data

This provider fetches executive and board member information from Wikidata,
which provides structured, stable data that's more reliable than scraping HTML.
"""

import logging
import time
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import requests

from .base import PeopleProvider, ProviderPayload

logger = logging.getLogger(__name__)


class WikidataProvider:
    """Provider for fetching people data from Wikidata."""

    WIKIDATA_API_URL = "https://www.wikidata.org/w/api.php"
    WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"

    # Wikidata properties for executive roles
    EXECUTIVE_PROPERTIES = {
        "P169": "chief executive officer",
        "P488": "chairperson",
        "P3320": "board member",
        "P1037": "director",
        "P6": "head of government",
        "P35": "head of state",
        "P39": "position held",
        "P108": "employer",
        "P1080": "from narrative universe",
    }

    def __init__(self, config=None):
        """Initialize the Wikidata provider."""
        self.config = config
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Wikipedia-Key-People-Scraper/1.0 (jake@jakedugan.com)",
                "Accept": "application/json",
            }
        )

    def fetch_company_people(
        self, company: Dict[str, Any]
    ) -> Optional[ProviderPayload]:
        """
        Fetch people data for a company from Wikidata.

        Args:
            company: Company dict with ticker, company_name, wikipedia_url, etc.

        Returns:
            ProviderPayload with JSON data, or None if not available
        """
        try:
            # First, get the Wikidata QID for this company
            qid = self._resolve_company_qid(company)
            if not qid:
                logger.debug(
                    f"No Wikidata QID found for {company.get('ticker', 'unknown')}"
                )
                return None

            # Query for executive and board member information
            people_data = self._query_company_executives(qid)
            if not people_data:
                logger.debug(f"No executive data found in Wikidata for {qid}")
                return None

            return ProviderPayload(
                url=f"https://www.wikidata.org/wiki/{qid}",
                content=people_data,
                content_type="json",
                revision_id=None,  # Wikidata doesn't have simple revision IDs
                fetched_at=None,  # Will be set by ProviderPayload
            )

        except Exception as e:
            logger.warning(
                f"Failed to fetch Wikidata data for {company.get('ticker', 'unknown')}: {e}"
            )
            return None

    def get_provider_name(self) -> str:
        """Get the name of this provider."""
        return "wikidata"

    def get_confidence_score(self) -> float:
        """Get the confidence score for Wikidata data quality."""
        return 0.95  # High confidence due to structured nature

    def _resolve_company_qid(self, company: Dict[str, Any]) -> Optional[str]:
        """
        Resolve a company's Wikidata QID from Wikipedia URL or company name.

        Args:
            company: Company dictionary

        Returns:
            Wikidata QID (e.g., 'Q95') or None
        """
        wikipedia_url = company.get("wikipedia_url")
        if wikipedia_url:
            # Extract title from Wikipedia URL
            title = wikipedia_url.split("/")[-1]
            if title:
                return self._get_qid_from_wikipedia_title(title)

        # Fallback: search by company name
        company_name = company.get("company_name")
        if company_name:
            return self._search_qid_by_name(company_name)

        return None

    def _get_qid_from_wikipedia_title(self, title: str) -> Optional[str]:
        """Get Wikidata QID from Wikipedia page title."""
        try:
            params = {
                "action": "query",
                "prop": "pageprops",
                "titles": title,
                "format": "json",
            }

            response = self.session.get(self.WIKIDATA_API_URL, params=params)
            response.raise_for_status()

            data = response.json()
            pages = data.get("query", {}).get("pages", {})

            for page_data in pages.values():
                wikibase_item = page_data.get("pageprops", {}).get("wikibase_item")
                if wikibase_item:
                    return wikibase_item

        except Exception as e:
            logger.debug(f"Failed to get QID from Wikipedia title {title}: {e}")

        return None

    def _search_qid_by_name(self, company_name: str) -> Optional[str]:
        """Search for Wikidata QID by company name."""
        try:
            # Use Wikidata search API
            params = {
                "action": "wbsearchentities",
                "search": company_name,
                "language": "en",
                "type": "item",
                "format": "json",
            }

            response = self.session.get(self.WIKIDATA_API_URL, params=params)
            response.raise_for_status()

            data = response.json()
            search_results = data.get("search", [])

            # Return the first result (most relevant)
            if search_results:
                return search_results[0].get("id")

        except Exception as e:
            logger.debug(f"Failed to search QID by name {company_name}: {e}")

        return None

    def _query_company_executives(self, qid: str) -> Optional[Dict[str, Any]]:
        """
        Query Wikidata for executive information about a company.

        Args:
            qid: Wikidata QID for the company

        Returns:
            Dictionary with executive information
        """
        sparql_query = f"""
        SELECT ?person ?personLabel ?position ?positionLabel ?startDate ?endDate WHERE {{
          {{
            wd:{qid} wdt:P169 ?person .  # chief executive officer
            OPTIONAL {{ ?person wdt:P580 ?startDate . }}  # start date
            OPTIONAL {{ ?person wdt:P582 ?endDate . }}    # end date
            BIND("Chief Executive Officer" AS ?positionLabel)
          }} UNION {{
            wd:{qid} wdt:P488 ?person .  # chairperson
            OPTIONAL {{ ?person wdt:P580 ?startDate . }}
            OPTIONAL {{ ?person wdt:P582 ?endDate . }}
            BIND("Chairperson" AS ?positionLabel)
          }} UNION {{
            wd:{qid} wdt:P3320 ?person .  # board member
            OPTIONAL {{ ?person wdt:P580 ?startDate . }}
            OPTIONAL {{ ?person wdt:P582 ?endDate . }}
            BIND("Board Member" AS ?positionLabel)
          }}

          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
        }}
        """

        try:
            params = {"query": sparql_query, "format": "json"}

            response = self.session.get(self.WIKIDATA_SPARQL_URL, params=params)
            response.raise_for_status()

            data = response.json()
            results = data.get("results", {}).get("bindings", [])

            if not results:
                return None

            # Process results into our format
            executives = []
            for result in results:
                person_qid = result.get("person", {}).get("value", "").split("/")[-1]
                person_name = result.get("personLabel", {}).get("value", "")
                position = result.get("positionLabel", {}).get("value", "")
                start_date = result.get("startDate", {}).get("value")
                end_date = result.get("endDate", {}).get("value")

                if person_name and position:
                    executives.append(
                        {
                            "person_qid": person_qid,
                            "person_name": person_name,
                            "position": position,
                            "start_date": start_date,
                            "end_date": end_date,
                        }
                    )

            return {
                "company_qid": qid,
                "executives": executives,
                "source": "wikidata",
                "query_timestamp": time.time(),
            }

        except Exception as e:
            logger.debug(f"Failed to query Wikidata for {qid}: {e}")
            return None
