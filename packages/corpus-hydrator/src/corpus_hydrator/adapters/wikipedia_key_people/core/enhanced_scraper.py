"""
Enhanced Wikipedia Key People Scraper

This module extends the base scraper to handle complex page structures including:
- Nested tables within sections
- Tables outside the main infobox
- Board of directors and other complex structures
- Multiple extraction strategies for maximum coverage
"""

import logging
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urljoin
import re

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import warnings
from bs4 import BeautifulSoup, NavigableString, Tag, XMLParsedAsHTMLWarning

from .base_extractor import BaseWikipediaPeopleExtractor
from corpus_types.schemas.wikipedia_key_people import WikipediaKeyPerson

# Set up logging
logger = logging.getLogger(__name__)

# ────────── Enhanced HTTP Setup ──────────
HEADERS = {"User-Agent": "jake@jakedugan.com"}

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

wiki_adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50)
session.mount("https://en.wikipedia.org", wiki_adapter)

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)


class EnhancedWikipediaKeyPeopleExtractor(BaseWikipediaPeopleExtractor):
    """
    Enhanced extractor that handles complex page structures beyond the basic infobox.
    """

    def extract_key_people(self, company: Dict[str, Any]) -> List[WikipediaKeyPerson]:
        """
        Extract key people using multiple strategies for maximum coverage.
        """
        ticker = company["ticker"]
        company_name = company["company_name"]
        wikipedia_url = company["wikipedia_url"]

        logger.debug(f"Enhanced extraction for {ticker} from {wikipedia_url}")

        try:
            # Add rate limiting
            import time
            time.sleep(1.0 / self.config.scraping.wikipedia_rate_limit)

            r = session.get(wikipedia_url, timeout=self.config.scraping.request_timeout)
            r.raise_for_status()

            soup = BeautifulSoup(r.text, "lxml")
            all_people = []

            # Strategy 1: Original infobox extraction
            logger.debug(f"Strategy 1: Infobox extraction for {ticker}")
            infobox_people = self._extract_from_infobox(soup, company)
            all_people.extend(infobox_people)
            logger.debug(f"Found {len(infobox_people)} people from infobox")

            # Strategy 2: Section-based extraction (Board of directors, etc.)
            logger.debug(f"Strategy 2: Section extraction for {ticker}")
            section_people = self._extract_from_sections(soup, company)
            all_people.extend(section_people)
            logger.debug(f"Found {len(section_people)} people from sections")

            # Strategy 3: Table-based extraction (any table with people data)
            logger.debug(f"Strategy 3: Table extraction for {ticker}")
            table_people = self._extract_from_all_tables(soup, company)
            all_people.extend(table_people)
            logger.debug(f"Found {len(table_people)} people from tables")

            # Strategy 4: List-based extraction (any lists that might contain people)
            logger.debug(f"Strategy 4: List extraction for {ticker}")
            list_people = self._extract_from_lists(soup, company)
            all_people.extend(list_people)
            logger.debug(f"Found {len(list_people)} people from lists")

            # Clean and normalize the extracted people
            cleaned_people = []
            for person in all_people:
                cleaned_person = self._clean_extracted_person(person)
                if cleaned_person:
                    cleaned_people.append(cleaned_person)

            # Remove duplicates based on clean name
            seen_names = set()
            unique_people = []

            for person in cleaned_people:
                name_key = person.clean_name.lower().strip()
                if name_key not in seen_names and len(name_key) > 2:
                    seen_names.add(name_key)
                    unique_people.append(person)

            logger.info(f"Enhanced extraction found {len(unique_people)} unique people for {ticker}")
            return unique_people[:self.config.scraping.max_people_per_company]

        except Exception as e:
            logger.warning(f"Enhanced extraction failed for {ticker}: {e}")
            return []

    def _extract_from_infobox(self, soup: BeautifulSoup, company: Dict[str, Any]) -> List[WikipediaKeyPerson]:
        """Extract from the main infobox (original method)."""
        people = []

        infobox = soup.select_one("table.infobox.vcard")
        if not infobox:
            return people

        # Search for sections containing key people
        for tr in infobox.find_all("tr"):
            th = tr.find("th")
            td = tr.find("td")

            if not th or not td:
                continue

            heading = th.get_text(" ", strip=True).lower()

            # Check if this section contains key people
            if any(keyword in heading for keyword in self.config.content.role_keywords):
                logger.debug(f"Found key people section in infobox: '{heading}'")
                section_people = self._extract_people_from_section(td, company)
                people.extend(section_people)

        return people

    def _extract_from_sections(self, soup: BeautifulSoup, company: Dict[str, Any]) -> List[WikipediaKeyPerson]:
        """Extract from page sections like 'Board of directors', 'Leadership', etc."""
        people = []

        # Find all headings that might contain people information
        headings = soup.find_all(['h2', 'h3', 'h4'])

        for heading in headings:
            heading_text = heading.get_text().strip().lower()

            # Check if this section might contain people
            if any(keyword in heading_text for keyword in [
                'board', 'director', 'leadership', 'management', 'executive',
                'officer', 'team', 'people', 'chairman', 'president',
                'current and former', 'former executives', 'current executives'
            ]):
                logger.debug(f"Found potential people section: '{heading_text}'")

                # Get the content after this heading
                section_content = self._get_section_content(heading)
                if section_content:
                    section_people = self._extract_from_section_content(section_content, company, heading_text)
                    people.extend(section_people)

        return people

    def _get_section_content(self, heading) -> Optional[BeautifulSoup]:
        """Get the content that follows a heading."""
        content = []

        # Start from the heading and collect all following elements until next heading
        current = heading.next_sibling

        while current:
            if current.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                # Stop at next heading
                break

            if current.name and current.name not in ['script', 'style']:
                content.append(str(current))

            current = current.next_sibling

        if content:
            # Create a mini soup from the content
            content_html = ''.join(content)
            return BeautifulSoup(content_html, 'lxml')

        return None

    def _extract_from_section_content(self, section_soup: BeautifulSoup, company: Dict[str, Any], section_name: str) -> List[WikipediaKeyPerson]:
        """Extract people from section content."""
        people = []

        # Look for tables in this section
        tables = section_soup.find_all('table')
        for table in tables:
            table_people = self._extract_from_table(table, company, f"section_{section_name}")
            people.extend(table_people)

        # Look for lists in this section
        lists = section_soup.find_all(['ul', 'ol'])
        for list_elem in lists:
            # Skip navigation/sidebar lists
            if list_elem.find_parent(class_=lambda x: x and any(term in ' '.join(x) for term in ['nav', 'menu', 'sidebar', 'navigation'])):
                continue


            list_people = self._extract_from_list(list_elem, company, f"section_{section_name}")
            people.extend(list_people)

        # Look for divs that might contain people info
        divs = section_soup.find_all('div', class_=lambda x: x and any(term in ' '.join(x) for term in ['board', 'director', 'executive']))
        for div in divs:
            div_people = self._extract_from_element(div, company, f"section_{section_name}")
            people.extend(div_people)

        return people

    def _extract_from_all_tables(self, soup: BeautifulSoup, company: Dict[str, Any]) -> List[WikipediaKeyPerson]:
        """Extract from tables that are clearly about people (board, executives, etc.)."""
        people = []

        # Find tables that are specifically about people/leadership
        # Look for tables in specific sections or with specific headers
        people_sections = [
            'board of directors', 'executive officers', 'leadership team',
            'senior management', 'key executives', 'corporate officers',
            'board members', 'executive team', 'current and former executives',
            'former executives', 'current executives'
        ]

        for section_name in people_sections:
            # Find headings that match people sections
            headings = soup.find_all(['h2', 'h3', 'h4'], text=lambda t: t and section_name in t.lower())

            for heading in headings:
                # Get content after this heading
                section_content = self._get_section_content(heading)
                if section_content:
                    tables = section_content.find_all('table')
                    for table in tables:
                        table_people = self._extract_from_table(table, company, f"section_{section_name}")
                        people.extend(table_people)

        return people

    def _extract_from_table(self, table, company: Dict[str, Any], source: str) -> List[WikipediaKeyPerson]:
        """Extract people from a table element."""
        people = []

        # Look for table rows
        rows = table.find_all('tr')

        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 2:
                # Check each cell for people data
                for cell in cells:
                    cell_text = cell.get_text(" ", strip=True)

                    # Look for links in this cell (often indicate people)
                    links = cell.find_all('a', href=True)
                    for link in links:
                        link_text = link.get_text(" ", strip=True)
                        if self._looks_like_person_name(link_text):
                            person = self._create_person_from_text(link_text, company, f"{source}_link")
                            if person:
                                people.append(person)

                    # Also check the full cell text
                    if self._contains_people_data(cell_text):
                        # Try to extract people from this cell
                        cell_people = self._extract_people_from_text(cell_text, company, source)
                        people.extend(cell_people)

        return people

    def _extract_from_lists(self, soup: BeautifulSoup, company: Dict[str, Any]) -> List[WikipediaKeyPerson]:
        """Extract from lists that are clearly about people in specific sections."""
        people = []

        # Only extract from lists in specific people-related sections
        people_sections = [
            'board of directors', 'executive officers', 'leadership team',
            'senior management', 'key executives', 'corporate officers',
            'board members', 'executive team'
        ]

        for section_name in people_sections:
            # Find headings that match people sections
            headings = soup.find_all(['h2', 'h3', 'h4'], text=lambda t: t and section_name in t.lower())

            for heading in headings:
                # Get content after this heading
                section_content = self._get_section_content(heading)
                if section_content:
                    lists = section_content.find_all(['ul', 'ol'])
                    for list_elem in lists:
                        # Skip navigation/sidebar lists
                        if list_elem.find_parent(class_=lambda x: x and any(term in ' '.join(x) for term in ['nav', 'menu', 'sidebar', 'navigation'])):
                            continue

                        list_people = self._extract_from_list(list_elem, company, f"section_{section_name}")
                        people.extend(list_people)

        return people

    def _extract_from_list(self, list_elem, company: Dict[str, Any], source: str) -> List[WikipediaKeyPerson]:
        """Extract people from a list element."""
        people = []

        list_items = list_elem.find_all('li')

        for i, item in enumerate(list_items):
            item_text = item.get_text(" ", strip=True)

            # Check if this looks like a person entry
            if self._contains_people_data(item_text):
                item_people = self._extract_people_from_text(item_text, company, source)
                people.extend(item_people)

        return people

    def _extract_from_element(self, element, company: Dict[str, Any], source: str) -> List[WikipediaKeyPerson]:
        """Extract people from any element."""
        people = []

        element_text = element.get_text(" ", strip=True)

        if self._contains_people_data(element_text):
            element_people = self._extract_people_from_text(element_text, company, source)
            people.extend(element_people)

        return people

    def _extract_people_from_text(self, text: str, company: Dict[str, Any], source: str) -> List[WikipediaKeyPerson]:
        """Extract people from arbitrary text. Enhanced to handle structured formats."""
        people = []

        # First, try to handle structured format like "Name, Title and description [citations]"
        structured_person = self._extract_structured_person(text, company, source)
        if structured_person:
            people.append(structured_person)
            return people

        # Fallback to original parsing logic
        # Split by common separators
        separators = [', ', '; ', ' and ', ' & ', ' | ', '\n']

        for separator in separators:
            if separator in text:
                parts = text.split(separator)
                for part in parts:
                    part = part.strip()
                    if len(part) > 3 and self._looks_like_person_name(part):
                        person = self._create_person_from_text(part, company, f"{source}_text")
                        if person:
                            people.append(person)
                break

        # If no separators worked, try the whole text
        if not people and len(text) > 3 and self._looks_like_person_name(text):
            person = self._create_person_from_text(text, company, f"{source}_full")
            if person:
                people.append(person)

        return people

    def _extract_structured_person(self, text: str, company: Dict[str, Any], source: str) -> Optional[WikipediaKeyPerson]:
        """Extract person from structured format like 'Name, Title and description [citations]'."""
        if not text or ',' not in text:
            return None

        # Clean the text first - remove citations like [62][63]
        cleaned_text = re.sub(r'\[\d+\]', '', text).strip()

        # Split on first comma to separate name from title/description
        parts = cleaned_text.split(',', 1)
        if len(parts) != 2:
            return None

        name_part = parts[0].strip()
        title_part = parts[1].strip()

        # Check if the name part looks like a person name
        if not self._looks_like_person_name(name_part):
            return None

        # Clean up the title part - remove extra prefixes like "former", "current"
        title_clean = title_part

        # Handle "former" and "current" prefixes
        if title_clean.lower().startswith('former '):
            title_clean = title_clean[7:].strip()
        elif title_clean.lower().startswith('current '):
            title_clean = title_clean[8:].strip()

        # Create the person with structured data
        try:
            from corpus_types.schemas.wikipedia_key_people import WikipediaKeyPerson
            from datetime import datetime

            # Create the person with cleaned data
            temp_person = WikipediaKeyPerson(
                ticker=company['ticker'],
                company_name=company['company_name'],
                raw_name=name_part,
                clean_name=name_part,  # Will be cleaned by _clean_extracted_person
                clean_title=title_clean,  # Will be cleaned by _clean_extracted_person
                wikipedia_url=company['wikipedia_url'],
                extraction_method='structured_parsing',
                confidence_score=0.95,  # High confidence for structured format
                scraped_at=datetime.now(),
                parse_success=True
            )

            # Apply the same cleaning as other methods
            return self._clean_extracted_person(temp_person)
        except Exception as e:
            logger.warning(f"Failed to create structured person from '{text}': {e}")
            return None

    def _clean_extracted_person(self, person: WikipediaKeyPerson) -> Optional[WikipediaKeyPerson]:
        """Clean and normalize an extracted person entry."""
        try:
            # Clean the name
            clean_name = self._clean_person_name(person.clean_name)

            # Clean the title
            clean_title = self._clean_person_title(person.clean_title)

            # Skip if name cleaning resulted in invalid data
            if not clean_name or len(clean_name.strip()) < 2:
                return None

            # Skip entries that are clearly not people (like company descriptions)
            skip_patterns = [
                'as bankamericard', 'as visa', 'inc', 'corp', 'ltd', 'llc',
                'company', 'corporation', 'systems', 'software', 'group'
            ]

            if any(pattern in clean_name.lower() for pattern in skip_patterns):
                return None

            # Create cleaned person
            from corpus_types.schemas.wikipedia_key_people import WikipediaKeyPerson
            from datetime import datetime

            return WikipediaKeyPerson(
                ticker=person.ticker,
                company_name=person.company_name,
                raw_name=person.raw_name,
                clean_name=clean_name,
                clean_title=clean_title,
                wikipedia_url=person.wikipedia_url,
                extraction_method=person.extraction_method,
                confidence_score=person.confidence_score,
                scraped_at=person.scraped_at,
                parse_success=person.parse_success
            )

        except Exception as e:
            logger.warning(f"Failed to clean person {person.clean_name}: {e}")
            return None

    def _clean_person_name(self, name: str) -> str:
        """Clean a person name by aggressively removing ALL parentheses and brackets."""
        if not name:
            return ""

        # Remove ALL content between parentheses ( )
        name = re.sub(r'\([^)]*\)', '', name)

        # Remove ALL content between square brackets [ ]
        name = re.sub(r'\[[^\]]*\]', '', name)

        # Remove quotation marks
        name = name.replace('"', '').replace("'", "")

        # Remove any remaining stray parentheses, brackets, or punctuation
        name = re.sub(r'[\(\)\[\]\{\}\<\>]', '', name)

        # Remove common punctuation that shouldn't be in names
        name = re.sub(r'[,:;]', '', name)

        # Normalize whitespace - collapse multiple spaces into single spaces
        name = re.sub(r'\s+', ' ', name)

        # Remove leading/trailing whitespace
        cleaned = name.strip()

        # If the result is too short or empty after cleanup, return empty
        if len(cleaned) < 2:
            return ""

        # Filter out entries that are just titles/roles without actual names
        # Check if the cleaned result contains only job titles and punctuation
        title_only_pattern = r'^(?:(?:chairman|president|executive|ceo|chief|officer|director|senior|vice|managing|general|cfo|coo|cto|cio|vp|svp|evp|manager|supervisor|lead|head|principal|partner|associate|analyst|specialist|coordinator|administrator|consultant|advisor|counsel|attorney|lawyer|accountant|auditor|engineer|developer|architect|designer|scientist|researcher|technician|operator|representative|assistant|secretary|clerk|worker|employee|staff|member|representative|delegate|ambassador|commissioner|inspector|examiner|investigator|auditor|controller|treasurer|secretary|trustee|director|manager|supervisor|coordinator|administrator|executive|officer|president|chairman|ceo|chief|cfo|coo|cto|cio|vp|svp|evp)(?:\s|,|&|and)*)+$'
        if re.match(title_only_pattern, cleaned, re.IGNORECASE):
            return ""

        # If the result contains only punctuation or common non-name words, return empty
        if re.match(r'^[^\w]*$', cleaned) or re.match(r'^(and|or|the|a|an|of|for|to|in|on|at|by|with|as|but|if|then|than|so|yet|nor|for|from|into|onto|upon|over|under|above|below|between|among|through|during|before|after|since|until|while|because|although|though|unless|whether|where|when|why|how|what|which|who|whom|whose|that|this|these|those|here|there|everywhere|nowhere|anywhere|somewhere|every|some|any|all|both|neither|either|each|every|other|another|such|what|whatever|whichever|whoever|whomever|whosever)[^\w]*$', cleaned, re.IGNORECASE):
            return ""

        return cleaned

    def _clean_person_title(self, title: str) -> str:
        """Clean a person title by removing parentheses and quotation marks."""
        if not title:
            return "Executive"

        # Remove parentheses and their contents
        title = re.sub(r'\([^)]*\)', '', title)

        # Remove quotation marks
        title = title.replace('"', '').replace("'", "")

        # Normalize whitespace and remove extra commas
        title = re.sub(r'\s*,\s*', ' ', title)
        title = ' '.join(title.split())

        # Capitalize properly
        if title.lower() == "executive":
            return "Executive"

        return title.strip()

    def _contains_people_data(self, text: str) -> bool:
        """Check if text likely contains people information. Much more restrictive now."""
        if not text or len(text.strip()) < 3:
            return False

        text_lower = text.lower()

        # Exclude obvious non-people content
        exclude_patterns = [
            'about wikipedia', 'download', 'pdf', 'navigation', 'menu',
            'sidebar', 'category', 'talk', 'edit', 'history', 'search',
            'amazon.com', 'prime', 'echo', 'fire', 'alexa', 'aws',
            'wikipedia', 'commons', 'mediawiki', 'wikimedia',
            'bahasa', 'español', 'français', 'deutsch', 'italiano',
            'português', 'русский', '中文', '日本語', 'العربية',
            'nvidia', 'geforce', 'rtx', 'tesla', 'model', 'cybertruck',
            'apple', 'iphone', 'ipad', 'macbook', 'watch', 'airpods',
            'microsoft', 'windows', 'office', 'azure', 'xbox',
            'google', 'android', 'chrome', 'youtube', 'gmail'
        ]

        if any(pattern in text_lower for pattern in exclude_patterns):
            return False

        # Only check for people-related keywords in specific contexts
        people_keywords = [
            'director of', 'executive officer', 'officer of', 'chairman of',
            'president of', 'ceo of', 'cfo of', 'board of', 'leadership of',
            'management of', 'chief executive', 'chief financial'
        ]

        if any(keyword in text_lower for keyword in people_keywords):
            return True

        # Check if it looks like a person name with title (e.g., "John Smith, CEO")
        words = text.split()
        if len(words) >= 2:
            # Look for patterns like "Name, Title" or "Title: Name"
            if ',' in text:
                parts = [part.strip() for part in text.split(',')]
                if len(parts) == 2:
                    name_part, title_part = parts
                    if (self._looks_like_person_name(name_part) and
                        any(title in title_part.lower() for title in ['ceo', 'cfo', 'director', 'president', 'chairman', 'officer'])):
                        return True

        return False

    def _looks_like_person_name(self, text: str) -> bool:
        """Check if text looks like a person name. Very restrictive now."""
        if not text or len(text) < 3 or len(text) > 80:
            return False

        # Remove common non-name elements
        clean_text = re.sub(r'\([^)]*\)', '', text)  # Remove parentheses
        clean_text = re.sub(r'\[.*\]', '', clean_text)  # Remove brackets
        clean_text = re.sub(r'[^\w\s]', '', clean_text)  # Remove punctuation except spaces
        clean_text = clean_text.strip()

        words = clean_text.split()

        # Must have exactly 2-4 words (most common for person names)
        if len(words) < 2 or len(words) > 4:
            return False

        # All words should start with capital letters
        if not all(word and word[0].isupper() for word in words):
            return False

        # Should not contain numbers
        if any(char.isdigit() for char in clean_text):
            return False

        # Should not contain common company/product indicators
        company_indicators = [
            'inc', 'corp', 'ltd', 'llc', 'company', 'corporation',
            'systems', 'software', 'services', 'group', 'holdings',
            'technologies', 'solutions', 'international', 'global'
        ]

        if any(indicator in clean_text.lower() for indicator in company_indicators):
            return False

        # Should not be all caps (likely an acronym)
        if clean_text.isupper():
            return False

        return True

    def _create_person_from_text(self, text: str, company: Dict[str, Any], source: str) -> Optional[WikipediaKeyPerson]:
        """Create a person object from text."""
        # Use the existing parsing method
        person = self._parse_person_text(text, company)

        # Apply cleaning if person was created
        if person:
            return self._clean_extracted_person(person)

        return None
