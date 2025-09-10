"""Enhanced CourtListener provenance extraction with court, law, year, and company fields."""

import json
import re
from pathlib import Path
from typing import Dict, Optional, List
import logging

logger = logging.getLogger(__name__)


class CourtProvenanceExtractor:
    """Enhanced extractor for court provenance information."""

    # Court mappings based on common CourtListener court abbreviations
    COURT_MAPPINGS = {
        "scotus": "Supreme Court of the United States",
        "ca1": "United States Court of Appeals for the First Circuit",
        "ca2": "United States Court of Appeals for the Second Circuit",
        "ca3": "United States Court of Appeals for the Third Circuit",
        "ca4": "United States Court of Appeals for the Fourth Circuit",
        "ca5": "United States Court of Appeals for the Fifth Circuit",
        "ca6": "United States Court of Appeals for the Sixth Circuit",
        "ca7": "United States Court of Appeals for the Seventh Circuit",
        "ca8": "United States Court of Appeals for the Eighth Circuit",
        "ca9": "United States Court of Appeals for the Ninth Circuit",
        "ca10": "United States Court of Appeals for the Tenth Circuit",
        "ca11": "United States Court of Appeals for the Eleventh Circuit",
        "cadc": "United States Court of Appeals for the District of Columbia Circuit",
        "cafc": "United States Court of Appeals for the Federal Circuit",
        "dcd": "United States District Court for the District of Columbia",
        "nyed": "United States District Court for the Eastern District of New York",
        "nysd": "United States District Court for the Southern District of New York",
        "nynd": "United States District Court for the Northern District of New York",
        "nywd": "United States District Court for the Western District of New York",
    }

    # Law type mappings based on common legal categories
    LAW_MAPPINGS = {
        "antitrust": "Antitrust Law",
        "securities": "Securities Law",
        "patent": "Patent Law",
        "copyright": "Copyright Law",
        "trademark": "Trademark Law",
        "contract": "Contract Law",
        "tort": "Tort Law",
        "constitutional": "Constitutional Law",
        "administrative": "Administrative Law",
        "environmental": "Environmental Law",
        "labor": "Labor Law",
        "tax": "Tax Law",
        "bankruptcy": "Bankruptcy Law",
        "criminal": "Criminal Law",
        "civil": "Civil Law",
    }

    def __init__(self):
        self.court_pattern = re.compile(r"([a-z]+(?:\d+)?)")
        self.year_pattern = re.compile(r"(?:19|20)\d{2}")
        self.company_pattern = re.compile(
            r"\b(?:corp|corporation|inc|llc|ltd|co|company|incorporated|limited|corp\.|inc\.|llc\.|ltd\.|co\.)\b",
            re.IGNORECASE
        )

    def extract_court_from_doc_id(self, doc_id: str) -> Optional[str]:
        """Extract court name from document ID or path."""
        if not doc_id:
            return None

        # Try to match court abbreviation in doc_id
        match = self.court_pattern.search(doc_id)
        if match:
            court_abbrev = match.group(1).lower()
            return self.COURT_MAPPINGS.get(court_abbrev, court_abbrev)

        return None

    def extract_court_from_path(self, path: str) -> Optional[str]:
        """Extract court information from file path."""
        if not path:
            return None

        # Look for court abbreviation in path
        match = self.court_pattern.search(path)
        if match:
            court_abbrev = match.group(1).lower()
            return self.COURT_MAPPINGS.get(court_abbrev, court_abbrev)

        return None

    def extract_year_from_case_id(self, case_id: str) -> Optional[int]:
        """Extract year from case identifier."""
        if not case_id:
            return None

        match = self.year_pattern.search(case_id)
        if match:
            try:
                return int(match.group(0))
            except ValueError:
                pass
        return None

    def extract_year_from_content(self, content: str) -> Optional[int]:
        """Extract year from document content."""
        if not content:
            return None

        # Look for years in a reasonable range (1900-2030)
        years = self.year_pattern.findall(content)
        valid_years = []
        for year_str in years:
            try:
                year = int(year_str)
                if 1900 <= year <= 2030:
                    valid_years.append(year)
            except ValueError:
                continue

        # Return the most recent valid year if any found
        return max(valid_years) if valid_years else None

    def extract_law_from_content(self, content: str) -> Optional[str]:
        """Extract law type from document content."""
        if not content:
            return None

        content_lower = content.lower()

        # Check for specific legal terms that indicate law type
        for law_key, law_name in self.LAW_MAPPINGS.items():
            if law_key in content_lower:
                return law_name

        return None

    def extract_company_from_content(self, content: str) -> Optional[str]:
        """Extract company name from document content."""
        if not content:
            return None

        # Look for company mentions near company indicators
        sentences = content.split('.')
        for sentence in sentences:
            if self.company_pattern.search(sentence):
                # Try to extract company name before the indicator
                words = sentence.split()
                for i, word in enumerate(words):
                    if self.company_pattern.match(word):
                        # Look backwards for potential company name
                        start_idx = max(0, i - 3)
                        potential_name = ' '.join(words[start_idx:i])
                        if potential_name and len(potential_name) > 3:
                            return potential_name.strip()

        return None

    def extract_provenance_from_quote(self, quote: Dict[str, any]) -> Dict[str, any]:
        """Extract all provenance fields from a quote dictionary."""
        result = {}

        # Extract court information
        doc_id = quote.get("doc_id", "")
        case_id = quote.get("case_id", "")
        content = quote.get("context", "") or quote.get("text", "")
        src_path = quote.get("_metadata_src_path", "")

        # Try multiple sources for court extraction
        result["court"] = (
            self.extract_court_from_doc_id(doc_id) or
            self.extract_court_from_path(src_path) or
            self.extract_court_from_path(case_id)
        )

        # Extract year information
        result["year"] = (
            self.extract_year_from_case_id(case_id) or
            self.extract_year_from_content(content) or
            quote.get("case_year")
        )

        # Extract law type
        result["law"] = self.extract_law_from_content(content)

        # Extract company information
        result["company"] = (
            self.extract_company_from_content(content) or
            self.extract_company_from_content(quote.get("speaker", ""))
        )

        return result

    def enrich_quotes_with_provenance(self, quotes: List[Dict[str, any]]) -> List[Dict[str, any]]:
        """Add provenance fields to a list of quotes."""
        enriched_quotes = []

        for quote in quotes:
            provenance = self.extract_provenance_from_quote(quote)

            # Create enriched quote
            enriched_quote = quote.copy()
            enriched_quote.update(provenance)
            enriched_quotes.append(enriched_quote)

        return enriched_quotes

    def extract_provenance_from_doc(self, doc: Dict[str, any]) -> Dict[str, any]:
        """Extract provenance fields from a document dictionary."""
        result = {}

        # Extract court information
        doc_id = doc.get("doc_id", "")
        source_uri = doc.get("source_uri", "")
        meta = doc.get("meta", {})

        result["court"] = (
            meta.get("court") or
            self.extract_court_from_doc_id(doc_id) or
            self.extract_court_from_path(source_uri)
        )

        # Extract year from metadata or content
        result["year"] = (
            meta.get("year") or
            self.extract_year_from_content(doc.get("raw_text", "")) or
            self.extract_year_from_case_id(str(meta.get("docket", "")))
        )

        # Extract law type from content
        result["law"] = self.extract_law_from_content(doc.get("raw_text", ""))

        # Extract company from content or metadata
        result["company"] = (
            meta.get("party") or
            self.extract_company_from_content(doc.get("raw_text", ""))
        )

        return result


def add_provenance_to_quotes_batch(
    quotes_file: Path,
    output_file: Path,
    extractor: Optional[CourtProvenanceExtractor] = None
) -> None:
    """Process a quotes JSONL file and add provenance fields."""
    if extractor is None:
        extractor = CourtProvenanceExtractor()

    logger.info(f"Processing quotes from {quotes_file}")

    with open(quotes_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8') as outfile:

        for line_num, line in enumerate(infile, 1):
            if line_num % 1000 == 0:
                logger.info(f"Processed {line_num} quotes")

            try:
                quote = json.loads(line.strip())
                provenance = extractor.extract_provenance_from_quote(quote)

                # Merge provenance fields
                enriched_quote = quote.copy()
                enriched_quote.update(provenance)

                outfile.write(json.dumps(enriched_quote, ensure_ascii=False) + '\n')

            except Exception as e:
                logger.error(f"Error processing line {line_num}: {e}")
                continue

    logger.info(f"Enriched quotes written to {output_file}")


def add_provenance_to_docs_batch(
    docs_file: Path,
    output_file: Path,
    extractor: Optional[CourtProvenanceExtractor] = None
) -> None:
    """Process a documents JSONL file and add provenance fields."""
    if extractor is None:
        extractor = CourtProvenanceExtractor()

    logger.info(f"Processing documents from {docs_file}")

    with open(docs_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8') as outfile:

        for line_num, line in enumerate(infile, 1):
            if line_num % 1000 == 0:
                logger.info(f"Processed {line_num} documents")

            try:
                doc = json.loads(line.strip())
                provenance = extractor.extract_provenance_from_doc(doc)

                # Merge provenance fields
                enriched_doc = doc.copy()
                enriched_doc.update(provenance)

                outfile.write(json.dumps(enriched_doc, ensure_ascii=False) + '\n')

            except Exception as e:
                logger.error(f"Error processing line {line_num}: {e}")
                continue

    logger.info(f"Enriched documents written to {output_file}")
