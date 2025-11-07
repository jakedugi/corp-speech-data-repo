"""
Data Normalization for Wikipedia Key People Scraper

This module provides data normalization and cleaning functionality
for extracted Wikipedia key people data.

Features:
- Unicode NFC normalization
- Controlled vocabulary mapping
- Deduplication with conflict resolution
- Person name standardization
"""

import logging
import re
import unicodedata
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

from corpus_types.schemas.wikipedia_key_people import WikipediaKeyPerson

from .utils.enums import NON_PERSON_TERMS, ROLE_VOCABULARY, ExtractionMethod

logger = logging.getLogger(__name__)


class WikipediaKeyPeopleNormalizer:
    """Normalizes and cleans extracted Wikipedia key people data."""

    # ============================================================================
    # Centralized Regex Patterns (moved from scattered locations)
    # ============================================================================

    # Name cleaning patterns
    NAME_CLEANING_PATTERNS = [
        # Remove footnote references like [1], [2], etc.
        (r"\[\d+\]", ""),
        # Remove parenthetical content (except generational suffixes)
        (r"\((?!Jr\.?|Sr\.?|III|IV|V|VI|VII|VIII|IX|X)\w[^)]*\)", ""),
        # Clean up extra whitespace
        (r"\s+", " "),
        # Remove trailing/leading whitespace
        (r"^\s+|\s+$", ""),
    ]

    # Title normalization patterns
    TITLE_NORMALIZATION_PATTERNS = [
        # C-Suite abbreviations
        (r"\bCEO\b", "Chief Executive Officer"),
        (r"\bCFO\b", "Chief Financial Officer"),
        (r"\bCOO\b", "Chief Operating Officer"),
        (r"\bCTO\b", "Chief Technology Officer"),
        (r"\bCIO\b", "Chief Information Officer"),
        (r"\bCMO\b", "Chief Marketing Officer"),
        (r"\bCHRO\b", "Chief Human Resources Officer"),
        (r"\bCSO\b", "Chief Strategy Officer"),
        (r"\bCLO\b", "Chief Legal Officer"),
        (r"\bCAO\b", "Chief Administrative Officer"),
        (r"\bCBO\b", "Chief Business Officer"),
        # Leadership titles
        (r"\bChair\b", "Chair"),
        (r"\bChairman\b", "Chairman"),
        (r"\bChairwoman\b", "Chairwoman"),
        (r"\bChairperson\b", "Chairperson"),
        (r"\bPresident\b", "President"),
        (r"\bVP\b", "Vice President"),
        (r"\bSVP\b", "Senior Vice President"),
        (r"\bEVP\b", "Executive Vice President"),
        # Board titles
        (r"\bDirector\b", "Board Member"),
        (r"\bDir\.?\b", "Board Member"),
        # Operational titles
        (r"\bGC\b", "General Counsel"),
        (r"\bTreas\.?\b", "Treasurer"),
        (r"\bSec\.?\b", "Secretary"),
    ]

    # Patterns to identify non-person entries
    NON_PERSON_PATTERNS = [
        r"^\s*(the\s+)?company\s*$",
        r"^\s*founders?\s*$",
        r"^\s*leadership\s+team\s*$",
        r"^\s*management\s+team\s*$",
        r"^\s*executive\s+team\s*$",
        r"^\s*board\s+of\s+directors\s*$",
        r"^\s*our\s+team\s*$",
        r"^\s*senior\s+management\s*$",
        r"^\s*key\s+personnel\s*$",
        r"^\s*corporate\s+officers?\s*$",
        r"^\s*company\s+officials?\s*$",
    ]

    def __init__(self):
        """Initialize the normalizer."""
        # Initialize any state that needs to be computed
        pass

    def normalize_people(
        self, people: List[WikipediaKeyPerson]
    ) -> List[WikipediaKeyPerson]:
        """
        Normalize a list of Wikipedia key people.

        Args:
            people: List of people to normalize

        Returns:
            List of normalized people
        """
        logger.info(f"Normalizing {len(people)} people")

        normalized_people = []

        for person in people:
            try:
                # Create a copy to avoid modifying the original
                normalized = WikipediaKeyPerson(**person.dict())

                # Normalize the data
                normalized.clean_name = self.normalize_name(normalized.clean_name)
                normalized.clean_title = self.normalize_title(normalized.clean_title)

                # Update confidence score based on normalization quality
                if normalized.clean_name and normalized.clean_title:
                    # Boost confidence if both name and title look good
                    if self._is_high_quality_name(normalized.clean_name):
                        normalized.confidence_score = min(
                            1.0, normalized.confidence_score + 0.1
                        )

                normalized_people.append(normalized)

            except Exception as e:
                logger.warning(f"Failed to normalize person {person.clean_name}: {e}")
                # Keep original if normalization fails
                normalized_people.append(person)

        # Remove duplicates after normalization
        deduplicated = self.remove_duplicates(normalized_people)

        logger.info(f"Normalization complete: {len(deduplicated)} unique people")
        return deduplicated

    def normalize_name(self, name: str) -> str:
        """
        Normalize a person name using centralized patterns.

        Args:
            name: Raw name to normalize

        Returns:
            Normalized name
        """
        if not name:
            return ""

        # Apply centralized cleaning patterns
        for pattern, replacement in self.NAME_CLEANING_PATTERNS:
            name = re.sub(pattern, replacement, name, flags=re.UNICODE)

        # Title case for names (if still meaningful)
        if len(name.strip()) > 1:
            name = self._title_case_name(name)

        return name.strip()

    # --------------------------------------------------------------------------- #
    # Enhanced Normalization (v2.0)                                            #
    # --------------------------------------------------------------------------- #

    def normalize_name_unicode(self, name: str) -> str:
        """
        Normalize a person name with Unicode NFC canonical decomposition.

        This ensures consistent representation of accented characters and
        other Unicode text normalization.
        """
        if not name:
            return ""

        # Apply Unicode NFC normalization
        name = unicodedata.normalize("NFC", name)

        # Apply existing normalization
        name = self.normalize_name(name)

        return name

    def normalize_title_controlled_vocabulary(self, title: str) -> str:
        """
        Normalize a job title using controlled vocabulary mapping.

        Maps various title variations to a controlled set of canonical forms.
        """
        if not title:
            return ""

        # Apply existing normalization first
        title = self.normalize_title(title)

        # Enhanced controlled vocabulary mapping
        controlled_mappings = {
            # CEO variations
            r"\bchief executive officer\b": "Chief Executive Officer",
            r"\bchief exec\b": "Chief Executive Officer",
            r"\bchief executive\b": "Chief Executive Officer",
            r"\bexecutive director\b": "Chief Executive Officer",
            # CFO variations
            r"\bchief financial officer\b": "Chief Financial Officer",
            r"\bchief finance officer\b": "Chief Financial Officer",
            r"\bfinance director\b": "Chief Financial Officer",
            # COO variations
            r"\bchief operating officer\b": "Chief Operating Officer",
            r"\bchief operations officer\b": "Chief Operating Officer",
            # CTO variations
            r"\bchief technology officer\b": "Chief Technology Officer",
            r"\bchief tech officer\b": "Chief Technology Officer",
            # CIO variations
            r"\bchief information officer\b": "Chief Information Officer",
            r"\bchief it officer\b": "Chief Information Officer",
            # CMO variations
            r"\bchief marketing officer\b": "Chief Marketing Officer",
            # Chair variations
            r"\bchairman\b": "Chairman",
            r"\bchairwoman\b": "Chairwoman",
            r"\bchair\b": "Chair",
            r"\bchairperson\b": "Chairperson",
            # President variations
            r"\bchief executive\b": "President",
            r"\bexecutive president\b": "President",
            # Board member variations
            r"\bdirector\b": "Board Member",
            r"\bbod member\b": "Board Member",
            r"\bboard director\b": "Board Member",
        }

        # Apply mappings (case insensitive)
        for pattern, canonical in controlled_mappings.items():
            title = re.sub(pattern, canonical, title, flags=re.IGNORECASE)

        return title.strip()

    def deduplicate_people_advanced(
        self, people: List[WikipediaKeyPerson]
    ) -> List[WikipediaKeyPerson]:
        """
        Advanced deduplication with conflict resolution.

        Handles cases where the same person appears multiple times with
        different titles or confidence scores.
        """
        if not people:
            return []

        # Group by normalized name and ticker
        name_groups = defaultdict(list)

        for person in people:
            # Create normalized key
            normalized_name = self.normalize_name_unicode(person.clean_name)
            key = (normalized_name.lower(), person.ticker)

            name_groups[key].append(person)

        # Resolve duplicates
        deduplicated = []

        for (name_key, ticker), group in name_groups.items():
            if len(group) == 1:
                # No duplicates, keep as-is
                deduplicated.append(group[0])
            else:
                # Resolve duplicates by selecting best candidate
                best_person = self._resolve_duplicate_person(group)
                deduplicated.append(best_person)

        logger.info(
            f"Deduplicated {len(people)} people to {len(deduplicated)} unique entries"
        )
        return deduplicated

    def _resolve_duplicate_person(
        self, duplicates: List[WikipediaKeyPerson]
    ) -> WikipediaKeyPerson:
        """
        Resolve duplicate person entries by selecting the best candidate.

        Selection criteria (in order):
        1. Highest confidence score
        2. Most recent extraction
        3. Longest/most complete title
        4. Alphabetical preference
        """
        if len(duplicates) == 1:
            return duplicates[0]

        # Sort by multiple criteria
        sorted_duplicates = sorted(
            duplicates,
            key=lambda p: (
                -p.confidence_score,  # Highest confidence first (negative for descending)
                -p.scraped_at.timestamp() if p.scraped_at else 0,  # Most recent first
                -len(p.clean_title),  # Longest title first
                p.clean_name.lower(),  # Alphabetical fallback
            ),
        )

        # Log the resolution
        winner = sorted_duplicates[0]
        logger.debug(
            f"Resolved duplicate for {winner.clean_name}: "
            f"kept '{winner.clean_title}' (confidence: {winner.confidence_score}), "
            f"discarded {len(duplicates) - 1} other entries"
        )

        return winner

    def normalize_people_batch(
        self,
        people: List[WikipediaKeyPerson],
        unicode_normalize: bool = True,
        controlled_vocabulary: bool = True,
        deduplicate: bool = True,
    ) -> List[WikipediaKeyPerson]:
        """
        Comprehensive batch normalization with all enhancements.

        Args:
            people: List of people to normalize
            unicode_normalize: Apply Unicode NFC normalization
            controlled_vocabulary: Apply controlled vocabulary mapping
            deduplicate: Remove duplicates with conflict resolution

        Returns:
            Normalized and deduplicated list of people
        """
        if not people:
            return []

        normalized_people = []

        for person in people:
            # Create a copy to avoid modifying the original
            normalized_person = WikipediaKeyPerson(**person.dict())

            # Apply Unicode normalization
            if unicode_normalize:
                normalized_person.clean_name = self.normalize_name_unicode(
                    normalized_person.clean_name
                )

            # Apply controlled vocabulary
            if controlled_vocabulary:
                normalized_person.clean_title = (
                    self.normalize_title_controlled_vocabulary(
                        normalized_person.clean_title
                    )
                )

            normalized_people.append(normalized_person)

        # Apply deduplication
        if deduplicate:
            normalized_people = self.deduplicate_people_advanced(normalized_people)

        logger.info(
            f"Batch normalized {len(people)} people: "
            f"Unicode={unicode_normalize}, "
            f"Vocabulary={controlled_vocabulary}, "
            f"Deduplicate={deduplicate}, "
            f"Result={len(normalized_people)} people"
        )

        return normalized_people

    def normalize_title(self, title: str) -> str:
        """
        Normalize a job title using centralized patterns and controlled vocabulary.

        Args:
            title: Raw title to normalize

        Returns:
            Normalized title
        """
        if not title:
            return ""

        original_title = title

        # Apply centralized normalization patterns
        for pattern, replacement in self.TITLE_NORMALIZATION_PATTERNS:
            title = re.sub(pattern, replacement, title, flags=re.IGNORECASE)

        # Map to controlled vocabulary
        title = self._map_to_controlled_vocabulary(title)

        # Clean up spacing and capitalization
        title = self._clean_title_capitalization(title)

        # Remove extra spaces
        title = " ".join(title.split())

        if title != original_title:
            logger.debug(f"Title normalized: '{original_title}' -> '{title}'")

        return title.strip()

    def _map_to_controlled_vocabulary(self, title: str) -> str:
        """
        Map a normalized title to the controlled vocabulary.

        Args:
            title: Normalized title string

        Returns:
            Title from controlled vocabulary, or original if no match
        """
        # Direct matches in controlled vocabulary
        if title.upper() in ROLE_VOCABULARY:
            return title.upper()

        # Fuzzy matching for common variations
        title_upper = title.upper()

        # Try partial matches
        for canonical_role in ROLE_VOCABULARY:
            if canonical_role in title_upper or title_upper in canonical_role:
                return canonical_role

        # Return original if no controlled match found
        return title

    def remove_duplicates(
        self, people: List[WikipediaKeyPerson]
    ) -> List[WikipediaKeyPerson]:
        """
        Remove duplicate people based on normalized name and company.

        Args:
            people: List of people that may contain duplicates

        Returns:
            List with duplicates removed
        """
        seen = set()
        unique_people = []

        for person in people:
            # Create a unique key based on normalized name and company
            key = (
                person.clean_name.lower().strip(),
                person.ticker.upper().strip(),
                person.clean_title.lower().strip(),
            )

            if key not in seen:
                seen.add(key)
                unique_people.append(person)
            else:
                logger.debug(
                    f"Duplicate removed: {person.clean_name} ({person.ticker}) - {person.clean_title}"
                )

        if len(unique_people) < len(people):
            removed_count = len(people) - len(unique_people)
            logger.info(f"Removed {removed_count} duplicate people")

        return unique_people

    def validate_people_data(self, people: List[WikipediaKeyPerson]) -> Dict[str, Any]:
        """
        Validate the quality of people data.

        Args:
            people: List of people to validate

        Returns:
            Validation report
        """
        report = {
            "total_people": len(people),
            "valid_names": 0,
            "valid_titles": 0,
            "high_confidence": 0,
            "issues": [],
        }

        name_issues = []
        title_issues = []

        for person in people:
            # Check name quality
            if self._is_valid_name(person.clean_name):
                report["valid_names"] += 1
            else:
                name_issues.append(f"{person.ticker}: '{person.clean_name}'")

            # Check title quality
            if self._is_valid_title(person.clean_title):
                report["valid_titles"] += 1
            else:
                title_issues.append(f"{person.ticker}: '{person.clean_title}'")

            # Check confidence
            if person.confidence_score >= 0.8:
                report["high_confidence"] += 1

        # Add issues to report
        if name_issues:
            report["issues"].append(
                f"Invalid names ({len(name_issues)}): {name_issues[:5]}"
            )
        if title_issues:
            report["issues"].append(
                f"Invalid titles ({len(title_issues)}): {title_issues[:5]}"
            )

        # Calculate percentages
        if report["total_people"] > 0:
            report["name_validity_rate"] = (
                report["valid_names"] / report["total_people"]
            )
            report["title_validity_rate"] = (
                report["valid_titles"] / report["total_people"]
            )
            report["high_confidence_rate"] = (
                report["high_confidence"] / report["total_people"]
            )

        return report

    def _title_case_name(self, name: str) -> str:
        """Apply proper title casing to names."""
        if not name:
            return name

        # Split into words
        words = name.split()

        # Title case each word
        title_cased = []
        for word in words:
            # Keep certain words lowercase (like "de", "van", "von")
            lowercase_words = {"de", "van", "von", "der", "den", "di", "la", "le"}

            if word.lower() in lowercase_words and len(title_cased) > 0:
                title_cased.append(word.lower())
            else:
                title_cased.append(word.capitalize())

        return " ".join(title_cased)

    def _clean_title_capitalization(self, title: str) -> str:
        """Clean up title capitalization."""
        if not title:
            return title

        # Split into words
        words = title.split()

        # Capitalize each word except small words in the middle
        small_words = {
            "a",
            "an",
            "and",
            "as",
            "at",
            "by",
            "for",
            "in",
            "of",
            "on",
            "or",
            "the",
            "to",
            "with",
        }

        cleaned_words = []
        for i, word in enumerate(words):
            if i == 0 or i == len(words) - 1:
                # Always capitalize first and last word
                cleaned_words.append(word.capitalize())
            elif word.lower() in small_words:
                # Keep small words lowercase
                cleaned_words.append(word.lower())
            else:
                # Capitalize other words
                cleaned_words.append(word.capitalize())

        return " ".join(cleaned_words)

    def _is_high_quality_name(self, name: str) -> bool:
        """Check if a name appears to be high quality."""
        if not name or len(name.strip()) < 3:
            return False

        # Should have at least two parts (first and last name)
        parts = name.split()
        if len(parts) < 2:
            return False

        # Should not contain numbers or excessive special characters
        if re.search(r"\d", name):
            return False

        # Should not be all caps or all lowercase
        if name.isupper() or name.islower():
            return False

        return True

    def _is_valid_name(self, name: str) -> bool:
        """Check if a name is valid."""
        if not name or not name.strip():
            return False

        # Basic length check
        if len(name.strip()) < 2:
            return False

        # Should not contain only numbers or symbols
        if re.match(r"^[^a-zA-Z]*$", name):
            return False

        return True

    def _is_valid_title(self, title: str) -> bool:
        """Check if a title is valid."""
        if not title or not title.strip():
            return False

        # Basic length check
        if len(title.strip()) < 2:
            return False

        # Should not contain only numbers or symbols
        if re.match(r"^[^a-zA-Z\s]*$", title):
            return False

        # Should not be too generic
        generic_titles = {"executive", "director", "officer", "manager", "person"}
        if title.lower().strip() in generic_titles:
            return False

        return True
