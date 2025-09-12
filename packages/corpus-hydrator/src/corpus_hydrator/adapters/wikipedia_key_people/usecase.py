"""
Use Cases for Wikipedia Key People Scraper

This module contains the main business logic and orchestration
for the Wikipedia key people extraction functionality.
"""

import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from .config import WikipediaKeyPeopleScraperConfig
from .core.scraper import WikipediaKeyPeopleScraper, WikipediaLinkExtractor
from .writer import WikipediaKeyPeopleWriter
from .normalize import WikipediaKeyPeopleNormalizer
from corpus_types.schemas.wikipedia_key_people import (
    WikipediaExtractionResult,
    WikipediaKeyPerson,
    WikipediaCompany,
    NormalizedCompany,
    NormalizedPerson,
    NormalizedRole,
    NormalizedAppointment
)

logger = logging.getLogger(__name__)


class WikipediaKeyPeopleUseCase:
    """Main use case for Wikipedia key people extraction."""

    def __init__(self, config: WikipediaKeyPeopleScraperConfig):
        """Initialize the use case."""
        self.config = config
        self.scraper = WikipediaKeyPeopleScraper(config.scraper)
        self.writer = WikipediaKeyPeopleWriter(config.output_dir)

        # Configure logging
        logging.basicConfig(
            level=getattr(logging, config.log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    def extract_index_key_people(self, index_name: str) -> WikipediaExtractionResult:
        """
        Extract key people from all companies in an index.

        Args:
            index_name: Name of the index to process

        Returns:
            ExtractionResult with all extracted data
        """
        logger.info(f"Starting key people extraction for {index_name}")

        try:
            # Extract key people using the scraper
            result = self.scraper.scrape_index(index_name)

            if result.success and result.key_people:
                # Save results if successful
                self.writer.write_results(result, index_name)

                if self.config.save_intermediate:
                    self.writer.write_intermediate_results(result, index_name)

            logger.info(f"Completed extraction for {index_name}: {len(result.key_people)} people from {result.companies_successful} companies")
            return result

        except Exception as e:
            logger.error(f"Failed to extract key people for {index_name}: {e}")
            # Return failed result
            result = WikipediaExtractionResult(
                operation_id=f"failed_{index_name}",
                index_name=index_name,
                success=False,
                error_message=str(e)
            )
            result.mark_completed()
            return result

    def extract_multiple_indices(self, index_names: List[str]) -> Dict[str, WikipediaExtractionResult]:
        """
        Extract key people from multiple indices.

        Args:
            index_names: List of index names to process

        Returns:
            Dictionary mapping index names to their results
        """
        logger.info(f"Starting extraction for {len(index_names)} indices: {', '.join(index_names)}")

        results = {}

        # Process indices sequentially to avoid overwhelming the system
        for index_name in index_names:
            logger.info(f"Processing index: {index_name}")
            result = self.extract_index_key_people(index_name)
            results[index_name] = result

        # Log summary
        total_companies = sum(r.companies_processed for r in results.values())
        total_successful = sum(r.companies_successful for r in results.values())
        total_people = sum(r.total_key_people for r in results.values())

        logger.info("Multi-index extraction complete:")
        logger.info(f"  Total companies processed: {total_companies}")
        logger.info(f"  Total companies successful: {total_successful}")
        logger.info(f"  Total key people extracted: {total_people}")

        if total_successful > 0:
            logger.info(f"  Average key people per company: {total_people/total_successful:.1f}")

        return results

    def extract_company_key_people(self, company_url: str, ticker: str, company_name: str) -> Optional[List[WikipediaKeyPerson]]:
        """
        Extract key people from a specific company.

        Args:
            company_url: Wikipedia URL for the company
            ticker: Company ticker symbol
            company_name: Company name

        Returns:
            List of key people or None if extraction failed
        """
        logger.info(f"Extracting key people for {ticker} ({company_name})")

        try:
            # Create a mock company for the scraper
            company_data = {
                "ticker": ticker,
                "company_name": company_name,
                "wikipedia_url": company_url,
                "index_name": "custom"
            }

            # Extract key people
            people = self.scraper.people_extractor.extract_key_people(company_data)

            logger.info(f"Extracted {len(people)} key people for {ticker}")
            return people

        except Exception as e:
            logger.error(f"Failed to extract key people for {ticker}: {e}")
            return None

    def validate_extraction_quality(self, results: Dict[str, WikipediaExtractionResult]) -> Dict[str, Any]:
        """
        Validate the quality of extraction results.

        Args:
            results: Dictionary of extraction results

        Returns:
            Quality metrics dictionary
        """
        quality_metrics = {
            "total_indices": len(results),
            "total_companies": 0,
            "successful_companies": 0,
            "total_people": 0,
            "quality_score": 0.0,
            "issues": []
        }

        for index_name, result in results.items():
            quality_metrics["total_companies"] += result.companies_processed
            quality_metrics["successful_companies"] += result.companies_successful
            quality_metrics["total_people"] += result.total_key_people

            # Check for quality issues
            if result.companies_successful == 0:
                quality_metrics["issues"].append(f"No successful extractions for {index_name}")

            if result.companies_processed > 0:
                success_rate = result.companies_successful / result.companies_processed
                if success_rate < 0.5:
                    quality_metrics["issues"].append(f"Low success rate for {index_name}: {success_rate:.1%}")

        # Calculate overall quality score
        if quality_metrics["total_companies"] > 0:
            success_rate = quality_metrics["successful_companies"] / quality_metrics["total_companies"]
            quality_metrics["quality_score"] = success_rate

        return quality_metrics

    # --------------------------------------------------------------------------- #
    # Production Normalized Format Support (v2.0)                               #
    # --------------------------------------------------------------------------- #

    def extract_index_normalized(
        self,
        index_name: str,
        output_dir: str = "data",
        max_companies: Optional[int] = None,
        workers: int = 1
    ) -> Dict[str, Any]:
        """
        Extract key people data in normalized table format for production use.

        Returns normalized tables with relationships and governance metadata.
        """
        logger.info(f"Starting normalized extraction for {index_name}")

        # Extract companies first
        companies = self._extract_companies_from_index(index_name, max_companies)
        if not companies:
            return {"error": f"No companies found for index {index_name}"}

        # Extract key people from companies
        all_people = self._extract_people_from_companies_normalized(
            companies, workers=workers
        )

        # Apply enhanced normalization
        normalizer = WikipediaKeyPeopleNormalizer()
        normalized_people = normalizer.normalize_people_batch(
            all_people,
            unicode_normalize=True,
            controlled_vocabulary=True,
            deduplicate=True
        )

        # Convert to normalized format
        writer = WikipediaKeyPeopleWriter(output_dir)
        companies_norm, people_norm, roles_norm, appointments_norm = writer.convert_legacy_to_normalized(
            normalized_people, index_name
        )

        # Write normalized tables with deterministic sorting and manifests
        result = writer.write_normalized_tables_with_manifest(
            companies_norm,
            people_norm,
            roles_norm,
            appointments_norm,
            dataset_name=f"{index_name}_key_people",
            output_dir=output_dir,
            provider_order=["wikipedia"]
        )
        manifest = result["manifest"]

        result = {
            "index_name": index_name,
            "companies_processed": len(companies),
            "companies_successful": len(companies_norm),
            "people_extracted": len(people_norm),
            "roles_identified": len(roles_norm),
            "appointments_created": len(appointments_norm),
            "manifest": manifest,
            "output_files": [
                f"{index_name}_key_people_companies.csv",
                f"{index_name}_key_people_people.csv",
                f"{index_name}_key_people_roles.csv",
                f"{index_name}_key_people_appointments.csv",
                f"{index_name}_key_people_manifest.json"
            ]
        }

        logger.info(f"Normalized extraction completed for {index_name}")
        return result

    def _extract_companies_from_index(
        self, index_name: str, max_companies: Optional[int] = None
    ) -> List[WikipediaCompany]:
        """Extract company list from index in normalized format."""
        try:
            # Use existing link extractor to get companies
            # Pass the scraper config from the main config
            link_extractor = WikipediaLinkExtractor(self.config.scraper)
            companies_data = link_extractor.extract_company_links(index_name)

            companies = []
            for i, company_data in enumerate(companies_data):
                if max_companies and i >= max_companies:
                    break

                company = WikipediaCompany(
                    ticker=company_data['ticker'],
                    company_name=company_data['company_name'],
                    wikipedia_url=company_data['wikipedia_url'],
                    index_name=index_name,
                    processed_at=datetime.now(),
                    key_people_count=0,
                    processing_success=True
                )
                companies.append(company)

            return companies

        except Exception as e:
            logger.error(f"Failed to extract companies from {index_name}: {e}")
            return []

    def _extract_people_from_companies_normalized(
        self,
        companies: List[WikipediaCompany],
        workers: int = 1
    ) -> List[WikipediaKeyPerson]:
        """
        Extract people from companies using normalized processing.

        Uses existing scraper logic but with enhanced error handling.
        """
        if workers > 1:
            return self._extract_people_parallel(companies, workers)
        else:
            return self._extract_people_sequential(companies)

    def _extract_people_parallel(
        self, companies: List[WikipediaCompany], workers: int
    ) -> List[WikipediaKeyPerson]:
        """Extract people using parallel processing."""
        all_people = []

        with ThreadPoolExecutor(max_workers=workers) as executor:
            # Submit all extraction tasks
            future_to_company = {
                executor.submit(self._extract_single_company_people, company): company
                for company in companies
            }

            # Collect results as they complete
            for future in future_to_company:
                company = future_to_company[future]
                try:
                    people = future.result()
                    all_people.extend(people)
                    logger.debug(f"Extracted {len(people)} people from {company.ticker}")
                except Exception as e:
                    logger.error(f"Failed to extract from {company.ticker}: {e}")

        return all_people

    def _extract_people_sequential(
        self, companies: List[WikipediaCompany]
    ) -> List[WikipediaKeyPerson]:
        """Extract people using sequential processing."""
        all_people = []

        for company in companies:
            try:
                people = self._extract_single_company_people(company)
                all_people.extend(people)
                logger.debug(f"Extracted {len(people)} people from {company.ticker}")
            except Exception as e:
                logger.error(f"Failed to extract from {company.ticker}: {e}")

        return all_people

    def _extract_single_company_people(self, company: WikipediaCompany) -> List[WikipediaKeyPerson]:
        """Extract people from a single company."""
        # Use existing scraper logic
        scraper = WikipediaKeyPeopleScraper(self.config.scraper)

        # Convert WikipediaCompany to the format expected by scraper
        company_dict = {
            'ticker': company.ticker,
            'company_name': company.company_name,
            'wikipedia_url': company.wikipedia_url,
            'index_name': company.index_name
        }

        try:
            people = scraper.people_extractor.extract_key_people(company_dict)
            return people
        except Exception as e:
            logger.warning(f"Failed to extract people from {company.ticker}: {e}")
            return []
