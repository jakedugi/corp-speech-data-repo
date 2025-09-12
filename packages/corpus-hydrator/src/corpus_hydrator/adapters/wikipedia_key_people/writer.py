"""
Data Writer for Wikipedia Key People Scraper

This module handles writing extracted data to various output formats.
"""

import logging
import hashlib
from pathlib import Path
from typing import List, Dict, Any, Optional
import pandas as pd
import json
from datetime import datetime
import jsonschema
from collections import OrderedDict

from corpus_types.schemas.wikipedia_key_people import (
    WikipediaExtractionResult,
    WikipediaKeyPerson,
    WikipediaCompany,
    NormalizedCompany,
    NormalizedPerson,
    NormalizedRole,
    NormalizedAppointment,
    DatasetManifest
)

logger = logging.getLogger(__name__)


class WikipediaKeyPeopleWriter:
    """Handles writing extracted Wikipedia key people data to various formats."""

    def __init__(self, output_dir: str = "data"):
        """Initialize the writer."""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def write_results(self, result: WikipediaExtractionResult, index_name: str):
        """
        Write extraction results to output files.

        Args:
            result: Extraction result to write
            index_name: Name of the index processed
        """
        if not result.success:
            logger.warning(f"Skipping write for failed extraction: {result.index_name}")
            return

        try:
            # Write key people data
            if result.key_people:
                self._write_key_people_csv(result, index_name)
                self._write_key_people_json(result, index_name)

            # Write company summary
            if result.companies:
                self._write_companies_csv(result, index_name)

            # Write statistics
            self._write_statistics(result, index_name)

            logger.info(f"Results written to {self.output_dir}")

        except Exception as e:
            logger.error(f"Failed to write results for {index_name}: {e}")

    def write_intermediate_results(self, result: WikipediaExtractionResult, index_name: str):
        """
        Write intermediate results for debugging/analysis.

        Args:
            result: Extraction result to write
            index_name: Name of the index processed
        """
        try:
            # Write raw extraction data
            intermediate_dir = self.output_dir / "intermediate" / index_name
            intermediate_dir.mkdir(parents=True, exist_ok=True)

            # Write individual company results
            for company in result.companies:
                if company.key_people_count > 0:
                    company_file = intermediate_dir / f"{company.ticker}_people.json"
                    company_data = {
                        "ticker": company.ticker,
                        "company_name": company.company_name,
                        "wikipedia_url": company.wikipedia_url,
                        "key_people_count": company.key_people_count,
                        "processing_success": company.processing_success,
                        "processed_at": company.processed_at.isoformat() if company.processed_at else None
                    }

                    with open(company_file, 'w', encoding='utf-8') as f:
                        json.dump(company_data, f, indent=2, ensure_ascii=False)

            logger.info(f"Intermediate results written to {intermediate_dir}")

        except Exception as e:
            logger.error(f"Failed to write intermediate results for {index_name}: {e}")

    def _write_key_people_csv(self, result: WikipediaExtractionResult, index_name: str):
        """Write key people data to CSV format."""
        people_data = []
        for person in result.key_people:
            people_data.append({
                'ticker': person.ticker,
                'company_name': person.company_name,
                'raw_name': person.raw_name,
                'clean_name': person.clean_name,
                'clean_title': person.clean_title,
                'source': person.source,
                'wikipedia_url': person.wikipedia_url,
                'extraction_method': person.extraction_method,
                'scraped_at': person.scraped_at.isoformat() if person.scraped_at else None,
                'parse_success': person.parse_success,
                'confidence_score': person.confidence_score
            })

        if people_data:
            df = pd.DataFrame(people_data)
            output_file = self.output_dir / f"{index_name}_key_people.csv"
            df.to_csv(output_file, index=False, encoding='utf-8')
            logger.info(f"Key people CSV written: {output_file} ({len(people_data)} records)")

    def _write_key_people_json(self, result: WikipediaExtractionResult, index_name: str):
        """Write key people data to JSON format."""
        people_data = []
        for person in result.key_people:
            people_data.append({
                'ticker': person.ticker,
                'company_name': person.company_name,
                'raw_name': person.raw_name,
                'clean_name': person.clean_name,
                'clean_title': person.clean_title,
                'source': person.source,
                'wikipedia_url': person.wikipedia_url,
                'extraction_method': person.extraction_method,
                'scraped_at': person.scraped_at.isoformat() if person.scraped_at else None,
                'parse_success': person.parse_success,
                'confidence_score': person.confidence_score
            })

        if people_data:
            output_file = self.output_dir / f"{index_name}_key_people.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(people_data, f, indent=2, ensure_ascii=False)
            logger.info(f"Key people JSON written: {output_file} ({len(people_data)} records)")

    def _write_companies_csv(self, result: WikipediaExtractionResult, index_name: str):
        """Write company summary data to CSV format."""
        company_data = []
        for company in result.companies:
            company_data.append({
                'ticker': company.ticker,
                'company_name': company.company_name,
                'wikipedia_url': company.wikipedia_url,
                'index_name': company.index_name,
                'key_people_count': company.key_people_count,
                'processing_success': company.processing_success,
                'processed_at': company.processed_at.isoformat() if company.processed_at else None
            })

        if company_data:
            df = pd.DataFrame(company_data)
            output_file = self.output_dir / f"{index_name}_companies.csv"
            df.to_csv(output_file, index=False, encoding='utf-8')
            logger.info(f"Companies CSV written: {output_file} ({len(company_data)} records)")

    def _write_statistics(self, result: WikipediaExtractionResult, index_name: str):
        """Write extraction statistics to a text file."""
        stats_file = self.output_dir / f"{index_name}_stats.txt"

        with open(stats_file, 'w', encoding='utf-8') as f:
            f.write(f"=== {index_name.upper()} Key People Extraction Statistics ===\n\n")
            f.write(f"Operation ID: {result.operation_id}\n")
            f.write(f"Started: {result.started_at}\n")
            f.write(f"Completed: {result.completed_at}\n")

            if result.completed_at and result.started_at:
                duration = result.completed_at - result.started_at
                f.write(f"Duration: {duration.total_seconds():.1f} seconds\n")

            f.write("\n")
            f.write(f"Companies processed: {result.companies_processed}\n")
            f.write(f"Companies successful: {result.companies_successful}\n")
            f.write(f"Success rate: {result.success_rate:.1%}\n")
            f.write(f"Total key people: {result.total_key_people}\n")

            if result.companies_successful > 0:
                avg_people = result.total_key_people / result.companies_successful
                f.write(f"Average key people per company: {avg_people:.1f}\n")

            f.write(f"Status: {'SUCCESS' if result.success else 'FAILED'}\n")

            if result.error_message:
                f.write(f"Error: {result.error_message}\n")

            # Add extraction method breakdown
            if result.key_people:
                method_counts = {}
                for person in result.key_people:
                    method = person.extraction_method or "unknown"
                    method_counts[method] = method_counts.get(method, 0) + 1

                f.write("\nExtraction methods:\n")
                for method, count in sorted(method_counts.items()):
                    f.write(f"  {method}: {count} people ({count/len(result.key_people):.1%})\n")

        logger.info(f"Statistics written: {stats_file}")

    def write_comparison_report(self, results: Dict[str, WikipediaExtractionResult], output_file: str = None):
        """
        Write a comparison report for multiple index results.

        Args:
            results: Dictionary of extraction results
            output_file: Optional custom output file path
        """
        if not output_file:
            output_file = self.output_dir / "comparison_report.txt"

        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("=== Wikipedia Key People Extraction Comparison Report ===\n\n")
            f.write(f"Generated: {datetime.now().isoformat()}\n\n")

            # Summary table
            f.write("Index Summary:\n")
            f.write("-" * 80 + "\n")
            f.write("<15")
            f.write("-" * 80 + "\n")

            total_companies = 0
            total_successful = 0
            total_people = 0

            for index_name, result in results.items():
                success_rate = result.success_rate * 100
                avg_people = 0
                if result.companies_successful > 0:
                    avg_people = result.total_key_people / result.companies_successful

                f.write("<15")

                total_companies += result.companies_processed
                total_successful += result.companies_successful
                total_people += result.total_key_people

            f.write("-" * 80 + "\n")
            overall_success = (total_successful / total_companies * 100) if total_companies > 0 else 0
            overall_avg = (total_people / total_successful) if total_successful > 0 else 0

            f.write("<15")

            # Detailed breakdown
            f.write("\n\nDetailed Results:\n")
            for index_name, result in results.items():
                f.write(f"\n{index_name.upper()}:\n")
                f.write(f"  Companies: {result.companies_successful}/{result.companies_processed}\n")
                f.write(f"  Key People: {result.total_key_people}\n")
                f.write(f"  Success Rate: {result.success_rate:.1%}\n")

                if result.companies_successful > 0:
                    avg = result.total_key_people / result.companies_successful
                    f.write(f"  Average per Company: {avg:.1f}\n")

                if not result.success and result.error_message:
                    f.write(f"  Error: {result.error_message}\n")

        logger.info(f"Comparison report written: {output_file}")

    # --------------------------------------------------------------------------- #
    # Production Normalized Table Structure (v2.0)                              #
    # --------------------------------------------------------------------------- #

    def write_normalized_tables(
        self,
        companies: List[NormalizedCompany],
        people: List[NormalizedPerson],
        roles: List[NormalizedRole],
        appointments: List[NormalizedAppointment],
        dataset_name: str = "wikipedia_key_people",
        provider_order: List[str] = None
    ) -> DatasetManifest:
        """
        Write data in normalized table structure for production use.

        Returns a manifest with integrity hashes and metadata.
        """
        if provider_order is None:
            provider_order = ["wikipedia"]

        manifest = DatasetManifest(
            schema_version="2.0.0",
            dataset_name=dataset_name,
            source="wikipedia",
            provider_order=provider_order,
            companies_count=len(companies),
            people_count=len(people),
            roles_count=len(roles),
            appointments_count=len(appointments)
        )

        # Write companies table
        if companies:
            companies_df = pd.DataFrame([c.dict() for c in companies])
            companies_df = companies_df.sort_values('company_id')
            companies_file = self.output_dir / f"{dataset_name}_companies.csv"
            companies_df.to_csv(companies_file, index=False)
            manifest.companies_sha256 = self._calculate_sha256(companies_file)

        # Write people table
        if people:
            people_df = pd.DataFrame([p.dict() for p in people])
            people_df = people_df.sort_values('person_id')
            people_file = self.output_dir / f"{dataset_name}_people.csv"
            people_df.to_csv(people_file, index=False)
            manifest.people_sha256 = self._calculate_sha256(people_file)

        # Write roles table
        if roles:
            roles_df = pd.DataFrame([r.dict() for r in roles])
            roles_df = roles_df.sort_values('role_id')
            roles_file = self.output_dir / f"{dataset_name}_roles.csv"
            roles_df.to_csv(roles_file, index=False)
            manifest.roles_sha256 = self._calculate_sha256(roles_file)

        # Write appointments table
        if appointments:
            appointments_df = pd.DataFrame([a.dict() for a in appointments])
            appointments_df = appointments_df.sort_values(['company_id', 'role_id', 'person_id'])
            appointments_file = self.output_dir / f"{dataset_name}_appointments.csv"
            appointments_df.to_csv(appointments_file, index=False)
            manifest.appointments_sha256 = self._calculate_sha256(appointments_file)

        # Write manifest
        manifest_file = self.output_dir / f"{dataset_name}_manifest.json"
        with open(manifest_file, 'w') as f:
            json.dump(manifest.dict(), f, indent=2, default=str)

        logger.info(f"Normalized tables written for {dataset_name}:")
        logger.info(f"  Companies: {len(companies)}")
        logger.info(f"  People: {len(people)}")
        logger.info(f"  Roles: {len(roles)}")
        logger.info(f"  Appointments: {len(appointments)}")

        return manifest

    def _calculate_sha256(self, file_path: Path) -> str:
        """Calculate SHA256 hash of a file."""
        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    # --------------------------------------------------------------------------- #
    # Conversion from Legacy to Normalized Format                              #
    # --------------------------------------------------------------------------- #

    def convert_legacy_to_normalized(
        self,
        people: List[WikipediaKeyPerson],
        index_name: str = "unknown"
    ) -> tuple[List[NormalizedCompany], List[NormalizedPerson], List[NormalizedRole], List[NormalizedAppointment]]:
        """
        Convert legacy flat format to normalized table structure.

        This maintains backward compatibility while enabling the new structure.
        """
        companies = {}
        people_dict = {}
        roles = {}
        appointments = []

        for person in people:
            # Create/update company
            company_id = f"{person.ticker}_{index_name}"
            if company_id not in companies:
                companies[company_id] = NormalizedCompany(
                    company_id=company_id,
                    company_name=person.company_name,
                    ticker=person.ticker,
                    wikipedia_url=person.wikipedia_url,
                    index_name=index_name,
                    source_revision_id=None  # TODO: extract from URL or metadata
                )

            # Create/update person
            person_id = f"{person.clean_name}_{person.ticker}".replace(" ", "_").lower()
            if person_id not in people_dict:
                people_dict[person_id] = NormalizedPerson(
                    person_id=person_id,
                    full_name=person.raw_name,
                    normalized_name=person.clean_name
                )

            # Create/update role
            role_canon = self._normalize_role_title(person.clean_title)
            role_id = f"{role_canon}_{person.ticker}".replace(" ", "_").lower()
            if role_id not in roles:
                roles[role_id] = NormalizedRole(
                    role_id=role_id,
                    role_canon=role_canon,
                    role_raw=person.clean_title
                )

            # Create appointment
            appointment = NormalizedAppointment(
                company_id=company_id,
                person_id=person_id,
                role_id=role_id,
                source_url=person.wikipedia_url,
                extraction_strategy="legacy_conversion",
                confidence_score=person.confidence_score
            )
            appointments.append(appointment)

        return (
            list(companies.values()),
            list(people_dict.values()),
            list(roles.values()),
            appointments
        )

    def _normalize_role_title(self, title: str) -> str:
        """Map raw titles to controlled vocabulary."""
        title_lower = title.lower()

        # Map common variations to canonical forms
        role_mappings = {
            'ceo': 'CEO',
            'chief executive officer': 'CEO',
            'cfo': 'CFO',
            'chief financial officer': 'CFO',
            'chairman': 'CHAIR',
            'chair': 'CHAIR',
            'president': 'PRESIDENT',
            'founder': 'FOUNDER',
            'board member': 'BOARD_MEMBER',
            'executive': 'EXECUTIVE',
            'vice president': 'VICE_PRESIDENT',
            'vp': 'VICE_PRESIDENT'
        }

        for key, value in role_mappings.items():
            if key in title_lower:
                return value

        # Default fallback
        return 'EXECUTIVE'

    # --------------------------------------------------------------------------- #
    # Deterministic Outputs and Manifests (v2.0)                               #
    # --------------------------------------------------------------------------- #

    def write_deterministic_csv(
        self,
        data: List[Dict[str, Any]],
        output_path: Path,
        sort_keys: List[str],
        schema: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Write data to CSV with deterministic sorting for reproducible outputs.

        Args:
            data: List of dictionaries to write
            output_path: Path to write CSV file
            sort_keys: Keys to sort by (in order of priority)
            schema: Optional JSON schema for validation

        Returns:
            SHA256 hash of the output file
        """
        if not data:
            logger.warning(f"No data to write to {output_path}")
            return ""

        # Validate against schema if provided
        if schema:
            self._validate_data_against_schema(data, schema)

        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Sort deterministically
        df = df.sort_values(by=sort_keys, na_position='last').reset_index(drop=True)

        # Write to CSV with deterministic formatting
        df.to_csv(
            output_path,
            index=False,
            encoding='utf-8',
            date_format='%Y-%m-%dT%H:%M:%SZ',
            float_format='%.6f'
        )

        # Calculate SHA256 hash
        sha256_hash = self._calculate_file_hash(output_path)

        logger.info(f"Written {len(data)} records to {output_path} (SHA256: {sha256_hash[:8]}...)")
        return sha256_hash

    def write_deterministic_parquet(
        self,
        data: List[Dict[str, Any]],
        output_path: Path,
        sort_keys: List[str],
        schema: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Write data to Parquet with deterministic sorting.

        Args:
            data: List of dictionaries to write
            output_path: Path to write Parquet file
            sort_keys: Keys to sort by
            schema: Optional JSON schema for validation

        Returns:
            SHA256 hash of the output file
        """
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError:
            logger.warning("PyArrow not available, skipping Parquet output")
            return ""

        if not data:
            logger.warning(f"No data to write to {output_path}")
            return ""

        # Validate against schema if provided
        if schema:
            self._validate_data_against_schema(data, schema)

        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Sort deterministically
        df = df.sort_values(by=sort_keys, na_position='last').reset_index(drop=True)

        # Convert to PyArrow table with deterministic schema
        table = pa.Table.from_pandas(df, preserve_index=False)

        # Write to Parquet
        pq.write_table(table, output_path)

        # Calculate SHA256 hash
        sha256_hash = self._calculate_file_hash(output_path)

        logger.info(f"Written {len(data)} records to {output_path} (SHA256: {sha256_hash[:8]}...)")
        return sha256_hash

    def generate_dataset_manifest(
        self,
        dataset_name: str,
        output_files: Dict[str, str],
        metadata: Dict[str, Any],
        schema_version: str = "2.0.0"
    ) -> Dict[str, Any]:
        """
        Generate a comprehensive dataset manifest.

        Args:
            dataset_name: Name of the dataset
            output_files: Dict mapping file types to SHA256 hashes
            metadata: Additional metadata about the extraction
            schema_version: Version of the schema used

        Returns:
            Manifest dictionary
        """
        manifest = OrderedDict([
            ("schema_version", schema_version),
            ("dataset_name", dataset_name),
            ("extraction_timestamp", datetime.utcnow().isoformat()),
            ("row_counts", metadata.get("row_counts", {})),
            ("file_hashes", OrderedDict([
                ("csv", output_files.get("csv", "")),
                ("parquet", output_files.get("parquet", ""))
            ])),
            ("source_metadata", OrderedDict([
                ("provider_order", metadata.get("provider_order", ["wikipedia"])),
                ("extraction_parameters", metadata.get("extraction_parameters", {})),
                ("data_quality_metrics", metadata.get("quality_metrics", {}))
            ])),
            ("governance", OrderedDict([
                ("created_by", "wikipedia-key-people-scraper"),
                ("license", "CC-BY-SA 4.0 (inherited from Wikipedia)"),
                ("attribution_required", True),
                ("contact", "jake@jakedugan.com")
            ]))
        ])

        return manifest

    def write_manifest(self, manifest: Dict[str, Any], output_path: Path):
        """Write manifest to JSON file with pretty formatting."""
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)

        logger.info(f"Manifest written to {output_path}")

    def _validate_data_against_schema(self, data: List[Dict[str, Any]], schema: Dict[str, Any]):
        """Validate data against JSON schema."""
        try:
            for item in data:
                jsonschema.validate(instance=item, schema=schema)
        except jsonschema.ValidationError as e:
            logger.error(f"Schema validation failed: {e}")
            raise

    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate SHA256 hash of a file."""
        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    def write_normalized_tables_with_manifest(
        self,
        companies: List[Dict[str, Any]],
        people: List[Dict[str, Any]],
        roles: List[Dict[str, Any]],
        appointments: List[Dict[str, Any]],
        dataset_name: str = "wikipedia_key_people",
        output_dir: str = "data",
        provider_order: Optional[List[str]] = None,
        include_parquet: bool = True
    ) -> Dict[str, Any]:
        """
        Write normalized tables with deterministic sorting and comprehensive manifest.

        This is the production-ready output method that ensures:
        - Deterministic sorting for reproducible outputs
        - SHA256 hashes for integrity verification
        - Comprehensive manifest with governance metadata
        - Both CSV and Parquet formats
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # File hashes for manifest
        file_hashes = {}

        # Write companies
        if companies:
            companies_file = output_path / f"{dataset_name}_companies.csv"
            file_hashes["companies_csv"] = self.write_deterministic_csv(
                [c.dict() if hasattr(c, 'dict') else c for c in companies],
                companies_file,
                sort_keys=['company_id']
            )

            if include_parquet:
                companies_parquet = output_path / f"{dataset_name}_companies.parquet"
                self.write_deterministic_parquet(
                    [c.dict() if hasattr(c, 'dict') else c for c in companies],
                    companies_parquet,
                    sort_keys=['company_id']
                )

        # Write people
        if people:
            people_file = output_path / f"{dataset_name}_people.csv"
            file_hashes["people_csv"] = self.write_deterministic_csv(
                [p.dict() if hasattr(p, 'dict') else p for p in people],
                people_file,
                sort_keys=['person_id']
            )

            if include_parquet:
                people_parquet = output_path / f"{dataset_name}_people.parquet"
                self.write_deterministic_parquet(
                    [p.dict() if hasattr(p, 'dict') else p for p in people],
                    people_parquet,
                    sort_keys=['person_id']
                )

        # Write roles
        if roles:
            roles_file = output_path / f"{dataset_name}_roles.csv"
            file_hashes["roles_csv"] = self.write_deterministic_csv(
                [r.dict() if hasattr(r, 'dict') else r for r in roles],
                roles_file,
                sort_keys=['role_id']
            )

            if include_parquet:
                roles_parquet = output_path / f"{dataset_name}_roles.parquet"
                self.write_deterministic_parquet(
                    [r.dict() if hasattr(r, 'dict') else r for r in roles],
                    roles_parquet,
                    sort_keys=['role_id']
                )

        # Write appointments
        if appointments:
            appointments_file = output_path / f"{dataset_name}_appointments.csv"
            file_hashes["appointments_csv"] = self.write_deterministic_csv(
                [a.dict() if hasattr(a, 'dict') else a for a in appointments],
                appointments_file,
                sort_keys=['company_id', 'person_id', 'role_id']
            )

            if include_parquet:
                appointments_parquet = output_path / f"{dataset_name}_appointments.parquet"
                self.write_deterministic_parquet(
                    [a.dict() if hasattr(a, 'dict') else a for a in appointments],
                    appointments_parquet,
                    sort_keys=['company_id', 'person_id', 'role_id']
                )

        # Generate manifest
        row_counts = {
            "companies": len(companies) if companies else 0,
            "people": len(people) if people else 0,
            "roles": len(roles) if roles else 0,
            "appointments": len(appointments) if appointments else 0
        }

        metadata = {
            "row_counts": row_counts,
            "provider_order": provider_order or ["wikipedia"],
            "extraction_parameters": {
                "include_parquet": include_parquet,
                "deterministic_sorting": True
            },
            "quality_metrics": {
                "total_entities": sum(row_counts.values()),
                "data_completeness": "high"  # Could be calculated more precisely
            }
        }

        manifest = self.generate_dataset_manifest(
            dataset_name,
            file_hashes,
            metadata
        )

        # Write manifest
        manifest_file = output_path / f"{dataset_name}_manifest.json"
        self.write_manifest(manifest, manifest_file)

        logger.info(f"Dataset {dataset_name} written with {sum(row_counts.values())} total entities")

        return {
            "manifest": manifest,
            "output_files": list((output_path / f"{dataset_name}_*.csv").glob("*")) +
                           list((output_path / f"{dataset_name}_*.parquet").glob("*")) +
                           [manifest_file],
            "row_counts": row_counts,
            "file_hashes": file_hashes
        }
