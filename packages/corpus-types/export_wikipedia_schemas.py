#!/usr/bin/env python3
"""
Export Wikipedia Key People Schemas

This script exports JSON schemas for all Wikipedia Key People models
to support external validation and documentation.
"""

import json
from pathlib import Path
from corpus_types.schemas import (
    WikipediaKeyPerson,
    WikipediaCompany,
    WikipediaExtractionResult,
    WikipediaScrapingConfig,
    WikipediaContentConfig,
    WikipediaIndexConfig,
    WikipediaKeyPeopleConfig,
    NormalizedCompany,
    NormalizedPerson,
    NormalizedRole,
    NormalizedAppointment,
    DatasetManifest,
)
from corpus_types.utils.export_schema import export_model_schema


def main():
    """Export all Wikipedia key people schemas."""
    # Create output directory
    output_dir = Path("schemas")
    output_dir.mkdir(exist_ok=True)

    # Models to export
    models = [
        (WikipediaKeyPerson, "wikipedia_key_person"),
        (WikipediaCompany, "wikipedia_company"),
        (WikipediaExtractionResult, "wikipedia_extraction_result"),
        (WikipediaScrapingConfig, "wikipedia_scraping_config"),
        (WikipediaContentConfig, "wikipedia_content_config"),
        (WikipediaIndexConfig, "wikipedia_index_config"),
        (WikipediaKeyPeopleConfig, "wikipedia_key_people_config"),
        (NormalizedCompany, "normalized_company"),
        (NormalizedPerson, "normalized_person"),
        (NormalizedRole, "normalized_role"),
        (NormalizedAppointment, "normalized_appointment"),
        (DatasetManifest, "dataset_manifest"),
    ]

    print("📋 Exporting Wikipedia Key People schemas...")

    for model_cls, filename_prefix in models:
        print(f"  Exporting {model_cls.__name__}...")

        # Export schema
        schema = export_model_schema(model_cls, version="2.0.0")

        # Write to file
        output_file = output_dir / f"{filename_prefix}.schema.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(schema, f, indent=2, ensure_ascii=False)

        print(f"    ✅ {output_file}")

    print("\n🎉 All schemas exported successfully!")
    print(f"📁 Check the '{output_dir}' directory for JSON schema files")


if __name__ == "__main__":
    main()
