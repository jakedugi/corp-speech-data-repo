"""
Tests for Data Writer

Tests the writer module functionality for generating output files and manifests.
"""

import pytest
import json
import tempfile
from pathlib import Path
from datetime import datetime
from unittest.mock import Mock
import pandas as pd

from corpus_hydrator.adapters.index_constituents.writer import (
    to_dataframe,
    write_csv,
    write_parquet,
    write_bundle,
    generate_manifest,
    SCHEMA_VERSION
)
from corpus_types.schemas.models import IndexConstituent


class TestToDataFrame:
    """Test DataFrame conversion."""

    def test_to_dataframe_single_constituent(self):
        """Test converting single constituent to DataFrame."""
        constituent = IndexConstituent(
            symbol='AAPL',
            company_name='Apple Inc.',
            index_name='S&P 500',
            sector='Technology',
            industry='Consumer Electronics',
            date_added='2023-12-15',
            source_url='https://example.com',
            extracted_at=datetime.now()
        )

        df = to_dataframe([constituent])

        assert len(df) == 1
        assert df.iloc[0]['symbol'] == 'AAPL'
        assert df.iloc[0]['company_name'] == 'Apple Inc.'
        assert df.iloc[0]['index_name'] == 'S&P 500'
        assert df.iloc[0]['sector'] == 'Technology'
        assert df.iloc[0]['industry'] == 'Consumer Electronics'

    def test_to_dataframe_multiple_constituents(self):
        """Test converting multiple constituents to DataFrame."""
        constituents = [
            IndexConstituent(
                symbol='AAPL',
                company_name='Apple Inc.',
                index_name='S&P 500',
                source_url='https://example.com'
            ),
            IndexConstituent(
                symbol='MSFT',
                company_name='Microsoft Corp.',
                index_name='S&P 500',
                source_url='https://example.com'
            )
        ]

        df = to_dataframe(constituents)

        assert len(df) == 2
        assert df.iloc[0]['symbol'] == 'AAPL'
        assert df.iloc[1]['symbol'] == 'MSFT'

    def test_to_dataframe_empty_list(self):
        """Test converting empty constituent list."""
        df = to_dataframe([])
        assert len(df) == 0


class TestWriteCSV:
    """Test CSV file writing."""

    def test_write_csv_success(self, tmp_path):
        """Test successful CSV writing."""
        df = pd.DataFrame({
            'symbol': ['AAPL', 'MSFT'],
            'company_name': ['Apple Inc.', 'Microsoft Corp.'],
            'index_name': ['S&P 500', 'S&P 500']
        })

        csv_path = tmp_path / "test.csv"
        hash_value = write_csv(df, csv_path)

        assert csv_path.exists()
        assert isinstance(hash_value, str)
        assert len(hash_value) == 64  # SHA256 length

        # Verify content
        df_read = pd.read_csv(csv_path)
        assert len(df_read) == 2
        assert df_read.iloc[0]['symbol'] == 'AAPL'


class TestWriteParquet:
    """Test Parquet file writing."""

    def test_write_parquet_success(self, tmp_path):
        """Test successful Parquet writing."""
        df = pd.DataFrame({
            'symbol': ['AAPL', 'MSFT'],
            'company_name': ['Apple Inc.', 'Microsoft Corp.'],
            'index_name': ['S&P 500', 'S&P 500']
        })

        parquet_path = tmp_path / "test.parquet"
        hash_value = write_parquet(df, parquet_path)

        assert parquet_path.exists()
        assert isinstance(hash_value, str)
        assert len(hash_value) == 64  # SHA256 length

        # Verify content
        df_read = pd.read_parquet(parquet_path)
        assert len(df_read) == 2
        assert df_read.iloc[0]['symbol'] == 'AAPL'


class TestGenerateManifest:
    """Test manifest generation."""

    def test_generate_manifest_complete(self):
        """Test generating complete manifest."""
        manifest = generate_manifest(
            index_name='S&P 500',
            row_count=503,
            extracted_at='2025-09-11T12:41:15.063277',
            source_url='https://en.wikipedia.org/wiki/List_of_S%26P_500_companies',
            csv_hash='abc123',
            parquet_hash='def456'
        )

        assert manifest['index_name'] == 'S&P 500'
        assert manifest['rows'] == 503
        assert manifest['schema_version'] == SCHEMA_VERSION
        assert manifest['extracted_at'] == '2025-09-11T12:41:15.063277'
        assert manifest['source_url'] == 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        assert manifest['sha256_csv'] == 'abc123'
        assert manifest['sha256_parquet'] == 'def456'
        assert manifest['format'] == 'index_constituents'
        assert manifest['description'] == 'Constituents for S&P 500'

    def test_generate_manifest_minimal(self):
        """Test generating manifest with minimal data."""
        manifest = generate_manifest(
            index_name='Test Index',
            row_count=10,
            extracted_at='2025-01-01T00:00:00',
            source_url=None,
            csv_hash='hash1',
            parquet_hash='hash2'
        )

        assert manifest['index_name'] == 'Test Index'
        assert manifest['rows'] == 10
        assert manifest['source_url'] is None


class TestWriteBundle:
    """Test complete bundle writing."""

    def test_write_bundle_csv_only(self, tmp_path):
        """Test writing bundle with CSV only."""
        constituents = [
            IndexConstituent(
                symbol='AAPL',
                company_name='Apple Inc.',
                index_name='S&P 500',
                source_url='https://example.com',
                extracted_at=datetime.now()
            )
        ]

        try:
            manifest = write_bundle(constituents, tmp_path, formats=['csv'])
            print(f"Manifest returned: {manifest is not None}")
            print(f"Manifest keys: {list(manifest.keys()) if manifest else 'None'}")
        except Exception as e:
            print(f"Exception during write_bundle: {e}")
            import traceback
            traceback.print_exc()
            raise

        # Check files were created
        csv_file = tmp_path / "sp500_constituents.csv"
        manifest_file = tmp_path / "sp500_manifest.json"

        print(f"CSV file exists: {csv_file.exists()}")
        print(f"Manifest file exists: {manifest_file.exists()}")

        # List all files in the directory
        print("Files in directory:")
        for file in tmp_path.iterdir():
            print(f"  {file}")

        assert csv_file.exists()
        assert manifest_file.exists()

        # Check manifest content
        assert manifest['index_name'] == 'S&P 500'
        assert manifest['rows'] == 1
        assert 'sha256_csv' in manifest
        assert manifest['sha256_csv'] is not None  # CSV was requested
        assert 'sha256_parquet' in manifest
        assert manifest['sha256_parquet'] is None  # Parquet was not requested

        # Verify CSV content
        df = pd.read_csv(csv_file)
        assert len(df) == 1
        assert df.iloc[0]['symbol'] == 'AAPL'

    def test_write_bundle_parquet_only(self, tmp_path):
        """Test writing bundle with Parquet only."""
        constituents = [
            IndexConstituent(
                symbol='AAPL',
                company_name='Apple Inc.',
                index_name='S&P 500',
                source_url='https://example.com',
                extracted_at=datetime.now()
            )
        ]

        manifest = write_bundle(constituents, tmp_path, formats=['parquet'])

        # Check files were created
        parquet_file = tmp_path / "sp500_constituents.parquet"
        manifest_file = tmp_path / "sp500_manifest.json"

        assert parquet_file.exists()
        assert manifest_file.exists()

        # Check manifest content
        assert manifest['index_name'] == 'S&P 500'
        assert manifest['rows'] == 1
        assert 'sha256_parquet' in manifest
        assert manifest['sha256_parquet'] is not None  # Parquet was requested
        assert 'sha256_csv' in manifest
        assert manifest['sha256_csv'] is None  # CSV was not requested

        # Verify Parquet content
        df = pd.read_parquet(parquet_file)
        assert len(df) == 1
        assert df.iloc[0]['symbol'] == 'AAPL'

    def test_write_bundle_both_formats(self, tmp_path):
        """Test writing bundle with both CSV and Parquet."""
        constituents = [
            IndexConstituent(
                symbol='AAPL',
                company_name='Apple Inc.',
                index_name='S&P 500',
                source_url='https://example.com',
                extracted_at=datetime.now()
            ),
            IndexConstituent(
                symbol='MSFT',
                company_name='Microsoft Corp.',
                index_name='S&P 500',
                source_url='https://example.com',
                extracted_at=datetime.now()
            )
        ]

        manifest = write_bundle(constituents, tmp_path, formats=['csv', 'parquet'])

        # Check all files were created
        csv_file = tmp_path / "sp500_constituents.csv"
        parquet_file = tmp_path / "sp500_constituents.parquet"
        manifest_file = tmp_path / "sp500_manifest.json"

        assert csv_file.exists()
        assert parquet_file.exists()
        assert manifest_file.exists()

        # Check manifest content
        assert manifest['index_name'] == 'S&P 500'
        assert manifest['rows'] == 2
        assert 'sha256_csv' in manifest
        assert 'sha256_parquet' in manifest

        # Verify both file contents
        df_csv = pd.read_csv(csv_file)
        df_parquet = pd.read_parquet(parquet_file)

        assert len(df_csv) == 2
        assert len(df_parquet) == 2
        assert df_csv.iloc[0]['symbol'] == 'AAPL'
        assert df_parquet.iloc[0]['symbol'] == 'AAPL'

    def test_write_bundle_empty_constituents(self, tmp_path):
        """Test writing bundle with empty constituents list."""
        manifest = write_bundle([], tmp_path)

        assert manifest == {}
