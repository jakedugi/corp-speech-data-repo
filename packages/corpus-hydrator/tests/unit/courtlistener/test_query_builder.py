"""Tests for CourtListener query builder."""

import pytest
from pathlib import Path
from corpus_hydrator.adapters.courtlistener.parsers.query_builder import (
    build_queries,
    STATUTE_QUERIES,
    QueryBuilder,
)


class TestQueryBuilder:
    """Test the QueryBuilder class."""
    
    def test_init(self):
        """Test QueryBuilder initialization."""
        qb = QueryBuilder()
        assert qb is not None
    
    def test_build_statute_query_no_companies(self):
        """Test building query without company filtering."""
        qb = QueryBuilder()
        queries = qb.build_statute_query("FTC Section 5")
        
        assert isinstance(queries, list)
        assert len(queries) == 1
        assert "FTC Act" in queries[0]
        assert "Section 5" in queries[0]
    
    def test_build_statute_query_with_companies(self, tmp_path):
        """Test building query with company filtering."""
        qb = QueryBuilder()
        
        # Create test CSV
        csv_file = tmp_path / "companies.csv"
        csv_file.write_text("official_name\nApple\nMicrosoft\nGoogle\n")
        
        queries = qb.build_statute_query("FTC Section 5", csv_file, chunk_size=2)
        
        assert isinstance(queries, list)
        assert len(queries) == 2  # 3 companies with chunk_size=2 = 2 queries
        
        # Check first query contains first 2 companies
        assert "Apple" in queries[0]
        assert "Microsoft" in queries[0]
        assert "Google" not in queries[0]
        
        # Check second query contains remaining company
        assert "Google" in queries[1]
    
    def test_build_statute_query_test_mode(self, tmp_path):
        """Test building query in test mode (chunk_size=1)."""
        qb = QueryBuilder()
        
        # Create test CSV
        csv_file = tmp_path / "companies.csv"
        csv_file.write_text("official_name\nApple\nMicrosoft\nGoogle\n")
        
        queries = qb.build_statute_query("FTC Section 5", csv_file, chunk_size=1)
        
        assert isinstance(queries, list)
        assert len(queries) == 1  # Test mode returns only 1 query
        assert "Apple" in queries[0]  # Should use first company
        assert "Microsoft" not in queries[0]  # Should not include others


def test_build_queries_no_companies():
    """Test build_queries function without companies."""
    for statute in STATUTE_QUERIES:
        qlist = build_queries(statute, company_file=None, chunk_size=50)
        assert isinstance(qlist, list)
        assert len(qlist) == 1
        assert STATUTE_QUERIES[statute].strip() in qlist[0]


def test_build_queries_with_companies(tmp_path):
    """Test build_queries function with companies."""
    # Create test CSV
    csv_file = tmp_path / "companies.csv"
    csv_file.write_text("official_name\nApple\nMicrosoft\nGoogle\nAmazon\n")
    
    queries = build_queries("FTC Section 5", company_file=csv_file, chunk_size=2)
    
    assert len(queries) == 2  # 4 companies with chunk_size=2 = 2 queries
    assert "Apple" in queries[0] and "Microsoft" in queries[0]
    assert "Google" in queries[1] and "Amazon" in queries[1]


def test_statute_queries_available():
    """Test that STATUTE_QUERIES contains expected statutes."""
    assert "FTC Section 5 (9th Cir.)" in STATUTE_QUERIES
    assert "Lanham Act § 43(a)" in STATUTE_QUERIES
    assert "SEC Rule 10b-5" in STATUTE_QUERIES
    
    # Ensure all queries are non-empty strings
    for statute, query in STATUTE_QUERIES.items():
        assert isinstance(query, str)
        assert len(query.strip()) > 0
        assert statute in query or "FTC" in query or "Lanham" in query or "SEC" in query


def test_invalid_statute_raises_error():
    """Test that invalid statute raises KeyError."""
    with pytest.raises(KeyError):
        build_queries("Invalid Statute", company_file=None)
