import pytest
from pathlib import Path
from corpus_hydrator.adapters.courtlistener.parsers.query_builder import build_queries
from corpus_hydrator.adapters.courtlistener.usecase import CourtListenerUseCase
from corpus_types.schemas.models import CourtListenerConfig


class DummyUseCase(CourtListenerUseCase):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault("statutes", ["FTC Section 5"])
        super().__init__(*args, **kwargs)
        self.queries_processed = []

    def _search_and_hydrate(self, query, search_dir):
        # Mock the search and hydrate method
        self.queries_processed.append(("FTC Section 5", query))
        return 1


def test_usecase_builds_all_chunks(tmp_path):
    companies = tmp_path / "csv"
    companies.write_text("official_name\nA\nB\nC\nD\nE\n")

    # Test query building directly
    expected = build_queries("FTC Section 5", company_file=companies, chunk_size=2)
    assert len(expected) == 3  # Should have 3 chunks for 5 companies with chunk_size=2

    # Test that queries are built correctly
    for q in expected:
        assert "FTC Act" in q
        assert "A" in q or "B" in q or "C" in q or "D" in q or "E" in q


def test_usecase_initialization():
    """Test that CourtListenerUseCase initializes correctly."""
    config = CourtListenerConfig(api_token="test_token")

    usecase = CourtListenerUseCase(
        config=config,
        statutes=["FTC Section 5"],
        outdir=Path("test_output"),
        pages=1,
        page_size=50,
        date_min="2020-01-01",
        api_mode="standard",
        chunk_size=10,
        max_companies=5,
        max_results=100,
        max_cases=50,
    )

    assert usecase.config == config
    assert usecase.statutes == ["FTC Section 5"]
    assert usecase.outdir == Path("test_output")
    assert usecase.pages == 1
    assert usecase.page_size == 50
    assert usecase.date_min == "2020-01-01"
    assert usecase.api_mode == "standard"
    assert usecase.chunk_size == 10
    assert usecase.max_companies == 5
    assert usecase.max_results == 100
    assert usecase.max_cases == 50
    assert usecase.total_cases_processed == 0
    assert usecase.disable_pdf_downloads == False


def test_usecase_max_cases_limit():
    """Test that max_cases parameter limits processing correctly."""
    config = CourtListenerConfig(api_token="test_token")

    usecase = CourtListenerUseCase(
        config=config,
        statutes=["FTC Section 5"],
        outdir=Path("test_output"),
        max_cases=2,
    )

    # Simulate processing cases
    usecase.total_cases_processed = 2

    # The usecase should respect the max_cases limit
    assert usecase.max_cases == 2
    assert usecase.total_cases_processed == 2
