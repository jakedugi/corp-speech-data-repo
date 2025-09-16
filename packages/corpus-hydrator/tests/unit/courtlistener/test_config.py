"""Tests for CourtListener configuration."""

import pytest
from pathlib import Path
from corpus_hydrator.adapters.courtlistener.config import get_default_config, load_config
from corpus_types.schemas.models import CourtListenerConfig


def test_get_default_config():
    """Test that get_default_config returns a valid CourtListenerConfig."""
    config = get_default_config()
    
    assert isinstance(config, CourtListenerConfig)
    assert config.api_token is None  # Should be None by default
    assert isinstance(config.output_dir, Path)
    assert config.default_pages == 1
    assert config.default_page_size == 50
    assert config.api_mode == "standard"


def test_load_config_with_env_vars(monkeypatch):
    """Test loading config with environment variables."""
    monkeypatch.setenv("COURTLISTENER_API_TOKEN", "test_token")
    monkeypatch.setenv("COURTLISTENER_OUTPUT_DIR", "/tmp/test")
    monkeypatch.setenv("COURTLISTENER_DEFAULT_PAGES", "5")
    monkeypatch.setenv("COURTLISTENER_DEFAULT_PAGE_SIZE", "25")
    monkeypatch.setenv("COURTLISTENER_API_MODE", "recap")
    
    config = load_config()
    
    assert config.api_token == "test_token"
    assert config.output_dir == Path("/tmp/test")
    assert config.default_pages == 5
    assert config.default_page_size == 25
    assert config.api_mode == "recap"


def test_courtlistener_config_creation():
    """Test creating CourtListenerConfig directly."""
    config = CourtListenerConfig(
        api_token="test_token",
        output_dir=Path("/tmp/test"),
        default_pages=10,
        default_page_size=100,
        api_mode="recap",
        default_chunk_size=25,
    )
    
    assert config.api_token == "test_token"
    assert config.output_dir == Path("/tmp/test")
    assert config.default_pages == 10
    assert config.default_page_size == 100
    assert config.api_mode == "recap"
    assert config.default_chunk_size == 25
