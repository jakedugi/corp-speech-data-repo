# Contributing to Corpus API

Thank you for your interest in contributing to corpus-api! This document provides guidelines and information for contributors.

## Development Setup

### Prerequisites
- Python 3.8+
- uv package manager
- Git

### Installation
```bash
# Clone the repository
git clone https://github.com/your-org/corpus-api.git
cd corpus-api

# Install dependencies
uv sync --dev

# Activate virtual environment
source .venv/bin/activate
```

### Development Workflow
```bash
# Create a feature branch
git checkout -b feature/your-feature-name

# Make your changes
# Add tests
# Run tests
uv run pytest

# Format code
uv run black .
uv run isort .

# Type check
uv run mypy .

# Commit your changes
git commit -m "Add your feature description"
```

## Code Style

This project uses:
- **Black** for code formatting
- **isort** for import sorting
- **mypy** for type checking
- **flake8** for linting

### Pre-commit Hooks
```bash
# Install pre-commit hooks
uv run pre-commit install

# Run manually
uv run pre-commit run --all-files
```

## Testing

### Running Tests
```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=corpus_api

# Run specific test file
uv run pytest tests/test_courtlistener_client.py

# Run tests matching pattern
uv run pytest -k "courtlistener"
```

### Writing Tests
- Use `pytest` as the testing framework
- Place tests in `tests/` directory
- Use descriptive test names
- Include fixtures for common test data
- Mock external API calls

```python
import pytest
from corpus_api.adapters.courtlistener.courtlistener_client import CourtListenerClient

class TestCourtListenerClient:
    def test_search_opinions_basic(self, mock_api_response):
        client = CourtListenerClient(api_key="test-key")
        results = client.search_opinions({"courts": ["scotus"]})
        assert len(results) > 0
        assert "doc_id" in results[0]
```

## Adding New Data Sources

### 1. Create Adapter
```python
# corpus_api/adapters/newsource/newsource_client.py
from ..base_api_client import BaseAPIClient

class NewSourceClient(BaseAPIClient):
    def __init__(self, api_key=None):
        super().__init__(base_url="https://api.newsource.com")
        self.api_key = api_key

    def fetch_documents(self, query):
        # Implementation
        pass
```

### 2. Add CLI Command
```python
# corpus_api/cli/fetch.py
@app.command()
def newsource(...):
    # CLI implementation
    pass
```

### 3. Add Configuration
```yaml
# configs/sources/newsource.yaml
api:
  base_url: "https://api.newsource.com"
  timeout: 30
```

### 4. Add Tests
```python
# tests/test_newsource_client.py
class TestNewSourceClient:
    def test_fetch_documents(self):
        # Tests
        pass
```

## Documentation

### Code Documentation
- Use docstrings for all public functions/classes
- Follow Google docstring format
- Include type hints

```python
def search_opinions(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Search for court opinions based on query parameters.

    Args:
        query: Dictionary containing search parameters

    Returns:
        List of opinion documents

    Raises:
        APIError: If the API request fails
    """
```

### API Documentation
- Update README.md for new features
- Add examples to docstrings
- Update configuration examples

## Pull Request Process

1. **Fork** the repository
2. **Create** a feature branch
3. **Make** your changes
4. **Add** tests
5. **Ensure** all tests pass
6. **Update** documentation
7. **Submit** a pull request

### PR Checklist
- [ ] Tests added/updated
- [ ] Documentation updated
- [ ] Code formatted with Black
- [ ] Type hints added
- [ ] No linting errors
- [ ] CI passes

## Reporting Issues

### Bug Reports
Please include:
- Python version
- Operating system
- Steps to reproduce
- Expected vs actual behavior
- Error messages/logs

### Feature Requests
Please include:
- Use case description
- Proposed implementation
- Benefits and impact

## Code of Conduct

This project follows a code of conduct to ensure a welcoming environment for all contributors.

## License

By contributing to this project, you agree that your contributions will be licensed under the same license as the project.
