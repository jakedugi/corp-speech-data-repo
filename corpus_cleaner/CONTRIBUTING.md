# Contributing to Corpus Cleaner

Thank you for your interest in contributing to corpus-cleaner! This document provides guidelines for contributors.

## Development Setup

### Prerequisites
- Python 3.8+
- uv package manager

### Installation
```bash
uv sync --dev
```

## Code Style

This project uses:
- **Black** for code formatting
- **isort** for import sorting
- **mypy** for type checking
- **flake8** for linting

### Pre-commit Hooks
```bash
uv run pre-commit install
uv run pre-commit run --all-files
```

## Testing

### Running Tests
```bash
uv run pytest
```

### Writing Tests
- Test offset mapping preservation
- Include edge cases (empty text, unicode issues)
- Test configuration loading
- Validate golden file comparisons

## Adding New Cleaning Rules

### 1. Add Rule to TextCleaner
```python
def normalize_custom_text(self, text: str) -> str:
    """Custom normalization rule."""
    # Implementation
    return cleaned_text
```

### 2. Preserve Offset Mapping
```python
def normalize_custom_with_offsets(self, text: str) -> Tuple[str, List[Tuple[int, int]]]:
    """Custom rule with offset preservation."""
    # Track changes for offset mapping
    return cleaned_text, offset_map
```

### 3. Update Configuration
```yaml
custom:
  enable_custom_rule: true
  custom_parameter: "value"
```

### 4. Add Tests
```python
def test_custom_normalization():
    cleaner = TextCleaner()
    original = "input text"
    expected = "expected output"
    result, offset_map = cleaner.normalize_custom_with_offsets(original)
    assert result == expected
    # Test offset mapping
```

## Pull Request Process

1. **Test offset preservation** for any text changes
2. **Update configuration schema** for new options
3. **Add comprehensive tests** including edge cases
4. **Update documentation** with examples
5. **Ensure backwards compatibility**

## Offset Mapping Guidelines

When adding new normalization rules:

1. **Track all changes**: Character insertions, deletions, replacements
2. **Maintain alignment**: Ensure spans can be mapped back to original
3. **Test thoroughly**: Use the offset mapping validation utilities
4. **Document behavior**: Explain how offsets are affected

## Performance Considerations

- **Batch processing**: Don't break batch processing capabilities
- **Memory efficiency**: Consider memory usage for large documents
- **Streaming compatibility**: Ensure works with JSONL streaming

## License

By contributing to this project, you agree that your contributions will be licensed under the same license as the project.
