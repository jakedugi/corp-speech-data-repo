# Troubleshooting

Common issues and their solutions when running the data pipeline.

## Pipeline Issues

### "Command not found" errors

**Problem**: CLI commands not found (e.g., `hydrator`, `cleaner`, `extract`)

**Solutions**:
```bash
# Ensure packages are installed
uv pip install -e packages/corpus-types packages/corpus-hydrator packages/corpus-cleaner packages/corpus-extractors

# Or with pip
pip install -e packages/corpus-types packages/corpus-hydrator packages/corpus-cleaner packages/corpus-extractors

# Check installation
which hydrator
which cleaner
which extract
corpus-validate --help
```

### Import errors

**Problem**: `ModuleNotFoundError` when running commands

**Solutions**:
```bash
# Ensure you're in the correct directory
cd /path/to/data-repo

# Set Python path
export PYTHONPATH="$(pwd)/packages"

# Or run with PYTHONPATH
PYTHONPATH="$(pwd)/packages" make demo_e2e
```

### Permission denied

**Problem**: Cannot write to `data/` directory or install packages

**Solutions**:
```bash
# Create data directory with proper permissions
mkdir -p data
chmod 755 data

# Install in user space (if system install fails)
pip install --user -e packages/corpus-types

# Or use virtual environment
python -m venv .venv
source .venv/bin/activate
pip install -e packages/corpus-types
```

## Data Issues

### Empty outputs

**Problem**: Pipeline runs but produces empty files

**Solutions**:
```bash
# Check input data
wc -l data/docs.raw.jsonl
head -1 data/docs.raw.jsonl

# Run with verbose logging
LOG_LEVEL=DEBUG make demo_e2e

# Check if fixtures are being used correctly
ls -la fixtures/
```

### Schema validation failures

**Problem**: `corpus-validate` reports errors

**Solutions**:
```bash
# Get detailed error report
corpus-validate jsonl Doc data/docs.norm.jsonl --verbose

# Check first few records
head -5 data/docs.norm.jsonl | jq .

# Validate individual records
corpus-validate jsonl Doc data/docs.norm.jsonl --limit 10
```

### Quote extraction issues

**Problem**: No quotes found or incorrect speaker attribution

**Solutions**:
```bash
# Check text quality
head -1 data/docs.norm.jsonl | jq .raw_text

# Run with debug logging
LOG_LEVEL=DEBUG extract quotes --in data/docs.norm.jsonl --out data/quotes.jsonl

# Check normalization preserved quote markers
grep '"' data/docs.norm.jsonl | head -5
```

### Outcome classification problems

**Problem**: Incorrect or missing case outcomes

**Solutions**:
```bash
# Check document content
grep -i "outcome\|settlement\|dismissed" data/docs.norm.jsonl

# Run outcome extraction with debug
LOG_LEVEL=DEBUG extract outcomes --in data/docs.norm.jsonl --out data/outcomes.jsonl

# Verify case identification logic
grep "case_id" data/outcomes.jsonl
```

## Network and API Issues

### CourtListener API errors

**Problem**: API requests failing

**Solutions**:
```bash
# Check API key
echo $HYDRATOR_API_KEY

# Test API connectivity
curl -H "Authorization: Token $HYDRATOR_API_KEY" \
     "https://www.courtlistener.com/api/rest/v4/search/"

# Use offline mode for testing
hydrator fetch --use-fixture fixtures/docs.raw.small.jsonl --out data/docs.raw.jsonl

# Check rate limits
REQUESTS_TIMEOUT=60 MAX_RETRIES=5 make fetch
```

### Connection timeouts

**Problem**: Network requests timing out

**Solutions**:
```bash
# Increase timeouts
export REQUESTS_TIMEOUT=120
export MAX_RETRIES=10

# Run with retry logic
REQUESTS_TIMEOUT=120 MAX_RETRIES=10 make fetch

# Use smaller batches
# Edit configs/query.small.yaml to reduce max_results
```

## Text Processing Issues

### Normalization problems

**Problem**: Text normalization changing quote positions

**Solutions**:
```bash
# Check offset mapping
cleaner normalize --in data/docs.raw.jsonl --out data/docs.norm.jsonl --keep-offset-map

# Compare before/after
diff data/docs.raw.jsonl data/docs.norm.jsonl

# Verify Unicode handling
python -c "
import unicodedata
with open('data/docs.norm.jsonl') as f:
    text = f.read()
    print('Normalized form:', unicodedata.normalize('NFC', text[:100]))
"
```

### Character encoding issues

**Problem**: Unicode characters not handled correctly

**Solutions**:
```bash
# Check file encoding
file data/docs.raw.jsonl

# Convert to UTF-8 if needed
iconv -f latin1 -t utf8 data/docs.raw.jsonl > data/docs.raw.utf8.jsonl

# Verify UTF-8 compliance
python -c "
with open('data/docs.raw.jsonl', 'rb') as f:
    data = f.read()
    data.decode('utf-8')  # Will raise error if not UTF-8
"
```

## Performance Issues

### Slow processing

**Problem**: Pipeline taking too long

**Solutions**:
```bash
# Check system resources
top -l 1 | head -10

# Reduce batch sizes
export BATCH_SIZE=50

# Use fewer workers
export MAX_WORKERS=2

# Profile performance
python -m cProfile -s time $(which hydrator) fetch --query configs/query.small.yaml --out data/test.jsonl
```

### Memory issues

**Problem**: Out of memory errors

**Solutions**:
```bash
# Process in smaller chunks
export BATCH_SIZE=25

# Use streaming processing
export STREAMING=true

# Monitor memory usage
python -c "
import psutil
import os
process = psutil.Process(os.getpid())
print(f'Memory usage: {process.memory_info().rss / 1024 / 1024:.1f} MB')
"
```

## Determinism Issues

### Non-deterministic outputs

**Problem**: Same inputs produce different outputs

**Solutions**:
```bash
# Check random seeds
python -c "import random; random.seed(42)"

# Verify timestamps are consistent
python -c "
import json
with open('data/manifest.json') as f:
    manifest = json.load(f)
    print('Fingerprints:', manifest['fingerprints'])
"

# Run multiple times and compare
for i in range(3):
    make clean demo_e2e
    mv data/manifest.json data/manifest_$i.json

# Compare manifests
diff data/manifest_0.json data/manifest_1.json
```

### ID collisions

**Problem**: Duplicate IDs generated

**Solutions**:
```bash
# Check ID uniqueness
python -c "
import json
with open('data/quotes.jsonl') as f:
    quotes = [json.loads(line) for line in f]
    ids = [q['quote_id'] for q in quotes]
    duplicates = set([x for x in ids if ids.count(x) > 1])
    print('Duplicate IDs:', duplicates)
"

# Verify ID generation logic
python -c "
from corpus_types.utils.deterministic_ids import generate_quote_id
id1 = generate_quote_id('test', 10, 20, 'test quote')
id2 = generate_quote_id('test', 10, 20, 'test quote')
print('IDs match:', id1 == id2)
"
```

## Development Environment Issues

### Virtual environment problems

**Problem**: Package installation issues

**Solutions**:
```bash
# Recreate virtual environment
rm -rf .venv
python -m venv .venv
source .venv/bin/activate

# Install in correct order
pip install -e packages/corpus-types
pip install -e packages/corpus-hydrator
pip install -e packages/corpus-cleaner
pip install -e packages/corpus-extractors

# Verify installations
python -c "import corpus_types, corpus_hydrator, corpus_cleaner, corpus_extractors"
```

### Dependency conflicts

**Problem**: Package version conflicts

**Solutions**:
```bash
# Check current versions
pip list | grep -E "(pydantic|httpx|transformers)"

# Update conflicting packages
pip install --upgrade pydantic httpx transformers

# Use uv for better dependency resolution
uv sync

# Check for conflicts
pip check
```

## Logging and Debugging

### Enable debug logging

```bash
# Set environment variables
export LOG_LEVEL=DEBUG
export LOG_FORMAT=json

# Run with detailed logging
LOG_LEVEL=DEBUG make demo_e2e 2>&1 | tee pipeline.log
```

### Common debug commands

```bash
# Check Python path
python -c "import sys; print('\n'.join(sys.path))"

# Verify package locations
python -c "import corpus_types; print(corpus_types.__file__)"

# Test individual imports
python -c "from corpus_types.schemas.models import Doc; print('Import successful')"

# Check CLI entry points
python -c "import pkg_resources; [print(ep) for ep in pkg_resources.iter_entry_points('console_scripts') if 'corpus' in str(ep)]"
```

## Getting Help

### Documentation resources
- [docs/01_overview.md](01_overview.md) - Architecture overview
- [docs/02_running_the_pipeline.md](02_running_the_pipeline.md) - Step-by-step guide
- [docs/03_data_contracts.md](03_data_contracts.md) - Schema documentation

### Quick diagnostic script
```bash
#!/bin/bash
echo "=== Pipeline Diagnostics ==="
echo "Python version: $(python --version)"
echo "Working directory: $(pwd)"
echo "Python path: $PYTHONPATH"
echo ""
echo "Package installations:"
python -c "
try:
    import corpus_types
    print('✅ corpus_types installed')
except ImportError as e:
    print(f'❌ corpus_types: {e}')

try:
    import corpus_hydrator
    print('✅ corpus_hydrator installed')
except ImportError as e:
    print(f'❌ corpus_hydrator: {e}')

try:
    import corpus_cleaner
    print('✅ corpus_cleaner installed')
except ImportError as e:
    print(f'❌ corpus_cleaner: {e}')

try:
    import corpus_extractors
    print('✅ corpus_extractors installed')
except ImportError as e:
    print(f'❌ corpus_extractors: {e}')
"
echo ""
echo "CLI tools:"
which hydrator || echo "❌ hydrator not found"
which cleaner || echo "❌ cleaner not found"
which extract || echo "❌ extract not found"
which corpus-validate || echo "❌ corpus-validate not found"
```

Run this script to quickly diagnose most common issues.
