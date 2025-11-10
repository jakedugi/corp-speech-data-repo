#!/bin/bash
echo "Running complete data pipeline..."

# 1. Extract index constituents for all indexes
echo "Step 1: Extracting index constituents..."
uv run hydrator index-constituents --index all --output-dir data/ --force --verbose

# 2. Extract key people for all indexes  
echo "Step 2: Extracting key people data..."
uv run python scripts/orchestrator.py run --index dow,sp500,nasdaq100 --output-dir data/ --force --verbose

echo "Complete pipeline finished!"

