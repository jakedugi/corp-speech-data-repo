PY ?= python
RUN_ID ?= $(shell date -u +%Y%m%dT%H%M%SZ)
OUT ?= outputs/$(RUN_ID)
QUERY ?= configs/query.small.yaml

# Create outputs directory and symlink
$(OUT):
	mkdir -p $(OUT)
	ln -sf $(RUN_ID) outputs/latest 2>/dev/null || true

.PHONY: demo_e2e clean fetch normalize extract validate manifest help status fmt lint test ci wikipedia-help wikipedia-scrape-dow wikipedia-scrape-sp500 wikipedia-scrape-nasdaq wikipedia-scrape-all

help:
	@echo "Data Pipeline Orchestrator"
	@echo ""
	@echo "Commands:"
	@echo "  demo_e2e    - Run full pipeline: clean → fetch → normalize → extract → validate → manifest"
	@echo "  clean       - Remove output directory and recreate"
	@echo "  fetch       - Fetch raw documents using hydrator"
	@echo "  normalize   - Normalize documents using cleaner"
	@echo "  extract     - Extract quotes and outcomes using extractors"
	@echo "  validate    - Validate all outputs against schemas"
	@echo "  manifest    - Generate manifest with versions and fingerprints"
	@echo "  status      - Show current pipeline status"
	@echo ""
	@echo "Wikipedia Key People:"
	@echo "  wikipedia-help        - Show Wikipedia scraper help"
	@echo "  wikipedia-scrape-dow  - Scrape Dow Jones key people"
	@echo "  wikipedia-scrape-sp500 - Scrape S&P 500 key people"
	@echo "  wikipedia-scrape-nasdaq - Scrape NASDAQ 100 key people"
	@echo "  wikipedia-scrape-all  - Scrape all supported indices"
	@echo ""
	@echo "Configuration:"
	@echo "  RUN_ID=$(RUN_ID)"
	@echo "  OUT=$(OUT)"
	@echo "  QUERY=$(QUERY)"

demo_e2e: $(OUT) clean fetch normalize extract validate manifest
	@echo "Data pipeline complete!"
	@echo "Check $(OUT)/manifest.json for results"
	@echo "Latest run: outputs/latest"

clean:
	@echo "Cleaning outputs directory..."
	rm -rf $(OUT) && mkdir -p $(OUT)

fetch:
	@echo "Fetching raw documents..."
	@if [ -f "fixtures/docs.raw.small.jsonl" ]; then \
		hydrator fetch --query $(QUERY) --out $(OUT)/docs.raw.jsonl --use-fixture fixtures/docs.raw.small.jsonl; \
	else \
		hydrator fetch --query $(QUERY) --out $(OUT)/docs.raw.jsonl; \
	fi
	@echo "Fetched documents to $(OUT)/docs.raw.jsonl"

normalize:
	@echo "Normalizing documents..."
	cleaner normalize --in $(OUT)/docs.raw.jsonl --out $(OUT)/docs.norm.jsonl --keep-offset-map
	@echo "Normalized documents to $(OUT)/docs.norm.jsonl"

extract:
	@echo "Extracting quotes and outcomes..."
	extract quotes   --in $(OUT)/docs.norm.jsonl --out $(OUT)/quotes.jsonl
	extract outcomes --in $(OUT)/docs.norm.jsonl --out $(OUT)/outcomes.jsonl
	@echo "Extracted quotes to $(OUT)/quotes.jsonl"
	@echo "Extracted outcomes to $(OUT)/outcomes.jsonl"

validate:
	@echo "Validating outputs..."
	corpus-validate jsonl Doc     $(OUT)/docs.norm.jsonl
	corpus-validate jsonl Quote   $(OUT)/quotes.jsonl
	corpus-validate jsonl Outcome $(OUT)/outcomes.jsonl
	@echo "All validations passed!"

manifest:
	@echo "Generating manifest..."
	$(PY) scripts/write_manifest.py $(OUT)
	@echo "Manifest written to $(OUT)/manifest.json"

status:
	@echo "Pipeline Status:"
	@echo "  RUN_ID: $(RUN_ID)"
	@echo "  OUTPUT: $(OUT)"
	@echo "  LATEST: outputs/latest"
	@if [ -d "$(OUT)" ]; then \
		echo "  Files: $$(ls -1 $(OUT) 2>/dev/null | wc -l) artifacts"; \
		echo "  Size: $$(du -sh $(OUT) 2>/dev/null || echo 'N/A')"; \
	fi
	@if [ -L "outputs/latest" ]; then \
		echo "  Latest points to: $$(readlink outputs/latest)"; \
	fi

fmt:      ruff format .
lint:     ruff check . && mypy packages
test:     pytest -q
ci:       make lint && make test && make demo_e2e

# ============================================================================
# Wikipedia Key People Scraper
# ============================================================================

wikipedia-help:
	@echo "Wikipedia Key People Scraper"
	@echo ""
	@echo "Available commands:"
	@echo "  make wikipedia-scrape-dow     - Scrape Dow Jones constituents"
	@echo "  make wikipedia-scrape-sp500   - Scrape S&P 500 constituents"
	@echo "  make wikipedia-scrape-nasdaq  - Scrape NASDAQ 100 constituents"
	@echo "  make wikipedia-scrape-all     - Scrape all supported indices"
	@echo ""
	@echo "Examples:"
	@echo "  python -m corpus_hydrator.adapters.wikipedia_key_people.cli.commands scrape-index-normalized --index dow --max-companies 5 --verbose"

wikipedia-scrape-dow:
	@echo "Scraping Dow Jones key people..."
	python -m corpus_hydrator.adapters.wikipedia_key_people.cli.commands scrape-index-normalized --index dow --verbose

wikipedia-scrape-sp500:
	@echo "Scraping S&P 500 key people..."
	python -m corpus_hydrator.adapters.wikipedia_key_people.cli.commands scrape-index-normalized --index sp500 --verbose

wikipedia-scrape-nasdaq:
	@echo "Scraping NASDAQ 100 key people..."
	python -m corpus_hydrator.adapters.wikipedia_key_people.cli.commands scrape-index-normalized --index nasdaq100 --verbose

wikipedia-scrape-all: wikipedia-scrape-dow wikipedia-scrape-sp500 wikipedia-scrape-nasdaq
	@echo "All indices scraped successfully!"

# ============================================================================
# Development Utilities
# ============================================================================

setup-wikipedia:
	@echo "Setting up Wikipedia key people development environment..."
	uv pip install -e . --all-extras
	pre-commit install

test-wikipedia:
	@echo "Running Wikipedia key people tests..."
	python -m pytest tests/unit/adapters/wikipedia_key_people/ tests/contracts/test_wikipedia_key_people_contract.py -v

lint-wikipedia:
	@echo "Linting Wikipedia key people code..."
	flake8 packages/corpus-hydrator/src/corpus_hydrator/adapters/wikipedia_key_people/
	mypy packages/corpus-hydrator/src/corpus_hydrator/adapters/wikipedia_key_people/ --ignore-missing-imports

format-wikipedia:
	@echo "Formatting Wikipedia key people code..."
	black packages/corpus-hydrator/src/corpus_hydrator/adapters/wikipedia_key_people/
	isort packages/corpus-hydrator/src/corpus_hydrator/adapters/wikipedia_key_people/
