DATA_DIR ?= data
QUERY    ?= configs/query.small.yaml

.PHONY: demo_e2e clean fetch normalize extract validate manifest help

help:
	@echo "Data Pipeline Orchestrator"
	@echo ""
	@echo "Commands:"
	@echo "  demo_e2e    - Run full pipeline: clean → fetch → normalize → extract → validate → manifest"
	@echo "  clean       - Remove data directory and recreate"
	@echo "  fetch       - Fetch raw documents using hydrator"
	@echo "  normalize   - Normalize documents using cleaner"
	@echo "  extract     - Extract quotes and outcomes using extractors"
	@echo "  validate    - Validate all outputs against schemas"
	@echo "  manifest    - Generate manifest with versions and fingerprints"
	@echo ""
	@echo "Configuration:"
	@echo "  DATA_DIR=$(DATA_DIR)"
	@echo "  QUERY=$(QUERY)"

demo_e2e: clean fetch normalize extract validate manifest
	@echo "✅ Data pipeline complete!"
	@echo "📊 Check $(DATA_DIR)/manifest.json for results"

clean:
	@echo "🧹 Cleaning data directory..."
	rm -rf $(DATA_DIR)
	mkdir -p $(DATA_DIR)
	@echo "📁 Created $(DATA_DIR)/"

fetch:
	@echo "📥 Fetching raw documents..."
	@if [ -f "corpus_types/fixtures/docs.raw.small.jsonl" ]; then \
		corpus-fetch courtlistener --query $(QUERY) --use-fixture corpus_types/fixtures/docs.raw.small.jsonl --output $(DATA_DIR)/docs.raw.jsonl; \
	else \
		corpus-fetch courtlistener --query $(QUERY) --output $(DATA_DIR)/docs.raw.jsonl; \
	fi
	@echo "✅ Fetched documents to $(DATA_DIR)/docs.raw.jsonl"

normalize:
	@echo "🧽 Normalizing documents..."
	corpus-clean normalize --input $(DATA_DIR)/docs.raw.jsonl --output $(DATA_DIR)/docs.norm.jsonl
	@echo "✅ Normalized documents to $(DATA_DIR)/docs.norm.jsonl"

extract:
	@echo "🔍 Extracting quotes and outcomes..."
	corpus-extract-quotes --input $(DATA_DIR)/docs.norm.jsonl --output $(DATA_DIR)/quotes.jsonl
	corpus-extract-outcomes --input $(DATA_DIR)/docs.norm.jsonl --output $(DATA_DIR)/outcomes.jsonl
	@echo "✅ Extracted quotes to $(DATA_DIR)/quotes.jsonl"
	@echo "✅ Extracted outcomes to $(DATA_DIR)/outcomes.jsonl"

validate:
	@echo "✅ Validating outputs..."
	corpus-validate jsonl Doc $(DATA_DIR)/docs.norm.jsonl
	corpus-validate jsonl Quote $(DATA_DIR)/quotes.jsonl
	corpus-validate jsonl Outcome $(DATA_DIR)/outcomes.jsonl
	@echo "✅ All validations passed!"

manifest:
	@echo "📋 Generating manifest..."
	python scripts/write_manifest.py $(DATA_DIR)
	python scripts/write_run_log.py $(DATA_DIR) demo_e2e
	@echo "✅ Manifest written to $(DATA_DIR)/manifest.json"
	@echo "✅ Run log written to $(DATA_DIR)/RUN.md"
