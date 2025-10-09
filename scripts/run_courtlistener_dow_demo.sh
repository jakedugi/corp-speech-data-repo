#!/bin/bash
# CourtListener Dow Jones Demo Script

set -e

echo "CourtListener Dow Jones Orchestration Demo"
echo "=============================================="

# Check if API token is set
if [ -z "$COURTLISTENER_API_TOKEN" ]; then
    echo "ERROR Error: COURTLISTENER_API_TOKEN environment variable not set!"
    echo "Please set it with: export COURTLISTENER_API_TOKEN=your_token_here"
    exit 1
fi

# Check if Dow Jones file exists
if [ ! -f "data/dowjonesindustrialaverage_constituents.csv" ]; then
    echo "ERROR Error: Dow Jones constituents file not found!"
    echo "Expected: data/dowjonesindustrialaverage_constituents.csv"
    exit 1
fi

echo "OK Prerequisites check passed"

# Step 1: Preview what will be processed
echo ""
echo " Step 1: Preview query chunks"
echo "-------------------------------"
python -m corpus_hydrator.cli.cli_courtlistener orchestrate \
  --statutes "FTC Section 5 (9th Cir.)" \
  --company-file data/dowjonesindustrialaverage_constituents.csv \
  --print-query-chunks

echo ""
echo " Step 2: Ready to run full orchestration?"
echo "------------------------------------------"
echo "Command to run:"
echo "python -m corpus_hydrator.cli.cli_courtlistener orchestrate \\"
echo "  --statutes \"FTC Section 5 (9th Cir.)\" \\"
echo "  --company-file data/dowjonesindustrialaverage_constituents.csv \\"
echo "  --outdir results/courtlistener/dow-ftc5 \\"
echo "  --chunk-size 5 \\"
echo "  --pages 1 \\"
echo "  --page-size 50"
echo ""
echo "Run this command to start the full orchestration!"
echo ""
echo " Output will be saved to: results/courtlistener/dow-ftc5/"
echo " Expected: 30 companies → 6 chunks → CourtListener queries"
