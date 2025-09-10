"""Complete document processing pipeline with all features integrated."""

import json
import argparse
from pathlib import Path
from typing import List, Dict, Any, Optional
import logging

from .extract_quotes import QuoteExtractor
from .court_provenance import CourtProvenanceExtractor
from .case_outcome_imputer import add_final_judgement_to_quotes
from corpus_cleaner.cleaner import TextCleaner

logger = logging.getLogger(__name__)


class DocumentProcessor:
    """Complete document processing pipeline."""

    def __init__(
        self,
        case_dir: Optional[str] = None,
        keywords: Optional[List[str]] = None,
        company_aliases: Optional[List[str]] = None,
        enable_position_features: bool = True,
        enable_court_provenance: bool = True,
        enable_outcome_imputation: bool = True,
    ):
        """Initialize the document processor.

        Args:
            case_dir: Path to case directory for position features
            keywords: Keywords for quote extraction
            company_aliases: Company aliases for attribution
            enable_position_features: Whether to compute position features
            enable_court_provenance: Whether to extract court/law/company info
            enable_outcome_imputation: Whether to add final judgment amounts
        """
        self.case_dir = case_dir
        self.enable_position_features = enable_position_features
        self.enable_court_provenance = enable_court_provenance
        self.enable_outcome_imputation = enable_outcome_imputation

        # Initialize components
        self.quote_extractor = QuoteExtractor(
            keywords=keywords,
            company_aliases=set(company_aliases or []),
            case_dir=case_dir if enable_position_features else None
        )
        self.court_provenance_extractor = CourtProvenanceExtractor()
        self.text_cleaner = TextCleaner()

        # Initialize outcome imputation if enabled
        if enable_outcome_imputation:
            from .case_outcome_imputer import AmountSelector
            self.amount_selector = AmountSelector()

        logger.info("Document processor initialized with all features")

    def process_single_document(
        self,
        doc: Dict[str, Any],
        case_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Process a single document through the complete pipeline.

        Args:
            doc: Document dictionary with raw_text and metadata
            case_id: Case identifier (if not in doc)

        Returns:
            Processed document with all extracted fields
        """
        # Extract basic fields
        doc_id = doc.get("doc_id", "")
        raw_text = doc.get("raw_text", "")
        case_id = case_id or doc.get("case_id", "")

        if not raw_text:
            logger.warning(f"No raw_text found for document {doc_id}")
            return doc

        # Clean text
        cleaned_text = self.text_cleaner.clean(raw_text)

        # Extract quotes
        quotes = self.quote_extractor.extract_quotes(
            cleaned_text, doc_id=doc_id, case_id=case_id
        )

        # Convert quotes to dictionaries for processing
        quote_dicts = []
        for quote in quotes:
            quote_dict = quote.to_dict()
            quote_dicts.append(quote_dict)

        # Add court provenance if enabled
        if self.enable_court_provenance and quote_dicts:
            quote_dicts = self.court_provenance_extractor.enrich_quotes_with_provenance(quote_dicts)

        # Add final judgment if enabled (placeholder - would need actual case data)
        if self.enable_outcome_imputation and quote_dicts:
            # This is a placeholder - in practice you'd need to run the full case imputation
            # For now, we'll add a placeholder value
            final_amount = doc.get("final_judgement_real")  # Use if already computed
            if final_amount is not None:
                quote_dicts = add_final_judgement_to_quotes(quote_dicts, final_amount)

        # Create processed document
        processed_doc = doc.copy()
        processed_doc["cleaned_text"] = cleaned_text
        processed_doc["quotes"] = quote_dicts
        processed_doc["quote_count"] = len(quote_dicts)

        # Add provenance fields to document level
        if self.enable_court_provenance:
            doc_provenance = self.court_provenance_extractor.extract_provenance_from_doc(doc)
            processed_doc.update(doc_provenance)

        return processed_doc

    def process_document_batch(
        self,
        input_file: Path,
        output_file: Path,
        case_id: Optional[str] = None
    ) -> None:
        """Process a batch of documents.

        Args:
            input_file: Input JSONL file with documents
            output_file: Output JSONL file for processed documents
            case_id: Optional case ID override
        """
        logger.info(f"Processing documents from {input_file}")

        with open(input_file, 'r', encoding='utf-8') as infile, \
             open(output_file, 'w', encoding='utf-8') as outfile:

            for line_num, line in enumerate(infile, 1):
                if line_num % 100 == 0:
                    logger.info(f"Processed {line_num} documents")

                try:
                    doc = json.loads(line.strip())
                    processed_doc = self.process_single_document(doc, case_id)
                    outfile.write(json.dumps(processed_doc, ensure_ascii=False) + '\n')

                except Exception as e:
                    logger.error(f"Error processing document {line_num}: {e}")
                    continue

        logger.info(f"Processing complete. Output written to {output_file}")

    def process_quotes_only(
        self,
        input_file: Path,
        output_file: Path,
        case_id: Optional[str] = None
    ) -> None:
        """Process documents and output only quotes with all fields.

        Args:
            input_file: Input JSONL file with documents
            output_file: Output JSONL file for quotes only
            case_id: Optional case ID override
        """
        logger.info(f"Processing quotes from {input_file}")

        with open(input_file, 'r', encoding='utf-8') as infile, \
             open(output_file, 'w', encoding='utf-8') as outfile:

            for line_num, line in enumerate(infile, 1):
                if line_num % 100 == 0:
                    logger.info(f"Processed {line_num} documents")

                try:
                    doc = json.loads(line.strip())
                    processed_doc = self.process_single_document(doc, case_id)

                    # Output each quote as a separate line
                    for quote in processed_doc.get("quotes", []):
                        outfile.write(json.dumps(quote, ensure_ascii=False) + '\n')

                except Exception as e:
                    logger.error(f"Error processing document {line_num}: {e}")
                    continue

        logger.info(f"Quote extraction complete. Output written to {output_file}")


def main():
    """Command-line interface for document processing."""
    parser = argparse.ArgumentParser(description="Process documents with complete feature extraction")

    parser.add_argument(
        "--input", "-i", type=Path, required=True,
        help="Input JSONL file with documents"
    )
    parser.add_argument(
        "--output", "-o", type=Path, required=True,
        help="Output JSONL file"
    )
    parser.add_argument(
        "--case-dir", type=str,
        help="Case directory for position features"
    )
    parser.add_argument(
        "--case-id", type=str,
        help="Case ID override"
    )
    parser.add_argument(
        "--quotes-only", action="store_true",
        help="Output quotes only (not full documents)"
    )
    parser.add_argument(
        "--no-position", action="store_true",
        help="Disable position feature computation"
    )
    parser.add_argument(
        "--no-provenance", action="store_true",
        help="Disable court/law/company provenance extraction"
    )
    parser.add_argument(
        "--no-outcome", action="store_true",
        help="Disable final judgment imputation"
    )

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # Initialize processor
    processor = DocumentProcessor(
        case_dir=args.case_dir,
        enable_position_features=not args.no_position,
        enable_court_provenance=not args.no_provenance,
        enable_outcome_imputation=not args.no_outcome,
    )

    # Process based on mode
    if args.quotes_only:
        processor.process_quotes_only(args.input, args.output, args.case_id)
    else:
        processor.process_document_batch(args.input, args.output, args.case_id)


if __name__ == "__main__":
    main()
