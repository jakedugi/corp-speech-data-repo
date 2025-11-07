"""
The Attributor, responsible for identifying the speaker of a quote and
filtering candidates based on company aliases.
"""

import csv
import re
from pathlib import Path
from typing import Iterable, List, Set

import textacy.extract
from corpus_types.schemas.models import QuoteCandidate
from loguru import logger
from spacy.pipeline import EntityRuler

# Default role keywords - can be overridden via config
DEFAULT_ROLE_KEYWORDS = [
    "CEO",
    "CFO",
    "CTO",
    "COO",
    "President",
    "Vice President",
    "VP",
    "Officer",
    "Director",
    "Manager",
    "spokesperson",
    "representative",
    "Chairman",
    "Chairwoman",
    "Chair",
]


def get_nlp(
    model_name: str = "en_core_web_sm", enable_ner: bool = True, use_gpu: bool = False
):
    """
    Load spacy model with full NER support and fallback options.

    Args:
        model_name: Name of spacy model to load (default: en_core_web_sm)
        enable_ner: Whether to enable named entity recognition
        use_gpu: Whether to use GPU acceleration

    Returns:
        Loaded spacy Language object with NER configured
    """
    import spacy

    # Configure GPU usage
    if use_gpu:
        try:
            spacy.require_gpu()
            logger.info("GPU acceleration enabled for spaCy")
        except:
            logger.warning("GPU not available, using CPU")

    try:
        # Load the model
        nlp = spacy.load(model_name)
        logger.info(f"Successfully loaded spaCy model: {model_name}")

        # Ensure NER is available and enabled
        if enable_ner:
            if "ner" not in nlp.pipe_names:
                # Add NER pipeline if not present
                ner = nlp.add_pipe("ner")
                logger.info("Added NER pipeline to spaCy model")
            else:
                logger.info("NER pipeline already present in spaCy model")

            # Verify NER is working
            test_doc = nlp("Apple CEO Tim Cook said")
            if test_doc.ents:
                logger.info(
                    f"NER is working - found {len(test_doc.ents)} entities in test"
                )
            else:
                logger.warning("NER loaded but not finding entities in test")

        return nlp

    except OSError as e:
        logger.warning(
            f"Could not load {model_name}: {e}, using basic English pipeline"
        )

        # Fallback to basic English pipeline
        from spacy.lang.en import English

        nlp = English()

        # Add essential pipelines
        if "sentencizer" not in nlp.pipe_names:
            nlp.add_pipe("sentencizer")

        if enable_ner:
            # Add basic NER-like functionality
            logger.warning("Using basic NER fallback - limited functionality")

        return nlp


class Attributor:
    """
    Multi-sieve quote attribution:
      1) Rule-based cue regex
      2) Dependency-pattern fallback
      3) Textacy direct-quotation triples (original logic)
      4) Alias-enhanced NER via EntityRuler
      5) Optional quantized DistilBERT (commented)
      6) (Commented) Coreference and LLM fallbacks
      7) Final alias & role fallbacks
    """

    ANC_PATTERN = re.compile(
        r"\b(?:said|stated|noted|blogged|posted|wrote|quoted|"
        r"according to|testif(?:y|ied)|deposed|swor(?:e|n)|submitted|"
        r"annonce(?:d|ment)|privacy policy|public statements?)\b",
        re.I,
    )

    def __init__(
        self,
        company_aliases: Set[str],
        spacy_model: str = "en_core_web_sm",
        role_keywords: List[str] = None,
        executive_names: List[str] = None,
        enable_ner: bool = True,
        use_gpu: bool = False,
    ):
        """
        Initializes the attributor with comprehensive corporate data and NLP model.

        Args:
            company_aliases: Set of company/organization aliases to recognize
            spacy_model: Name of spacy model to load (default: en_core_web_sm)
            role_keywords: List of role keywords to recognize (default: DEFAULT_ROLE_KEYWORDS)
            executive_names: List of known executive names for direct matching
            enable_ner: Whether to enable spaCy NER
            use_gpu: Whether to use GPU acceleration
        """
        self.aliases = company_aliases  # already lowered in config
        self.executive_names = set(executive_names or [])
        self.enable_ner = enable_ner

        # Load spaCy model with NER support
        self.nlp = get_nlp(spacy_model, enable_ner=enable_ner, use_gpu=use_gpu)

        self.role_keywords = role_keywords or DEFAULT_ROLE_KEYWORDS

        # Initialize entity ruler for company and executive recognition
        self._add_alias_ruler()

        logger.info(f"Attributor initialized with:")
        logger.info(f"  - {len(company_aliases)} company aliases")
        logger.info(f"  - {len(self.executive_names)} executive names")
        logger.info(f"  - {len(self.role_keywords)} role keywords")
        logger.info(f"  - NER {'enabled' if enable_ner else 'disabled'}")
        logger.info(f"  - spaCy model: {spacy_model}")

        # Optional: load quantized DistilBERT (commented)
        # self.tokenizer = DistilBertTokenizerFast.from_pretrained("distilbert-base-uncased")
        # self.ort_sess = ort.InferenceSession("distilbert_cpu_quant.onnx")

    def _add_alias_ruler(self):
        """Add custom entity ruler with comprehensive aliases (companies + executives + roles)."""
        if "entity_ruler" not in self.nlp.pipe_names:
            ruler = self.nlp.add_pipe("entity_ruler", before="ner")
            patterns = []

            # Add company aliases
            for alias in self.aliases:
                patterns.append({"label": "ORG", "pattern": alias})

            # Add executive names
            for exec_name in self.executive_names:
                if len(exec_name) > 3:  # Skip very short names
                    patterns.append({"label": "PERSON", "pattern": exec_name})

            # Add role keywords
            for keyword in self.role_keywords:
                patterns.append({"label": "TITLE", "pattern": keyword})

            ruler.add_patterns(patterns)
            logger.info(f"Added {len(patterns)} patterns to entity ruler")

    def filter(self, candidates: List[QuoteCandidate]) -> Iterable[QuoteCandidate]:
        """
        Applies all sieves in order. If any sieve finds a valid attribution,
        we break and yield that result. Otherwise, discard the candidate.
        """

        for qc in candidates:
            # 0) Sanitize unbalanced quotes
            ctx = qc.context
            if ctx.count('"') % 2 == 1:
                ctx += '"'
            doc = self.nlp(ctx)
            low_ctx = ctx.lower()
            low_quote = qc.quote.lower() if qc.quote else ""
            aliases = self.aliases | set(self.role_keywords)

            # 1) Rule-based cue regex
            for sent in doc.sents:
                m = re.search(
                    rf'([A-Z][a-z]+)\s+{self.ANC_PATTERN.pattern}\s*[:,-]?\s*[""](.+?)[""]',
                    sent.text,
                )
                if m:
                    qc.speaker = m.group(1)
                    yield qc
                    break
            if getattr(qc, "speaker", None):
                continue

            # 2) Dependency-pattern fallback
            for sent in doc.sents:
                for token in sent:
                    if token.lemma_ in {
                        "say",
                        "state",
                        "note",
                        "post",
                        "blog",
                        "quote",
                        "write",
                    }:
                        subs = [
                            c
                            for c in token.children
                            if c.dep_ in {"nsubj", "nsubjpass"}
                        ]
                        if subs and subs[0].ent_type_ in {"PERSON", "ORG"}:
                            qc.speaker = subs[0].text
                            yield qc
                            break
                if getattr(qc, "speaker", None):
                    break
            if getattr(qc, "speaker", None):
                continue

            # 3) Original Textacy direct-quotation triples
            # try:
            #     for sent in doc.sents:
            #         if low_quote in sent.text.lower():
            #             mini = self.nlp(sent.text)
            #             try:
            #                 for sp, _, _ in textacy.extract.triples.direct_quotations(mini):
            #                     spk = (" ".join([t.text for t in sp]) if isinstance(sp, list) else sp.text)
            #                     if spk:
            #                         qc.speaker = spk
            #                         break
            #             except ValueError:
            #                 pass
            #             break
            # except Exception:
            #     pass
            if getattr(qc, "speaker", None):
                yield qc
                continue

            # 4) Alias-enhanced NER via EntityRuler
            entities = [
                ent
                for ent in doc.ents
                if ent.label_ in {"PERSON", "ORG", "CUSTOM_ENTITY"}
            ]
            for ent in entities:
                if ent.text.lower() in aliases:
                    qc.speaker = ent.text
                    yield qc
                    break
            if getattr(qc, "speaker", None):
                continue

            # 5) Optional quantized DistilBERT pipeline (disabled)
            # if hasattr(self, 'ort_sess'):
            #     inputs = self.tokenizer(ctx, return_tensors="np", truncation=True, max_length=512)
            #     outputs = self.ort_sess.run(None, {"input_ids": inputs["input_ids"], "attention_mask": inputs["attention_mask"]})
            #     ...

            # 6) Final alias & role fallbacks
            for alias in aliases:
                if alias in low_ctx:
                    qc.speaker = alias.title()
                    yield qc
                    break
            if getattr(qc, "speaker", None):
                continue

            # If no attribution found, discard this candidate
            # (do not yield)
