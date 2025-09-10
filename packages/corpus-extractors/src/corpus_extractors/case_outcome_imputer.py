#!/usr/bin/env python3
"""
Case outcome imputation functionality.

This module integrates the case outcome imputation logic into the corpus-extractors
package, providing final_judgement_real field population for quotes.
"""

from __future__ import annotations
import argparse
import orjson as json
import shutil
import sys
from pathlib import Path
from typing import Iterable, List, NamedTuple

from .extract_cash_amounts_stage1 import (
    AMOUNT_REGEX,
    PROXIMITY_PATTERN,
    JUDGMENT_VERBS,
    SPELLED_OUT_AMOUNTS,
    USD_AMOUNTS,
    extract_spelled_out_amount,
    extract_usd_amount,
    get_spacy_nlp,
    extract_spacy_amounts,
    passes_feature_filter,
    passes_enhanced_feature_filter,
    passes_enhanced_feature_filter_with_titles,
    compute_feature_votes,
    compute_enhanced_feature_votes,
    compute_enhanced_feature_votes_with_titles,
    CONTEXT_CHARS as DEFAULT_CONTEXT,
    DEFAULT_MIN_AMOUNT as DEFAULT_MIN,
    get_case_court_type,
    is_case_dismissed,
    get_case_flags,
    VotingWeights,
    DEFAULT_VOTING_WEIGHTS,
)

# ------------------------------------------------------------------------------
# Data structures
# ------------------------------------------------------------------------------


class Candidate(NamedTuple):
    value: float
    raw_text: str
    context: str
    feature_votes: int


class AmountSelector:
    def choose(self, candidates: List[Candidate]) -> float | None:
        if not candidates:
            return None
        # Sort by feature votes (descending), then by value (descending)
        sorted_candidates = sorted(
            candidates, key=lambda c: (c.feature_votes, c.value), reverse=True
        )
        return sorted_candidates[0].value


class ManualAmountSelector(AmountSelector):
    def choose(self, candidates: List[Candidate]) -> float | None:
        if not candidates:
            print("⚠ No candidate amounts found.")
            return None

        # Sort by feature votes (descending), then by value (descending)
        sorted_candidates = sorted(
            candidates, key=lambda c: (c.feature_votes, c.value), reverse=True
        )

        print("\n── Candidates (ranked by feature votes) ───────────────────────")
        for i, c in enumerate(sorted_candidates, 1):
            print(f"[{i}] {c.value:,.0f} (votes: {c.feature_votes})\t…{c.context}…")
        while True:
            choice = input("\nPick #, 's' to skip, or custom » ").strip()
            if choice.lower() == "s":
                return None
            if choice.isdigit() and 1 <= int(choice) <= len(sorted_candidates):
                return sorted_candidates[int(choice) - 1].value
            try:
                return float(choice.replace(",", ""))
            except ValueError:
                print("Invalid input—try again.")


# ------------------------------------------------------------------------------
# Core imputation functions
# ------------------------------------------------------------------------------


def scan_stage1(
    case_root: Path,
    min_amount: float,
    context_chars: int,
    min_features: int = 2,
    case_position_threshold: float = 0.5,
    docket_position_threshold: float = 0.5,
    voting_weights: VotingWeights = DEFAULT_VOTING_WEIGHTS,
    disable_spacy: bool = False,
    disable_spelled: bool = False,
    disable_usd: bool = False,
    disable_calcs: bool = False,
    disable_regex: bool = False,
) -> List[Candidate]:
    """
    Scan stage1 JSONL files for cash amounts. Each line in stage1 files is a JSON object
    with a 'text' field containing the actual document text.
    Enhanced with spaCy EntityRuler, spelled-out amounts, USD prefixes, judgment-verb filtering,
    and chronological position-based voting.
    """
    seen = set()
    out = []
    all_raw = []  # Track all candidates before filtering

    # Initialize spaCy pipeline once for reuse
    nlp = get_spacy_nlp()

    # Debug: Check if stage1 files exist
    stage1_files = list(case_root.rglob("*_stage1.jsonl"))

    for path in case_root.rglob("*_stage1.jsonl"):
        with open(path, "r", encoding="utf8") as f:
            for line in f:
                try:
                    data = json.loads(line)
                    text = data.get("text", "")
                    if not text:
                        continue

                    # Process spaCy EntityRuler amounts (new - highest priority)
                    if not disable_spacy:
                        spacy_candidates = extract_spacy_amounts(
                            text, nlp, min_amount, context_chars
                        )
                        all_raw.extend(spacy_candidates)  # Track raw candidates
                        for candidate in spacy_candidates:
                            ctx = candidate["context"]
                            # Enhanced filtering: require minimum feature votes including position and titles
                            if passes_enhanced_feature_filter_with_titles(
                                ctx,
                                str(path),
                                min_features,
                                case_position_threshold,
                                docket_position_threshold,
                                voting_weights,
                            ):
                                sig = f"{candidate['amount']}:{ctx[:60]}"
                                if sig not in seen:
                                    seen.add(sig)
                                    feature_votes = (
                                        compute_enhanced_feature_votes_with_titles(
                                            ctx,
                                            str(path),
                                            case_position_threshold,
                                            docket_position_threshold,
                                            voting_weights,
                                        )
                                    )
                                    out.append(
                                        Candidate(
                                            candidate["value"],
                                            candidate["amount"],
                                            ctx,
                                            feature_votes,
                                        )
                                    )

                    # Process enhanced spelled-out amounts (new)
                    if not disable_spelled:
                        spelled_matches = list(SPELLED_OUT_AMOUNTS.finditer(text))
                        for m in spelled_matches:
                            val = extract_spelled_out_amount(text, m)
                            all_raw.append(
                                {"amount": m.group(0), "value": val}
                            )  # Track raw
                            if val >= min_amount:
                                start, end = m.span()
                                ctx = text[
                                    max(0, start - context_chars) : end + context_chars
                                ].replace("\n", " ")
                                # Enhanced filtering: require minimum feature votes including position and titles
                                if passes_enhanced_feature_filter_with_titles(
                                    ctx,
                                    str(path),
                                    min_features,
                                    case_position_threshold,
                                    docket_position_threshold,
                                    voting_weights,
                                ):
                                    sig = f"{m.group(0)}:{ctx[:60]}"
                                    if sig not in seen:
                                        seen.add(sig)
                                        feature_votes = (
                                            compute_enhanced_feature_votes_with_titles(
                                                ctx,
                                                str(path),
                                                case_position_threshold,
                                                docket_position_threshold,
                                                voting_weights,
                                            )
                                        )
                                        out.append(
                                            Candidate(
                                                val, m.group(0), ctx, feature_votes
                                            )
                                        )

                    # Process enhanced USD amounts (new)
                    if not disable_usd:
                        usd_matches = list(USD_AMOUNTS.finditer(text))
                        for m in usd_matches:
                            val = extract_usd_amount(text, m)
                            all_raw.append(
                                {"amount": m.group(0), "value": val}
                            )  # Track raw
                            if val >= min_amount:
                                start, end = m.span()
                                ctx = text[
                                    max(0, start - context_chars) : end + context_chars
                                ].replace("\n", " ")
                                # Enhanced filtering: require minimum feature votes including position and titles
                                if passes_enhanced_feature_filter_with_titles(
                                    ctx,
                                    str(path),
                                    min_features,
                                    case_position_threshold,
                                    docket_position_threshold,
                                    voting_weights,
                                ):
                                    sig = f"{m.group(0)}:{ctx[:60]}"
                                    if sig not in seen:
                                        seen.add(sig)
                                        feature_votes = (
                                            compute_enhanced_feature_votes_with_titles(
                                                ctx,
                                                str(path),
                                                case_position_threshold,
                                                docket_position_threshold,
                                                voting_weights,
                                            )
                                        )
                                        out.append(
                                            Candidate(
                                                val, m.group(0), ctx, feature_votes
                                            )
                                        )

                    # Continue with existing regex extraction (enhanced with judgment-verb filtering)
                    if not disable_regex:
                        regex_matches = list(AMOUNT_REGEX.finditer(text))
                        for m in regex_matches:
                            amt = m.group(0)
                            # strip punctuation but leave "million"/"billion" suffix attached
                            norm = (
                                amt.lower()
                                .replace(",", "")
                                .replace("$", "")
                                .replace("usd", "")
                                .strip()
                            )

                            # Calculate actual value
                            if "million" in norm:
                                multiplier = 1_000_000
                                num_str = norm.replace("million", "").strip()
                            elif "billion" in norm:
                                multiplier = 1_000_000_000
                                num_str = norm.replace("billion", "").strip()
                            else:
                                multiplier = 1
                                num_str = norm

                            try:
                                val = float(num_str) * multiplier
                                all_raw.append(
                                    {"amount": amt, "value": val}
                                )  # Track raw
                            except ValueError:
                                continue

                            # Apply minimum threshold
                            if val < min_amount:
                                continue

                            start, end = m.span()
                            ctx = text[
                                max(0, start - context_chars) : end + context_chars
                            ].replace("\n", " ")

                            # Enhanced filtering: require minimum feature votes including position and titles
                            if not passes_enhanced_feature_filter_with_titles(
                                ctx,
                                str(path),
                                min_features,
                                case_position_threshold,
                                docket_position_threshold,
                                voting_weights,
                            ):
                                continue

                            sig = f"{amt}:{ctx[:60]}"
                            if sig in seen:
                                continue
                            seen.add(sig)

                            feature_votes = compute_enhanced_feature_votes_with_titles(
                                ctx,
                                str(path),
                                case_position_threshold,
                                docket_position_threshold,
                                voting_weights,
                            )
                            out.append(Candidate(val, amt, ctx, feature_votes))

                except json.JSONDecodeError:
                    continue

    # Debug summary
    if len(stage1_files) == 0 or len(out) == 0:
        print(
            f"[DEBUG] {case_root.name}: {len(stage1_files)} stage1 files, {len(out)} final candidates"
        )

    return out


def impute_for_case(
    case_root: Path,
    selector: AmountSelector,
    min_amount: float,
    context_chars: int,
    min_features: int,
    tokenized_root: Path,
    extracted_root: Path,
    outdir: Path | None,
    input_stage: int,
    output_stage: int,
    case_position_threshold: float = 0.5,
    docket_position_threshold: float = 0.5,
    fee_shifting_ratio_threshold: float = 1.0,
    patent_ratio_threshold: float = 20.0,
    dismissal_ratio_threshold: float = 0.5,
    bankruptcy_ratio_threshold: float = 0.5,
    voting_weights: VotingWeights = DEFAULT_VOTING_WEIGHTS,
    disable_spacy: bool = False,
    disable_spelled: bool = False,
    disable_usd: bool = False,
    disable_calcs: bool = False,
    disable_regex: bool = False,
):
    """Impute final judgment amounts for a case."""
    # map this tokenized-case back to its extracted-location
    # map this case folder directly into the extracted tree by name
    extracted_case_root = extracted_root / case_root.name

    # Get case flags with configurable thresholds
    flags = get_case_flags(
        extracted_case_root,
        fee_shifting_ratio_threshold,
        patent_ratio_threshold,
        dismissal_ratio_threshold,
        bankruptcy_ratio_threshold,
    )

    # Check if this is a bankruptcy court case
    court_type = get_case_court_type(extracted_case_root, bankruptcy_ratio_threshold)
    if court_type == "BANKRUPTCY":
        amount = None
        print(
            f"▶ {case_root.relative_to(tokenized_root)} → BANKRUPTCY COURT (auto-null)"
        )
    else:
        # Check if this is a dismissed case
        if flags["is_dismissed"]:
            amount = 0.0
            print(
                f"▶ {case_root.relative_to(tokenized_root)} → DISMISSED CASE (auto-zero)"
            )
        else:
            candidates = scan_stage1(
                extracted_case_root,
                min_amount,
                context_chars,
                min_features,
                case_position_threshold,
                docket_position_threshold,
                voting_weights,
                disable_spacy,
                disable_spelled,
                disable_usd,
                disable_calcs,
                disable_regex,
            )
            amount = selector.choose(candidates)
            print(f"▶ {case_root.relative_to(tokenized_root)} → {amount!r}")

    # Print flags if any are raised
    flag_messages = []
    if flags["has_fee_shifting"]:
        flag_messages.append("🚩 FEE-SHIFTING")
    if flags["has_large_patent_amounts"]:
        flag_messages.append("🚩 LARGE PATENT AMOUNTS")

    if flag_messages:
        print(f"   {' | '.join(flag_messages)}")

    for input_file in case_root.rglob(f"*_stage{input_stage}.jsonl"):
        rewrite_stage_file(
            input_file, amount, outdir, tokenized_root, input_stage, output_stage
        )


def rewrite_stage_file(
    input_file: Path,
    amount: float | None,
    outdir: Path | None,
    tokenized_root: Path,
    input_stage: int,
    output_stage: int,
):
    """Rewrite stage file with final_judgement_real field."""
    rel = input_file.relative_to(tokenized_root)
    rel_tok = input_file.relative_to(tokenized_root)
    target = (outdir or tokenized_root) / rel_tok.parent
    target.mkdir(parents=True, exist_ok=True)
    outname = input_file.name.replace(
        f"_stage{input_stage}.jsonl", f"_stage{output_stage}.jsonl"
    )
    tmp = target / (outname + ".tmp")

    with input_file.open(encoding="utf8") as fin, tmp.open(
        "w", encoding="utf8"
    ) as fout:
        for line in fin:
            rec = json.loads(line)
            rec["final_judgement_real"] = amount
            fout.write(json.dumps(rec).decode() + "\n")
    tmp.replace(target / outname)


def add_final_judgement_to_quotes(quotes: List[dict], final_amount: float | None) -> List[dict]:
    """
    Add final_judgement_real field to quotes for a case.

    Args:
        quotes: List of quote dictionaries
        final_amount: Final judgment amount from case imputation

    Returns:
        Quotes with final_judgement_real field added
    """
    for quote in quotes:
        quote["final_judgement_real"] = final_amount
    return quotes
