import re
import unicodedata
from typing import Dict, List, Tuple


class TextCleaner:
    _PAGE_MARKER = re.compile(r"^(?:Page \d+ of \d+)\s*$", flags=re.M)
    _DEHYPHEN = re.compile(r"(\w+)-\s*\n\s*(\w+)")
    _INDENT = re.compile(r"^[ \t]+", flags=re.M)
    _BLANK_LINES = re.compile(r"\n{3,}")
    _FANCY_QUOTES = {
        "\u2018": "'",
        "\u2019": "'",
        "\u201C": '"',
        "\u201D": '"',
        "\u2013": "-",
        "\u2014": "-",
        "\u00A0": " ",
    }

    def clean(self, text: str) -> str:
        text = self._PAGE_MARKER.sub("", text)
        # drop any standalone "Id." or "EPIC – CDD Complaint..." lines
        text = re.sub(
            r"(?m)^\s*(Id\.|EPIC\s*–\s*CDD Complaint|Federal Trade Commission).*\n",
            "",
            text,
        )
        # drop stray inline footnote markers like "[35]"
        text = re.sub(r"\s*\[\d+\]", "", text)
        text = unicodedata.normalize("NFKC", text)
        for f, r in self._FANCY_QUOTES.items():
            text = text.replace(f, r)
        # collapse any run of spaces/tabs into a single space
        text = re.sub(r"[ \t]{2,}", " ", text)
        # Remove lines that are just page numbers or footnote markers
        text = re.sub(r"(?m)^\s*\[?\d+\]?\s*$", "", text)
        # Remove hyphenation across lines
        text = re.sub(r"-\n", "", text)
        # Collapse single line-breaks into spaces (but keep paragraph breaks)
        text = re.sub(r"(?<!\n)\n(?!\n)", " ", text)
        text = self._DEHYPHEN.sub(r"\1\2", text)
        text = self._INDENT.sub("", text)
        text = self._BLANK_LINES.sub("\n\n", text)
        text = "\n".join(line.rstrip() for line in text.splitlines()).strip()
        # Final whitespace normalization: collapse any run of spaces/tabs into a single space
        text = re.sub(r"[ \t]{2,}", " ", text)
        return text

    def normalize_text_with_offsets(self, text: str) -> Tuple[str, List[Tuple[int, int]]]:
        """
        Normalize text and return character offset mappings.

        Returns:
            Tuple of (normalized_text, offset_map)
            where offset_map is [(orig_start, orig_end, norm_start, norm_end), ...]
        """
        # For now, return identity mapping - full implementation would track
        # character positions through each normalization step
        normalized = self.clean(text)
        # Simple identity mapping - in practice this would track each transformation
        offset_map = [(i, i, i, i) for i in range(len(normalized))]
        return normalized, offset_map
