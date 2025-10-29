from __future__ import annotations

import json
import math
from collections import Counter
from pathlib import Path
from typing import Iterable, Optional

import spacy


class DomainKeywordCollector:
    """Build corpus-aware stats for query-time keyword extraction with incremental updates."""

    def __init__(
        self,
        *,
        spacy_model: str = "en_core_web_sm",
        min_token_len: int = 3,
        max_token_len: int = 40,
    ) -> None:
        self._nlp = spacy.load(spacy_model, disable=["ner", "textcat"])
        self._min_len = min_token_len
        self._max_len = max_token_len

        self._doc_count = 0
        self._df = Counter()  # Document frequency
        self._tf = Counter()  # Total term frequency
        self._title_hits = Counter()
        self._llm_hits = Counter()
        self._acronym_hits = Counter()

    @classmethod
    def load(cls, target_dir: Path, **init_kwargs) -> "DomainKeywordCollector":
        """Load existing stats from disk to continue incremental updates."""
        instance = cls(**init_kwargs)
        stats_file = target_dir / "domain_stats.json"

        if not stats_file.exists():
            return instance

        with stats_file.open("r", encoding="utf-8") as fh:
            data = json.load(fh)

        instance._doc_count = data["doc_count"]

        # Reconstruct counters from metadata
        for term, meta in data["meta"].items():
            instance._df[term] = meta["df"]
            instance._tf[term] = meta["tf"]
            instance._title_hits[term] = meta["title_hits"]
            instance._llm_hits[term] = meta["llm_hits"]
            if meta["is_acronym"]:
                instance._acronym_hits[term] = 1

        return instance

    def consume_document(
        self,
        *,
        title: str,
        content: str,
        llm_keywords: Optional[Iterable[str]] = None,
    ) -> None:
        """Register a document's terms for DF/TF statistics."""
        if not content:
            return

        self._doc_count += 1

        # Track unique terms per document for DF
        doc_terms = set()

        # Track term frequencies within THIS document
        local_tf = Counter()

        doc = self._nlp(content)

        # Extract noun chunks (more meaningful phrases)
        for chunk in doc.noun_chunks:
            term = self._normalise_text(chunk.lemma_)
            if term:
                doc_terms.add(term)
                local_tf[term] += 1

        # Extract individual tokens (catch terms missed by chunking)
        # But skip if already captured as part of noun chunk
        processed_tokens = set()
        for chunk in doc.noun_chunks:
            for token in chunk:
                processed_tokens.add(token.i)

        for token in doc:
            if token.i in processed_tokens:
                continue  # Skip tokens already in noun chunks

            term = self._normalise_token(token)
            if not term:
                continue

            doc_terms.add(term)
            local_tf[term] += 1

            if token.text.isupper() and len(token.text) > 1:
                self._acronym_hits[term] += 1

        # Process title terms
        title_terms = self._extract_title_terms(title)
        for term in title_terms:
            doc_terms.add(term)
            self._title_hits[term] += 1
            local_tf[term] += 1

        # Process LLM-extracted keywords
        if llm_keywords:
            for keyword in llm_keywords:
                # Check for acronym BEFORE normalization
                is_acronym = keyword.isupper() and len(keyword) > 1

                # Try to preserve as phrase first
                phrase = self._normalise_phrase(keyword)
                if phrase:
                    doc_terms.add(phrase)
                    self._llm_hits[phrase] += 1
                    local_tf[phrase] += 1
                    if is_acronym:
                        self._acronym_hits[phrase] += 1

                    # Also add individual words from phrase
                    if ' ' in phrase:
                        for word in phrase.split():
                            term = self._normalise_text(word)
                            if term:
                                doc_terms.add(term)
                                self._llm_hits[term] += 1
                                local_tf[term] += 1

        # Update document frequency (unique terms in this doc)
        for term in doc_terms:
            self._df[term] += 1

        # Update global term frequency (sum of all occurrences)
        for term, freq in local_tf.items():
            self._tf[term] += freq

    def dump(self, target_dir: Path, *, min_df: int = 2) -> None:
        """Write JSON artifacts + SymSpell dictionary."""
        target_dir.mkdir(parents=True, exist_ok=True)

        # Terms that appear in at least min_df documents
        strong_terms = {
            term
            for term, df in self._df.items()
            if df >= min_df and self._tf[term] >= min_df
        }

        # Terms that appear in >85% of documents (likely stop words)
        stop_terms = {
            term
            for term, df in self._df.items()
            if self._doc_count and (df / self._doc_count) > 0.85
        }

        payload = {
            "doc_count": self._doc_count,
            "idf": {
                term: math.log((self._doc_count + 1) / (self._df[term] + 1)) + 1.0
                for term in strong_terms
            },
            "meta": {
                term: {
                    "title_hits": self._title_hits.get(term, 0),
                    "llm_hits": self._llm_hits.get(term, 0),
                    "tf": self._tf[term],
                    "df": self._df[term],
                    "is_acronym": bool(self._acronym_hits.get(term, 0)),
                }
                for term in strong_terms
            },
            "stop_terms": sorted(stop_terms),
        }

        (target_dir / "domain_stats.json").write_text(
            json.dumps(payload, ensure_ascii=True, indent=2),
            encoding="utf-8",
        )

        with (target_dir / "symspell_dictionary.txt").open("w", encoding="utf-8") as fh:
            for term in sorted(strong_terms):
                fh.write(f"{term} {self._tf[term]}\n")

    def _normalise_token(self, token) -> Optional[str]:
        if token.is_stop or token.is_punct or token.like_num:
            return None
        return self._normalise_text(token.lemma_)

    def _extract_title_terms(self, title: str) -> set[str]:
        if not title:
            return set()
        doc = self._nlp(title)
        return {
            term
            for term in {
                self._normalise_text(token.lemma_) for token in doc if not token.is_stop
            }
            if term
        }

    def _normalise_raw(self, text: str) -> Optional[str]:
        return self._normalise_text(text)

    def _normalise_text(self, text: str) -> Optional[str]:
        if not text:
            return None
        term = text.lower().strip()
        if not term.isalpha():
            return None
        if not (self._min_len <= len(term) <= self._max_len):
            return None
        return term

    def _normalise_phrase(self, text: str) -> Optional[str]:
        """Normalize multi-word phrases (allows spaces)."""
        if not text:
            return None
        term = text.lower().strip()
        # Allow alphanumeric + spaces, but must contain letters
        if not any(c.isalpha() for c in term):
            return None
        # Remove extra whitespace
        term = ' '.join(term.split())
        if not (self._min_len <= len(term) <= self._max_len):
            return None
        return term
