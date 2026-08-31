"""PII masking for the RAG corpus.

Support ticket text demonstrably contains customer names, email addresses and
phone numbers. CONTRACTS.md §10 requires it be masked *before* embedding, and
the sequencing is the reason this module is dictionary-driven rather than
regex-driven.

Why the dictionary comes first
------------------------------
The vector index is built in Phase 1, before `dim_customer` exists, so there is
no warehouse table to look real values up from. Regex alone would then be the
only defence, and regex cannot recognise a name: "Elowen Petrossian" has no
lexical property distinguishing it from "Express Shipping". Combine that with
the indexer's content-hash skip logic — which exists so re-indexing is cheap —
and a name that leaks on the first pass is never re-embedded. The leak becomes
permanent, and silent.

So the seed generator emits every name, email and phone it invents into
seeds/known_pii_terms.json, and masking reads that manifest. The regex pass
still runs afterwards, for the shapes it is genuinely good at.

Order of the passes is load-bearing
-----------------------------------
Emails, then phones, then names. Generated addresses embed the customer's name
(`wren.hollingsworth1881@nordpost.se`), so masking names first would rewrite the
address into `[CUSTOMER_NAME].[CUSTOMER_NAME]1881@nordpost.se` — no longer a
dictionary match, and left as a half-masked artefact that is worse than either
outcome. Longest term first within the name pass, for the same reason at word
scale: "Amara Abernathy" must be one token, not two.

Both the replacement tokens and the fallback patterns come from
docs/governance/pii_classification.yml. That file says it is the source of
truth; hardcoding `[CUSTOMER_NAME]` here would make that claim false.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from ..settings import project_root

# Categories in the order they are applied. Not alphabetical, not the order they
# appear in the YAML — see the module docstring.
CATEGORY_ORDER = ("email", "phone", "name")

# Manifest key per category. The manifest is written by the seed generator
# (src/meridian/seed/identity.py); these names are its interface.
MANIFEST_KEYS = {"email": "emails", "phone": "phones", "name": "names"}


@dataclass(frozen=True)
class MaskingPolicy:
    """The masking half of docs/governance/pii_classification.yml."""

    strategy: str
    dictionary_source: str
    replacements: dict[str, str]
    regex_fallback: dict[str, str]

    @classmethod
    def from_yaml(cls, path: Path | None = None) -> MaskingPolicy:
        path = path or (project_root() / "docs" / "governance" / "pii_classification.yml")
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        masking = doc.get("masking") or {}

        replacements = masking.get("replacements") or {}
        missing = [c for c in CATEGORY_ORDER if c not in replacements]
        if missing:
            raise ValueError(
                f"{path} defines no replacement token for {missing}. Every category "
                f"in {list(CATEGORY_ORDER)} needs one, or that class of PII is "
                f"silently not masked."
            )

        return cls(
            strategy=masking.get("strategy", "dictionary_then_regex"),
            dictionary_source=masking.get("dictionary_source", "seeds/known_pii_terms.json"),
            replacements=dict(replacements),
            regex_fallback=dict(masking.get("regex_fallback") or {}),
        )


@dataclass
class MaskReport:
    """Which pass caught what.

    Worth counting separately. If the regex pass is catching names the manifest
    should have held, the manifest is stale and the next unusual value will get
    through; if the dictionary is doing all the work, the regex is dead weight
    that still costs a pass over every chunk. Either way it is visible in the
    indexer's run log rather than something you find out by reading vectors.
    """

    dictionary: Counter = field(default_factory=Counter)
    regex: Counter = field(default_factory=Counter)

    @property
    def total(self) -> int:
        return sum(self.dictionary.values()) + sum(self.regex.values())

    def merge(self, other: MaskReport) -> None:
        self.dictionary.update(other.dictionary)
        self.regex.update(other.regex)

    def as_dict(self) -> dict:
        return {
            "dictionary": dict(self.dictionary),
            "regex": dict(self.regex),
            "total": self.total,
        }


class Masker:
    """Applies the policy. Construct once and reuse — compiling the dictionary
    into alternation patterns costs far more than masking a document with them.
    """

    def __init__(self, policy: MaskingPolicy, manifest: dict) -> None:
        self.policy = policy
        self._terms: dict[str, list[str]] = {}
        self._dict_patterns: dict[str, re.Pattern[str]] = {}
        self._regex_patterns: dict[str, re.Pattern[str]] = {}

        for category in CATEGORY_ORDER:
            terms = [t for t in manifest.get(MANIFEST_KEYS[category], []) if t]
            # Longest first so the most specific term wins: Python's alternation
            # is leftmost-first, not longest-match, so this ordering *is* the
            # longest-match rule. Ties broken alphabetically to keep the compiled
            # pattern stable across runs — an unstable pattern would make the
            # masked text, and therefore its content hash, non-deterministic.
            terms.sort(key=lambda t: (-len(t), t))
            self._terms[category] = terms
            if terms:
                # Lookarounds rather than \b. A term can begin with a non-word
                # character — every generated phone number starts with "+" —
                # and \b between a space and a "+" is not a boundary, so a
                # \b-anchored alternation silently never matches any phone.
                # That failure is invisible without MaskReport, because the
                # regex fallback quietly catches them and the output looks
                # correct; it was found by noticing phones scored as regex hits
                # when the manifest holds every one of them. These lookarounds
                # assert "not glued to a word character" instead, which is the
                # property actually wanted and holds whatever the term's edges
                # happen to be.
                self._dict_patterns[category] = re.compile(
                    r"(?<!\w)(?:" + "|".join(re.escape(t) for t in terms) + r")(?!\w)",
                    re.IGNORECASE,
                )
            pattern = policy.regex_fallback.get(category)
            if pattern:
                self._regex_patterns[category] = re.compile(pattern, re.IGNORECASE)

    # -- construction -------------------------------------------------------

    @classmethod
    def from_project(
        cls,
        manifest_path: Path | None = None,
        policy_path: Path | None = None,
    ) -> Masker:
        policy = MaskingPolicy.from_yaml(policy_path)
        path = manifest_path or (project_root() / policy.dictionary_source)
        if not path.is_file():
            raise FileNotFoundError(
                f"PII manifest not found at {path}. Run `make seed` first — masking "
                f"is dictionary-based and refuses to fall back to regex alone, "
                f"because a name that leaks into the index once is never "
                f"re-embedded (CONTRACTS.md §10)."
            )
        return cls(policy, json.loads(path.read_text(encoding="utf-8")))

    # -- masking ------------------------------------------------------------

    def mask(self, text: str) -> str:
        return self.mask_with_report(text)[0]

    def mask_with_report(self, text: str) -> tuple[str, MaskReport]:
        report = MaskReport()
        if not text:
            return text, report

        for category in CATEGORY_ORDER:
            token = self.policy.replacements[category]

            pattern = self._dict_patterns.get(category)
            if pattern is not None:
                text, n = pattern.subn(token, text)
                if n:
                    report.dictionary[category] += n

            fallback = self._regex_patterns.get(category)
            if fallback is not None:
                text, n = fallback.subn(token, text)
                if n:
                    report.regex[category] += n

        return text, report

    def mask_fields(self, row: dict, fields: tuple[str, ...]) -> tuple[dict, MaskReport]:
        """Mask named fields of a record, leaving everything else untouched."""
        report = MaskReport()
        out = dict(row)
        for name in fields:
            value = out.get(name)
            if isinstance(value, str) and value:
                out[name], sub = self.mask_with_report(value)
                report.merge(sub)
        return out, report

    # -- verification -------------------------------------------------------

    def manifest_terms(self, category: str | None = None) -> list[str]:
        if category is None:
            return [t for c in CATEGORY_ORDER for t in self._terms[c]]
        return list(self._terms[category])

    def find_leaks(self, text: str, *, limit: int = 20) -> list[str]:
        """Manifest terms still present in `text`.

        Deliberately implemented as a plain case-folded substring scan rather
        than by reusing the alternation patterns above. A verifier built from
        the same regex as the masker can only ever prove the masker is
        idempotent; substring containment is a strictly stronger condition than
        the word-boundary match masking performs, so a text that passes this
        has no manifest term in it under any tokenisation.
        """
        if not text:
            return []
        haystack = text.casefold()
        found: list[str] = []
        for category in CATEGORY_ORDER:
            for term in self._terms[category]:
                if term.casefold() in haystack:
                    found.append(term)
                    if len(found) >= limit:
                        return found
        return found
