"""Masking rules, unit level.

These pin the properties the corpus-wide test in test_pii_manifest.py cannot
distinguish. That test asks "did anything leak"; a masker with the passes in the
wrong order, or with a broken dictionary silently covered by the regex fallback,
can answer no. These ask how the result was reached.
"""

from __future__ import annotations

import pytest

from meridian.rag.masking import CATEGORY_ORDER, Masker, MaskingPolicy


@pytest.fixture(scope="module")
def policy() -> MaskingPolicy:
    return MaskingPolicy.from_yaml()


def test_replacement_tokens_come_from_the_governance_file(policy):
    """pii_classification.yml calls itself the source of truth.

    Hardcoding `[CUSTOMER_NAME]` in masking.py would make that false while
    leaving every other test green.
    """
    assert policy.replacements["name"] == "[CUSTOMER_NAME]"
    assert policy.replacements["email"] == "[EMAIL]"
    assert policy.replacements["phone"] == "[PHONE]"
    assert policy.strategy == "dictionary_then_regex"


def test_policy_rejects_a_category_with_no_replacement(tmp_path):
    """A missing token must fail loudly. Left alone, that class of PII is simply
    not masked and nothing says so."""
    path = tmp_path / "pii.yml"
    path.write_text(
        "masking:\n  strategy: dictionary_then_regex\n  replacements:\n    email: '[EMAIL]'\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="replacement token"):
        MaskingPolicy.from_yaml(path)


def test_email_is_masked_before_name(policy):
    """Generated addresses embed the customer's name.

    Masking names first rewrites `wren.hollingsworth@x.com` into
    `[CUSTOMER_NAME].[CUSTOMER_NAME]@x.com`, which is no longer a dictionary
    match for the address and leaves a half-masked artefact behind.
    """
    m = Masker(
        policy,
        {
            "names": ["Wren Hollingsworth", "Wren", "Hollingsworth"],
            "emails": ["wren.hollingsworth@nordpost.se"],
            "phones": [],
        },
    )
    out = m.mask("Contact wren.hollingsworth@nordpost.se about it.")
    assert out == "Contact [EMAIL] about it."
    assert "[CUSTOMER_NAME]" not in out


def test_longest_term_wins(policy):
    """`re` alternation is leftmost-first, not longest-match, so ordering the
    terms by length is what makes a full name one token rather than two."""
    m = Masker(
        policy, {"names": ["Amara", "Abernathy", "Amara Abernathy"], "emails": [], "phones": []}
    )
    assert m.mask("Regards, Amara Abernathy") == "Regards, [CUSTOMER_NAME]"


def test_terms_starting_with_a_non_word_character_are_dictionary_matched(policy):
    """Regression: every generated phone starts with '+'.

    \\b between a space and a '+' is not a word boundary, so a \\b-anchored
    alternation never matched a single phone number. The regex fallback caught
    them and the output looked correct — only the per-pass attribution showed
    the dictionary doing nothing.
    """
    m = Masker(policy, {"names": [], "emails": [], "phones": ["+1-200-579-8987"]})
    out, report = m.mask_with_report("Phone is +1-200-579-8987 if faster.")
    assert out == "Phone is [PHONE] if faster."
    assert report.dictionary["phone"] == 1
    assert report.regex.get("phone", 0) == 0


def test_masking_respects_word_boundaries(policy):
    m = Masker(policy, {"names": ["Wren"], "emails": [], "phones": []})
    assert m.mask("Wren asked") == "[CUSTOMER_NAME] asked"
    # Not glued inside a longer word.
    assert m.mask("Wrenches and screwdrivers") == "Wrenches and screwdrivers"


def test_regex_is_a_fallback_not_the_mechanism(policy):
    """An address the manifest never saw is still caught, and is attributed to
    the regex pass so a stale manifest is visible rather than merely survivable."""
    m = Masker(policy, {"names": [], "emails": ["known@x.com"], "phones": []})
    out, report = m.mask_with_report("Write to stranger@elsewhere.org please.")
    assert out == "Write to [EMAIL] please."
    assert report.regex["email"] == 1
    assert report.dictionary.get("email", 0) == 0


def test_business_identifiers_survive_masking(masker):
    """Order numbers, amounts and timestamps carry the retrieval signal.

    The phone regex is permissive by design, and a pattern that ate order
    numbers or timestamps would destroy exactly the literals BM25 depends on
    while still reporting zero leaks.
    """
    text = (
        "Order O0010507 placed 2024-09-11T13:43:00+00:00 for $1,234.56, "
        "item P00042, ticket T000226."
    )
    out = masker.mask(text)
    for literal in ("O0010507", "2024-09-11T13:43:00+00:00", "$1,234.56", "P00042", "T000226"):
        assert literal in out, f"masking destroyed {literal!r}: {out}"


def test_find_leaks_actually_finds_a_leak(masker):
    """The verifier must be able to fail. A `find_leaks` that returned [] for
    everything would make the headline PII test pass unconditionally."""
    name = masker.manifest_terms("name")[0]
    leaks = masker.find_leaks(f"Hello {name}, about your order")
    # A full name contains its own parts, and the manifest holds all three, so
    # the scan reports every one. That breadth is the intended behaviour of a
    # substring check — the assertion is that the term is caught, not that it is
    # caught exactly once.
    assert name in leaks
    assert masker.find_leaks("Hello, about your order") == []


def test_category_order_is_the_documented_one():
    assert CATEGORY_ORDER == ("email", "phone", "name")
