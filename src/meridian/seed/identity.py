"""Customer identities and the PII manifest.

Every name, email and phone number this module invents is recorded in a manifest
written to seeds/known_pii_terms.json.

That manifest is what makes AI-first sequencing safe. The RAG index is built in
Phase 1, before dim_customer exists, so there is no warehouse to look up real
values from — masking would fall back to regex alone. Regex misses unusual names,
and because the indexer skips chunks whose content hash is unchanged, anything
that leaks on the first pass is never re-embedded. Dictionary masking from a
manifest removes that failure mode entirely.
"""

from __future__ import annotations

import random

FIRST_NAMES = [
    "Amara", "Benedikt", "Camille", "Dmitri", "Elowen", "Farrah", "Gideon", "Halina",
    "Ibrahim", "Juno", "Kwame", "Liesel", "Mateo", "Nadia", "Oskar", "Priya",
    "Quentin", "Rosalind", "Soren", "Tamsin", "Ulises", "Verity", "Wren", "Xiomara",
    "Yannick", "Zola", "Anouk", "Bastien", "Cordelia", "Dashiell", "Esme", "Florian",
    "Greta", "Hakim", "Ingrid", "Jasper", "Kenji", "Lucia", "Milo", "Nerys",
    "Ottoline", "Pascal", "Rafferty", "Saoirse", "Tobias", "Ursula", "Viggo", "Willa",
]

LAST_NAMES = [
    "Ashworth", "Brennan", "Calloway", "Duarte", "Eskildsen", "Fontaine", "Gallardo",
    "Hollingsworth", "Iversen", "Jankowski", "Kowalczyk", "Lindqvist", "Marchetti",
    "Nakamura", "Oyelaran", "Petrossian", "Quintanilla", "Ravensworth", "Stavropoulos",
    "Thackeray", "Ubiratan", "Vasquez", "Whitmore", "Xanthopoulos", "Yamamoto",
    "Zieliński", "Abernathy", "Bergström", "Castellanos", "Delacroix", "Engelhardt",
    "Fairweather", "Grimaldi", "Haverford", "Ionescu", "Jörgensen",
]

EMAIL_DOMAINS = ["mailhaven.com", "postbox.io", "swiftmail.net", "corrida.org", "nordpost.se"]

CITIES = [
    ("Portland", "US"), ("Austin", "US"), ("Rotterdam", "NL"), ("Lyon", "FR"),
    ("Gothenburg", "SE"), ("Porto", "PT"), ("Kraków", "PL"), ("Bristol", "GB"),
    ("Hamilton", "NZ"), ("Valparaíso", "CL"), ("Tallinn", "EE"), ("Ghent", "BE"),
]


class PIIRegistry:
    """Collects every generated PII value so it can be masked by dictionary."""

    def __init__(self) -> None:
        self.names: set[str] = set()
        self.emails: set[str] = set()
        self.phones: set[str] = set()

    def record(self, first: str, last: str, email: str, phone: str) -> None:
        # Both the full name and each part: ticket text uses "Hi, this is Amara"
        # as often as it uses the full name, and a manifest that only holds full
        # names would miss those.
        self.names.update({f"{first} {last}", first, last})
        self.emails.add(email)
        self.phones.add(phone)

    def as_manifest(self) -> dict:
        return {
            "names": sorted(self.names),
            "emails": sorted(self.emails),
            "phones": sorted(self.phones),
            "counts": {
                "names": len(self.names),
                "emails": len(self.emails),
                "phones": len(self.phones),
            },
        }


def make_customers(rng: random.Random, n: int, registry: PIIRegistry) -> list[dict]:
    """Generate n customers with stable ids C000001..C00000n."""
    customers = []
    seen_emails: set[str] = set()

    for i in range(1, n + 1):
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        city, country = rng.choice(CITIES)

        # Emails must be unique — a duplicate would make the natural key ambiguous
        # and quietly break dedup downstream.
        base = f"{first.lower()}.{last.lower()}".replace("ł", "l").replace("ö", "o")
        email = f"{base}@{rng.choice(EMAIL_DOMAINS)}"
        if email in seen_emails:
            email = f"{base}{i}@{rng.choice(EMAIL_DOMAINS)}"
        seen_emails.add(email)

        phone = f"+1-{rng.randint(200, 989)}-{rng.randint(200, 989)}-{rng.randint(1000, 9999)}"

        registry.record(first, last, email, phone)

        customers.append(
            {
                "customer_id": f"C{i:06d}",
                "first_name": first,
                "last_name": last,
                "email": email,
                "phone": phone,
                "city": city,
                "country": country,
            }
        )

    return customers
