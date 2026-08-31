#!/usr/bin/env python3
"""Write a `.env` from `.env.example` with real secrets in place of the placeholders.

Thirteen values in the template have to be replaced before anything starts, and
filling them in by hand is the first thing a person does after cloning. It is
also the step most likely to be done badly — the failure mode is not an error,
it is a stack that comes up perfectly on a password somebody published in a
public template.

**Substitution is in place, never appended.** That distinction is the whole
reason this is a script rather than a `cat >> .env`. `settings.load_dotenv` is
first-wins — it assigns a key only `if override or key not in os.environ`, so
once a key is set the later duplicates are skipped. Appending overrides after a
copy of the template therefore produces a file where the *placeholder* wins.
CI gets away with exactly that shape only because it then exports a deduplicated
last-wins copy into `$GITHUB_ENV`, and real environment variables beat the file.
Nothing on a developer machine does that, and `change_me_locally` is a *set*
value, so no check anywhere would refuse it.

Placeholders are matched by their value, not by a list of key names. A list only
ever covers the secrets somebody remembered, which are by definition not the
ones that go missing — the same argument the packaging test makes, for the same
reason. Add a secret to `.env.example` with one of the placeholder values below
and it is generated here with no change to this file.
"""

from __future__ import annotations

import argparse
import base64
import os
import secrets
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / ".env.example"
TARGET = ROOT / ".env"

# Every placeholder value that appears in .env.example, mapped to how to
# replace it. `fernet` is separate because Airflow requires a 32-byte urlsafe
# base64 key specifically, not any random string.
PLACEHOLDERS = {
    "change_me_locally": "token",
    "generate-with-openssl-rand-base64-32": "token",
    "generate_a_random_value_locally": "token",
    "set-a-password-here": "token",
}

# The Fernet placeholder embeds the command to generate it, spaces and all, so
# it is matched by prefix rather than by equality.
FERNET_PREFIX = "generate_with__python"


def _token() -> str:
    """32 URL-safe characters.

    Comfortably over the 32-byte floor RFC 7518 §3.2 sets for HS256, which is
    what API_JWT_SECRET is used for — PyJWT warns below it, and a warning in a
    log nobody reads is not a control.
    """
    return secrets.token_urlsafe(32)[:43]


def _fernet() -> str:
    return base64.urlsafe_b64encode(os.urandom(32)).decode()


def render(template: str) -> tuple[str, int]:
    """Return the template with placeholders replaced, and how many were."""
    out: list[str] = []
    replaced = 0
    for raw in template.splitlines():
        line = raw.rstrip("\n")
        stripped = line.strip()
        if stripped.startswith("#") or "=" not in stripped:
            out.append(line)
            continue

        key, _, value = line.partition("=")
        value = value.strip()

        if value in PLACEHOLDERS:
            out.append(f"{key}={_token()}")
            replaced += 1
        elif value.startswith(FERNET_PREFIX):
            out.append(f"{key}={_fernet()}")
            replaced += 1
        else:
            out.append(line)

    return "\n".join(out) + "\n", replaced


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--force",
        action="store_true",
        help="overwrite an existing .env — this rotates every secret, so a "
        "running stack will no longer accept them",
    )
    args = parser.parse_args()

    if not TEMPLATE.is_file():
        print(f"no template at {TEMPLATE}", file=sys.stderr)
        return 1

    if TARGET.exists() and not args.force:
        print(
            f"{TARGET.name} already exists — leaving it alone.\n"
            "Pass --force to regenerate, but note that rotating the passwords "
            "does not rotate them inside a database that has already been "
            "initialised: run `make reset` afterwards, or the roles will no "
            "longer authenticate.",
            file=sys.stderr,
        )
        return 1

    rendered, replaced = render(TEMPLATE.read_text(encoding="utf-8"))

    # Refuse to write a file that still has a placeholder in it. If the template
    # grows a secret whose placeholder is not one of the values above, that is a
    # silent hole, and this is the check that turns it into a failure.
    leftovers = [
        line.partition("=")[0]
        for line in rendered.splitlines()
        if not line.strip().startswith("#")
        and "=" in line
        and (
            line.partition("=")[2].strip() in PLACEHOLDERS
            or line.partition("=")[2].strip().startswith(FERNET_PREFIX)
        )
    ]
    if leftovers:
        print(f"unreplaced placeholders remain: {leftovers}", file=sys.stderr)
        return 1

    TARGET.write_text(rendered, encoding="utf-8")
    TARGET.chmod(0o600)

    print(f"wrote {TARGET.name} with {replaced} generated secrets (mode 600)")
    print("ANTHROPIC_API_KEY is left empty on purpose — the AI layer degrades")
    print("to extractive answers without it rather than failing.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
