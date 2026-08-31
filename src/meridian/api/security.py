"""Two credentials, for two different kinds of caller.

**A JWT for humans and interactive clients.** Short-lived, signed with
`API_JWT_SECRET`, carrying a subject and a set of scopes. Scopes rather than a
bare "authenticated" flag because the API has one genuinely privileged
operation — asking the model a question costs money — and an authorisation model
that cannot express "may read tickets but may not spend tokens" would force that
distinction into a second service.

**A static ingest key for machines.** `POST /v1/support/tickets` is called by
the ticketing system, not by a person, and a machine-to-machine caller that has
to refresh a token every fifteen minutes is a machine that will eventually fail
to. CONTRACTS.md's `.env` carries `API_INGEST_KEY` for exactly this.

Both comparisons use `hmac.compare_digest`. A plain `==` on a secret leaks its
length and, on a long enough sample, its prefix, through timing — a real attack
on a network-exposed comparison and free to avoid.

Passwords are hashed with `hashlib.scrypt`, from the standard library. bcrypt or
argon2 would be better and both are dependencies; scrypt is memory-hard, in
Python since 3.6, and correct here. The demo users are seeded with a per-user
salt rather than a shared one, because a shared salt makes one rainbow table
work on every account.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import os
import secrets
from dataclasses import dataclass

ALGORITHM = "HS256"

# What a token may do. Deliberately coarse — three scopes, not fifteen — because
# a permission model nobody can hold in their head gets bypassed with a wildcard.
SCOPE_TICKETS_READ = "tickets:read"
SCOPE_TICKETS_WRITE = "tickets:write"
SCOPE_AI = "ai:query"
ALL_SCOPES = (SCOPE_TICKETS_READ, SCOPE_TICKETS_WRITE, SCOPE_AI)


class AuthError(Exception):
    """Any credential problem. Deliberately one type.

    The handler turns every instance into the same 401 with the same body.
    Distinguishing "no such user" from "wrong password" in the response is an
    account-enumeration oracle, and it is the kind that gets added for
    debugging and never removed.
    """


@dataclass(frozen=True)
class Principal:
    subject: str
    scopes: tuple[str, ...]
    kind: str  # "user" | "service"

    def requires(self, scope: str) -> None:
        if scope not in self.scopes:
            raise AuthError(f"token lacks the {scope!r} scope")


def _secret() -> str:
    secret = os.environ.get("API_JWT_SECRET", "")
    if not secret:
        # Refuse rather than generate one. A per-process random secret would
        # make the API "work" while invalidating every token on restart and
        # silently accepting nothing across two workers — a failure that looks
        # like a client bug.
        raise AuthError("API_JWT_SECRET is not set; the API cannot issue or verify tokens")
    return secret


def _expiry_minutes() -> int:
    try:
        return int(os.environ.get("API_JWT_EXPIRY_MINUTES", "60"))
    except ValueError:
        return 60


def hash_password(password: str, salt: bytes | None = None) -> str:
    """scrypt, with a per-user salt, stored as `salt$hash` in hex."""
    salt = salt or secrets.token_bytes(16)
    digest = hashlib.scrypt(password.encode("utf-8"), salt=salt, n=2**14, r=8, p=1, dklen=32)
    return f"{salt.hex()}${digest.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        salt_hex, digest_hex = stored.split("$", 1)
        salt = bytes.fromhex(salt_hex)
    except ValueError:
        return False
    candidate = hashlib.scrypt(password.encode("utf-8"), salt=salt, n=2**14, r=8, p=1, dklen=32)
    return hmac.compare_digest(candidate.hex(), digest_hex)


def issue_token(subject: str, scopes: tuple[str, ...]) -> tuple[str, int]:
    """Return (token, seconds until expiry)."""
    import jwt

    minutes = _expiry_minutes()
    now = dt.datetime.now(dt.UTC)
    payload = {
        "sub": subject,
        "scopes": list(scopes),
        "iat": now,
        "exp": now + dt.timedelta(minutes=minutes),
        # An audience and issuer, checked on the way back in. Without them a
        # token minted by any other service sharing this secret would be
        # accepted here, which is how one leaked secret becomes several
        # compromised services.
        "iss": "meridian-api",
        "aud": "meridian",
    }
    return jwt.encode(payload, _secret(), algorithm=ALGORITHM), minutes * 60


def decode_token(token: str) -> Principal:
    import jwt

    try:
        payload = jwt.decode(
            token,
            _secret(),
            # A list, and never `algorithms=None`. Accepting the algorithm the
            # *token* names is the classic JWT vulnerability: a token with
            # `alg: none` verifies against anything, and one with `alg: HS256`
            # against a service expecting RS256 lets the public key be used as
            # the HMAC secret.
            algorithms=[ALGORITHM],
            audience="meridian",
            issuer="meridian-api",
        )
    except jwt.ExpiredSignatureError as exc:
        raise AuthError("token has expired") from exc
    except jwt.InvalidTokenError as exc:
        raise AuthError(f"invalid token: {exc}") from exc

    subject = payload.get("sub")
    if not subject:
        raise AuthError("token carries no subject")

    scopes = tuple(payload.get("scopes") or ())
    unknown = set(scopes) - set(ALL_SCOPES)
    if unknown:
        raise AuthError(f"token carries unknown scopes: {sorted(unknown)}")

    return Principal(subject=subject, scopes=scopes, kind="user")


def verify_ingest_key(presented: str | None) -> Principal:
    expected = os.environ.get("API_INGEST_KEY", "")
    if not expected:
        raise AuthError("API_INGEST_KEY is not set; the ingest endpoint is closed")
    if not presented or not hmac.compare_digest(presented, expected):
        raise AuthError("invalid ingest key")
    return Principal(
        subject="ingest",
        # The narrowest set that does the job. An ingest key is long-lived and
        # lives in a config file somewhere, so it must not also be able to read
        # the corpus or spend model tokens.
        scopes=(SCOPE_TICKETS_WRITE,),
        kind="service",
    )
