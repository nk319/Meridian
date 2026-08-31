"""Issue tokens.

Demo users, seeded from the environment rather than from a table. There is no
`users` table in CONTRACTS.md §1 and adding one would be inventing schema the
contract does not have — this is a data platform whose API needs *an*
authentication story, not an identity provider.

`API_DEMO_USERS` is `name:password:scope,scope;name:password:scope`. Absent, a
single `analyst` account is created with a password from `API_DEMO_PASSWORD`,
and if that is missing too the endpoint refuses to issue anything rather than
falling back to a default credential. A hardcoded password in a repository is
worse than no authentication, because it looks like authentication.
"""

from __future__ import annotations

import os
from functools import cache

from fastapi import APIRouter, HTTPException, status

from ..models import TokenRequest, TokenResponse
from ..security import (
    ALL_SCOPES,
    SCOPE_AI,
    SCOPE_TICKETS_READ,
    SCOPE_TICKETS_WRITE,
    AuthError,
    hash_password,
    issue_token,
    verify_password,
)

router = APIRouter(prefix="/v1/auth", tags=["auth"])

DEFAULT_SCOPES = (SCOPE_TICKETS_READ, SCOPE_TICKETS_WRITE, SCOPE_AI)


@cache
def _users() -> dict[str, tuple[str, tuple[str, ...]]]:
    """{username: (stored_hash, scopes)}. Parsed once."""
    raw = os.environ.get("API_DEMO_USERS", "").strip()
    users: dict[str, tuple[str, tuple[str, ...]]] = {}

    if raw:
        for entry in raw.split(";"):
            if not entry.strip():
                continue
            parts = entry.split(":")
            name, password = parts[0].strip(), parts[1]
            scopes = (
                tuple(s.strip() for s in parts[2].split(",") if s.strip())
                if len(parts) > 2
                else DEFAULT_SCOPES
            )
            users[name] = (hash_password(password), scopes)
        return users

    password = os.environ.get("API_DEMO_PASSWORD", "")
    if password:
        users["analyst"] = (hash_password(password), DEFAULT_SCOPES)
    return users


@router.post("/token", response_model=TokenResponse)
def token(request: TokenRequest) -> TokenResponse:
    users = _users()
    if not users:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "no API users are configured. Set API_DEMO_PASSWORD or "
                "API_DEMO_USERS. There is deliberately no default credential."
            ),
        )

    record = users.get(request.username)
    # The password is verified even when the user does not exist, against a
    # throwaway hash. Returning early would make a missing user measurably
    # faster than a wrong password, which is a username oracle you get for free
    # by not thinking about it.
    stored, scopes = record if record else (hash_password("no-such-user"), ())
    if not verify_password(request.password, stored) or not record:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    granted = scopes
    if request.scopes:
        unknown = set(request.scopes) - set(ALL_SCOPES)
        if unknown:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"unknown scopes: {sorted(unknown)}",
            )
        # Intersection, never union. A client may ask for *less* than it holds
        # — which is how a read-only client avoids carrying a token that can
        # spend model tokens — and asking for more is silently narrowed rather
        # than granted.
        granted = tuple(s for s in scopes if s in set(request.scopes))
        if not granted:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"{request.username} holds none of the requested scopes",
            )

    try:
        access_token, expires_in = issue_token(request.username, granted)
    except AuthError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)
        ) from exc

    return TokenResponse(access_token=access_token, expires_in=expires_in, scopes=list(granted))
