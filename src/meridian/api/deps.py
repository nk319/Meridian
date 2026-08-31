"""Connections, the retriever, and who is allowed to call what.

**The role a handler connects as is the security boundary, and it is chosen
here.** `meridian_app` for the ticket endpoints (CRUD on `oltp`, nothing in the
warehouse); `analytics_ro` for `/v1/ai/*` (SELECT on `gold`, `rag` and `meta`,
nothing on `secure`, `oltp` or `silver`). A handler cannot pick its own — the
dependency does — which is what makes CONTRACTS.md §1 a property of the process
rather than of whoever writes the next endpoint.

**The retriever is a singleton, built once at startup.** Constructing one loads
a 67 MB ONNX model; doing it per request would put three seconds of model load
in front of every search. It is created in the app's lifespan and torn down
with it.

**Connections are per-request, from a small pool.** psycopg connections are not
thread-safe, and FastAPI runs sync handlers in a thread pool — so a module-level
connection shared across handlers is a race that shows up under load as
`InFailedSqlTransaction` on an unrelated query. The pool hands each request its
own and takes it back afterwards.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Annotated

from fastapi import Depends, Header, HTTPException, status

from ..db import UpstreamUnavailable, connect
from ..settings import settings
from .security import (
    SCOPE_AI,
    SCOPE_TICKETS_READ,
    SCOPE_TICKETS_WRITE,
    AuthError,
    Principal,
    decode_token,
    verify_ingest_key,
)

# Populated by the app's lifespan. Module-level rather than passed through every
# signature because FastAPI's dependency system has no other place to put a
# process-wide resource, and the alternative is a global by another name.
_state: dict = {}


def set_retriever(retriever) -> None:
    _state["retriever"] = retriever


def get_retriever():
    retriever = _state.get("retriever")
    if retriever is None:
        # 503, not 500. The corpus not being indexed is an operational state
        # with an operator action attached to it, not a bug in this process.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "retrieval is unavailable — the vector store is empty or "
                "unreachable. Run `make rag-index`."
            ),
        )
    return retriever


def _connection(role: str, dbname: str | None = None) -> Iterator:
    try:
        conn = connect(role, dbname, vectors=(role == "analytics_ro"))
    except UpstreamUnavailable as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"database unavailable: {exc}",
        ) from exc
    try:
        yield conn
    finally:
        # Rolling back before returning the connection matters more than it
        # looks: a handler that raised mid-transaction leaves it aborted, and
        # the next request to get that connection fails on a statement that has
        # nothing to do with the problem.
        try:
            conn.rollback()
        finally:
            conn.close()


def oltp_connection() -> Iterator:
    """As `meridian_app`, against the `oltp` database.

    The database has to be named. `connect()` defaults to `warehouse`, and
    `meridian_app` has no CONNECT privilege there — so omitting it produces
    `permission denied for database "warehouse"` on the first ticket request.
    That is the grant boundary working, and it is worth naming here because the
    error mentions a database this endpoint has no business touching, which
    reads like a configuration problem rather than the correct refusal it is.
    """
    yield from _connection("meridian_app", settings().oltp_db)


def warehouse_connection() -> Iterator:
    """As `analytics_ro`. Cannot read `secure`, `oltp` or `silver` — by grant."""
    yield from _connection("analytics_ro")


# ---------------------------------------------------------------------------
# auth
# ---------------------------------------------------------------------------


def _unauthorised(detail: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=detail,
        # Required by RFC 6750 for a 401 on a bearer-token resource. Omitting
        # it is the difference between a client that knows how to retry and one
        # that guesses.
        headers={"WWW-Authenticate": "Bearer"},
    )


def current_principal(authorization: Annotated[str | None, Header()] = None) -> Principal:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise _unauthorised("missing bearer token")
    try:
        return decode_token(authorization.split(" ", 1)[1].strip())
    except AuthError as exc:
        raise _unauthorised(str(exc)) from exc


def _require(scope: str):
    def dependency(principal: Annotated[Principal, Depends(current_principal)]) -> Principal:
        try:
            principal.requires(scope)
        except AuthError as exc:
            # 403, not 401: the caller authenticated successfully and is not
            # permitted. Returning 401 here would send a well-behaved client
            # off to refresh a token that was never the problem.
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
        return principal

    return dependency


require_tickets_read = _require(SCOPE_TICKETS_READ)
require_tickets_write = _require(SCOPE_TICKETS_WRITE)
require_ai = _require(SCOPE_AI)


def ingest_principal(x_ingest_key: Annotated[str | None, Header()] = None) -> Principal:
    """The machine-to-machine path. A static key, compared in constant time."""
    try:
        return verify_ingest_key(x_ingest_key)
    except AuthError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(exc),
            headers={"WWW-Authenticate": 'ApiKey realm="meridian"'},
        ) from exc


def ingest_or_token(
    x_ingest_key: Annotated[str | None, Header()] = None,
    authorization: Annotated[str | None, Header()] = None,
) -> Principal:
    """Either credential, for the one endpoint both kinds of caller use.

    Writing a ticket is done by the ticketing system with a long-lived key and
    by a support agent with a token, and both are legitimate. The key is tried
    first because it is the common case and the cheaper check.
    """
    if x_ingest_key:
        return ingest_principal(x_ingest_key)
    principal = current_principal(authorization)
    try:
        principal.requires(SCOPE_TICKETS_WRITE)
    except AuthError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    return principal
