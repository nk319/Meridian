"""The API: authentication, authorisation, and the role boundary.

Two groups of test, split by what they need.

The **auth** tests need nothing but the repository — JWT signing and
verification, scope narrowing, constant-time key comparison — so they run in CI.
They are also where the security properties live, and every one of them
corresponds to a real vulnerability class rather than to a line of coverage.

The **endpoint** tests need Postgres, and skip without it. What they assert is
not "FastAPI routes requests" but the things this API adds on top of that: that
the ticket handlers connect as `meridian_app` and the AI handlers as
`analytics_ro`, that keyset pagination does not repeat or drop rows, and that
the ground-truth columns cannot be written by a caller.
"""

from __future__ import annotations

import os

import pytest

from meridian.api import security

# ---------------------------------------------------------------------------
# Auth — no database
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _jwt_secret(monkeypatch):
    """A known secret, long enough for HS256.

    32 bytes because PyJWT warns below that and is right — RFC 7518 §3.2 sets
    the minimum HMAC key length for SHA-256 at the hash output size. The repo's
    own `.env` was 24 bytes until the library said so.
    """
    monkeypatch.setenv("API_JWT_SECRET", "x" * 32)
    monkeypatch.setenv("API_JWT_EXPIRY_MINUTES", "60")


def test_a_token_round_trips_with_its_scopes():
    token, expires_in = security.issue_token("alice", (security.SCOPE_AI,))
    principal = security.decode_token(token)
    assert principal.subject == "alice"
    assert principal.scopes == (security.SCOPE_AI,)
    assert expires_in == 3600


def test_a_token_signed_with_another_secret_is_rejected(monkeypatch):
    token, _ = security.issue_token("alice", (security.SCOPE_AI,))
    monkeypatch.setenv("API_JWT_SECRET", "y" * 32)
    with pytest.raises(security.AuthError, match="invalid token"):
        security.decode_token(token)


def test_an_expired_token_is_rejected(monkeypatch):
    monkeypatch.setenv("API_JWT_EXPIRY_MINUTES", "-1")
    token, _ = security.issue_token("alice", (security.SCOPE_AI,))
    with pytest.raises(security.AuthError, match="expired"):
        security.decode_token(token)


def test_an_alg_none_token_is_rejected():
    """The textbook JWT vulnerability.

    A token declaring `alg: none` carries no signature, and a verifier that
    trusts the algorithm the *token* names accepts it — so anyone can mint an
    admin token with a text editor. `decode_token` passes an explicit
    `algorithms=[...]` list, which is the fix; this is the test that keeps it.
    """
    import jwt

    forged = jwt.encode(
        {
            "sub": "attacker",
            "scopes": list(security.ALL_SCOPES),
            "iss": "meridian-api",
            "aud": "meridian",
        },
        key="",
        algorithm="none",
    )
    with pytest.raises(security.AuthError):
        security.decode_token(forged)


def test_a_token_for_another_audience_is_rejected():
    """One leaked secret must not compromise every service sharing it.

    A token minted by a sibling service with the same signing key verifies
    cryptographically. The `aud` and `iss` claims are what make it fail here
    anyway.
    """
    import jwt

    other = jwt.encode(
        {"sub": "alice", "scopes": [], "iss": "somewhere-else", "aud": "other-service"},
        key="x" * 32,
        algorithm="HS256",
    )
    with pytest.raises(security.AuthError):
        security.decode_token(other)


def test_a_token_carrying_an_invented_scope_is_rejected():
    """Scopes are a closed set. An unknown one is a forged or stale token."""
    import jwt

    token = jwt.encode(
        {"sub": "alice", "scopes": ["admin:everything"], "iss": "meridian-api", "aud": "meridian"},
        key="x" * 32,
        algorithm="HS256",
    )
    with pytest.raises(security.AuthError, match="unknown scopes"):
        security.decode_token(token)


def test_a_missing_secret_refuses_rather_than_inventing_one():
    """Generating a per-process secret would be worse than failing.

    The API would appear to work, invalidate every token on restart, and — with
    two workers — accept tokens from one and reject them from the other. That
    reads as a client bug for as long as it takes somebody to find this line.
    """
    original = os.environ.pop("API_JWT_SECRET", None)
    try:
        with pytest.raises(security.AuthError, match="not set"):
            security.issue_token("alice", ())
    finally:
        if original is not None:
            os.environ["API_JWT_SECRET"] = original


def test_passwords_are_salted_per_user():
    """A shared salt makes one rainbow table work on every account."""
    first = security.hash_password("hunter2")
    second = security.hash_password("hunter2")
    assert first != second, "two hashes of one password are identical — the salt is shared"
    assert security.verify_password("hunter2", first)
    assert security.verify_password("hunter2", second)
    assert not security.verify_password("hunter3", first)


def test_a_malformed_stored_hash_fails_closed():
    assert not security.verify_password("anything", "not-a-valid-record")


def test_the_ingest_key_grants_write_and_nothing_else(monkeypatch):
    """A long-lived key in a config file must be the narrowest one that works.

    It must not also be able to read the corpus or spend model tokens — those
    are the two things worth stealing it for.
    """
    monkeypatch.setenv("API_INGEST_KEY", "s3cret-key")
    principal = security.verify_ingest_key("s3cret-key")
    assert principal.scopes == (security.SCOPE_TICKETS_WRITE,)
    assert security.SCOPE_AI not in principal.scopes
    assert security.SCOPE_TICKETS_READ not in principal.scopes

    with pytest.raises(security.AuthError):
        security.verify_ingest_key("s3cret-keY")
    with pytest.raises(security.AuthError):
        security.verify_ingest_key(None)


def test_an_unset_ingest_key_closes_the_endpoint(monkeypatch):
    """Absent configuration must not mean absent authentication."""
    monkeypatch.delenv("API_INGEST_KEY", raising=False)
    with pytest.raises(security.AuthError, match="not set"):
        security.verify_ingest_key("anything")


# ---------------------------------------------------------------------------
# Endpoints — Postgres required
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def client():
    from meridian.db import server_reachable

    if not server_reachable():
        pytest.skip("Postgres not reachable — run `make up`")

    from fastapi.testclient import TestClient

    from meridian.api.main import app

    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def auth(client):
    token, _ = security.issue_token("test", security.ALL_SCOPES)
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def a_ticket(client, auth):
    page = client.get("/v1/support/tickets", params={"limit": 1}, headers=auth)
    if page.status_code != 200 or not page.json()["items"]:
        pytest.skip("no tickets in oltp — run `make load-oltp`")
    return page.json()["items"][0]


def test_liveness_does_not_touch_the_database(client):
    """A `/health` that queries Postgres reports the database's outage as this
    process's, and an orchestrator answers by restarting a working API —
    repeatedly, for as long as the database is down."""
    body = client.get("/health").json()
    assert body["status"] == "ok"
    assert set(body["checks"]) == {"process"}


def test_readiness_reports_each_dependency_separately(client):
    checks = client.get("/ready").json()["checks"]
    assert set(checks) >= {"postgres", "retriever", "anthropic_key"}


def test_every_protected_route_refuses_an_anonymous_caller(client):
    for method, path in (
        ("GET", "/v1/support/tickets"),
        ("GET", "/v1/support/tickets/T000001"),
        ("PATCH", "/v1/support/tickets/T000001"),
        ("GET", "/v1/ai/search?q=anything"),
        ("POST", "/v1/ai/ask"),
    ):
        response = client.request(method, path, json={})
        assert response.status_code == 401, f"{method} {path} returned {response.status_code}"


def test_a_narrow_token_is_forbidden_rather_than_unauthenticated(client):
    """403, not 401.

    The caller authenticated fine and lacks a scope. Answering 401 sends a
    well-behaved client off to refresh a token that was never the problem, and
    it will come back with the same one.
    """
    token, _ = security.issue_token("reader", (security.SCOPE_TICKETS_READ,))
    headers = {"Authorization": f"Bearer {token}"}
    assert (
        client.get("/v1/support/tickets", params={"limit": 1}, headers=headers).status_code == 200
    )
    assert client.get("/v1/ai/search", params={"q": "anything"}, headers=headers).status_code == 403


def test_pagination_neither_repeats_nor_drops_a_row(client, auth):
    """The property offset pagination does not have.

    `created_ts` is not unique — the generator emits several tickets a minute —
    so a cursor on it alone loses every row sharing the boundary value. The
    tie-break on `ticket_id` is what makes the page boundary exact, and this is
    the test that would catch its removal.
    """
    seen: list[str] = []
    cursor = None
    for _ in range(6):
        params = {"limit": 7}
        if cursor:
            params["cursor"] = cursor
        page = client.get("/v1/support/tickets", params=params, headers=auth).json()
        seen.extend(item["ticket_id"] for item in page["items"])
        cursor = page.get("next_cursor")
        if not page["has_more"]:
            break

    assert len(seen) == len(set(seen)), "a ticket appeared on two pages"

    # And the union of the pages is a prefix of the same ordering, unpaginated.
    straight = client.get("/v1/support/tickets", params={"limit": len(seen)}, headers=auth).json()
    assert [i["ticket_id"] for i in straight["items"]] == seen


def test_a_created_ticket_cannot_choose_its_own_ground_truth(client, auth, a_ticket):
    """`intent` and `sentiment` are what the AI enrichment is scored against.

    `sentiment` is not in the request model at all and `intent` defaults, so a
    caller cannot write the answer key for the metric that grades the model.
    """
    from meridian.api.models import TicketCreate

    assert "sentiment" not in TicketCreate.model_fields

    created = client.post(
        "/v1/support/tickets",
        headers={"X-Ingest-Key": os.environ.get("API_INGEST_KEY", "")},
        json={
            "customer_id": a_ticket["customer_id"],
            "order_id": a_ticket["order_id"],
            "subject": "test: ground truth is server-set",
            "body": "created by the test suite",
            # Ignored — not a field on the model, so Pydantic drops it.
            "sentiment": "positive",
        },
    )
    if created.status_code == 401:
        pytest.skip("API_INGEST_KEY not set in this environment")
    assert created.status_code == 201
    assert created.json()["sentiment"] == "neutral"
    assert created.json()["status"] == "open"


def test_resolving_a_ticket_stamps_the_timestamp_the_check_constraint_needs(client, auth, a_ticket):
    """`resolved_ts >= created_ts` is a CHECK in the database.

    So status and resolved_ts cannot be allowed to drift apart in the handler —
    a status of `resolved` with a null timestamp would satisfy the constraint
    and be wrong, which is worse than failing.
    """
    created = client.post(
        "/v1/support/tickets",
        headers={"X-Ingest-Key": os.environ.get("API_INGEST_KEY", "")},
        json={
            "customer_id": a_ticket["customer_id"],
            "order_id": a_ticket["order_id"],
            "subject": "test: resolution stamps a timestamp",
            "body": "created by the test suite",
        },
    )
    if created.status_code == 401:
        pytest.skip("API_INGEST_KEY not set in this environment")
    ticket_id = created.json()["ticket_id"]

    resolved = client.patch(
        f"/v1/support/tickets/{ticket_id}", headers=auth, json={"status": "resolved"}
    ).json()
    assert resolved["status"] == "resolved"
    assert resolved["resolved_ts"] is not None

    reopened = client.patch(
        f"/v1/support/tickets/{ticket_id}", headers=auth, json={"status": "open"}
    ).json()
    assert reopened["resolved_ts"] is None, "un-resolving left a resolution timestamp behind"


def test_an_unknown_foreign_key_is_the_callers_fault(client, a_ticket):
    """422, not the 500 an unhandled IntegrityError would produce."""
    response = client.post(
        "/v1/support/tickets",
        headers={"X-Ingest-Key": os.environ.get("API_INGEST_KEY", "")},
        json={
            "customer_id": "C999999",
            "order_id": a_ticket["order_id"],
            "subject": "test",
            "body": "test",
        },
    )
    if response.status_code == 401:
        pytest.skip("API_INGEST_KEY not set in this environment")
    assert response.status_code == 422


def test_abstention_is_a_200_not_a_404(client, auth):
    """The request succeeded; the corpus does not cover the question.

    An error status would tell a client to retry something that will fail
    identically every time.
    """
    response = client.post(
        "/v1/ai/ask", headers=auth, json={"question": "what is the company share price?"}
    )
    if response.status_code == 503:
        pytest.skip("no vector store — run `make rag-index`")
    assert response.status_code == 200
    assert response.json()["abstained"] is True


def test_an_answer_always_carries_its_sources(client, auth):
    """An answer without its passages is a claim, and the whole argument for
    retrieval-augmented generation is that the claim can be checked."""
    response = client.post(
        "/v1/ai/ask", headers=auth, json={"question": "why do customers ask for refunds?"}
    )
    if response.status_code == 503:
        pytest.skip("no vector store — run `make rag-index`")
    body = response.json()
    assert body["abstained"] is False
    assert body["sources"], "an answered question returned no sources"
    assert all(hit["ticket_id"] for hit in body["sources"])


def test_search_exposes_which_ranker_found_each_hit(client, auth):
    """A hybrid retriever that cannot say which half found a document is one
    nobody can debug — and it was exactly this breakdown that revealed fusion
    dropping documents BM25 ranked first."""
    response = client.get(
        "/v1/ai/search", params={"q": "tracking has not updated", "top_k": 5}, headers=auth
    )
    if response.status_code == 503:
        pytest.skip("no vector store — run `make rag-index`")
    hits = response.json()["hits"]
    assert hits
    assert any(h["lexical_rank"] is not None for h in hits)
    assert any(h["vector_rank"] is not None for h in hits)


def test_the_ai_handlers_cannot_reach_pii_even_if_asked(client, auth):
    """The claim the whole role split exists to make.

    `/v1/ai/*` runs in a session opened as `analytics_ro`, which has no grant on
    `secure`. This asserts it directly against the same role the handler uses,
    because the property is about the *session*, not about the handler's SQL —
    a prompt injection that talked the model into asking for PII would get
    `InsufficientPrivilege` from Postgres.
    """
    import psycopg

    from meridian.db import connect

    with connect("analytics_ro", vectors=False) as conn:
        with pytest.raises(psycopg.errors.InsufficientPrivilege), conn.cursor() as cur:
            cur.execute("SELECT 1 FROM secure.customer_pii LIMIT 1")
        conn.rollback()


def test_the_ticket_handlers_have_no_warehouse_access(client):
    """`meridian_app` connects to `oltp` and cannot reach the warehouse at all.

    Not "has no grant on a table" — has no CONNECT privilege on the database.
    This was found the practical way: the first version of `oltp_connection`
    omitted the database name, and every ticket request failed with
    `permission denied for database "warehouse"`.
    """
    import psycopg

    from meridian.db import connect

    with pytest.raises((psycopg.OperationalError, Exception)) as caught:
        connect("meridian_app", vectors=False)
    assert "warehouse" in str(caught.value).lower()
