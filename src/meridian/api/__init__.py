"""The platform's own REST API.

Two jobs that would normally be two services, kept in one because the
interesting thing about them is that they connect to Postgres **as different
roles**:

  * the support-ticket endpoints write `oltp` as `meridian_app`, which has CRUD
    on that database and no warehouse access at all, and
  * every `/v1/ai/*` handler reads as `analytics_ro`, which has SELECT on
    `gold`, `rag` and `meta` and nothing on `secure`, `oltp` or `silver`.

CONTRACTS.md §1 specifies both. The consequence is worth stating plainly: a
request to `/v1/ai/ask` runs in a database session that is *incapable* of
reading a customer's email address. Not "does not"; cannot. A prompt injection
that talked the model into asking for PII would get an `InsufficientPrivilege`
from Postgres, and `tests/test_pii_boundary.py` is what keeps that true.

The API is also the source `meridian.ingest.restapi` captures from. Phase 2
read the JSON document this now serves; the columns, the watermark and the
Bronze path were fixed then precisely so that swapping the transport changes
nothing downstream.
"""
