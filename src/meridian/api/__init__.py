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

from meridian.settings import load_dotenv

# Bootstrap the same `.env` every other entrypoint gets.
#
# This package reads its own keys — API_JWT_SECRET, API_INGEST_KEY, the demo
# credentials — straight from the environment rather than through `settings()`,
# because they belong to the API alone and no other component has an opinion
# about them. That part is deliberate. What was not deliberate is the
# consequence: the `.env` load lives *inside* `settings()`, and no module under
# `meridian.api` calls it, so this was the one package in the project that never
# read `.env` at all.
#
# It was invisible wherever the variables happened to be exported already. CI
# writes them into $GITHUB_ENV; a developer shell that has run `set -a; source
# .env` has them too; and `/health` needs no token, so the server starts and
# answers. On a clean shell — a fresh clone, which is the case that actually
# matters — every authenticated endpoint failed at the first token mint, and
# `make api-demo` died on step one while every other target in the Makefile
# worked.
#
# `load_dotenv` does not override anything already set, so an explicitly
# exported value still wins over the file.
load_dotenv()
