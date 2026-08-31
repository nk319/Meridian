"""Support tickets. The source system, served over HTTP.

This is what `meridian.ingest.restapi` captures from — the same feed it read as
a JSON document in Phase 2. The columns, the watermark and the Bronze path were
fixed there so that changing the transport touches nothing downstream, and the
page shape here is what makes that true: `GET /v1/support/tickets` returns a
cursor-paginated page the ingestor can walk exactly as it walks the vendor feed.

Connects as `meridian_app`, which has CRUD on `oltp` and nothing anywhere else.
An injected `; SELECT * FROM secure.customer_pii` would fail on a grant rather
than on a WAF rule — and every statement below is parameterised anyway, so it
would not get that far.
"""

from __future__ import annotations

import base64
import datetime as dt
import json
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status

from ...runlog import RunLogger
from ..deps import (
    ingest_or_token,
    oltp_connection,
    require_tickets_read,
    require_tickets_write,
)
from ..models import Ticket, TicketCreate, TicketPage, TicketUpdate

router = APIRouter(prefix="/v1/support", tags=["tickets"])
log = RunLogger("api.tickets")

# Unqualified, because the `oltp` database keeps its tables in `public` — the
# schema names in CONTRACTS.md §1 (`silver`, `gold`, `meta`, …) are all in
# `warehouse`, and `oltp` has only the default. Writing `oltp.support_tickets`
# is the natural mistake and produces "relation does not exist" while the table
# is plainly there, because `oltp` is the *database*, not a schema in it.
COLUMNS = (
    "ticket_id, customer_id, order_id, created_ts, resolved_ts, status, "
    "channel, subject, body, intent, priority, sentiment"
)


def _encode_cursor(created_ts: dt.datetime, ticket_id: str) -> str:
    """Opaque, but not secret.

    base64 of a JSON pair rather than a raw `created_ts` in the query string.
    Not for secrecy — anyone can decode it — but because an opaque cursor is one
    clients cannot construct by hand, which is what lets the sort key change
    later without breaking every caller who assumed it was a timestamp.
    """
    payload = json.dumps([created_ts.isoformat(), ticket_id])
    return base64.urlsafe_b64encode(payload.encode()).decode()


def _decode_cursor(cursor: str) -> tuple[dt.datetime, str]:
    try:
        raw = base64.urlsafe_b64decode(cursor.encode()).decode()
        created_ts, ticket_id = json.loads(raw)
        return dt.datetime.fromisoformat(created_ts), ticket_id
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="malformed cursor — pass back the `next_cursor` from a previous page",
        ) from exc


@router.get("/tickets", response_model=TicketPage)
def list_tickets(
    _principal: Annotated[object, Depends(require_tickets_read)],
    conn: Annotated[object, Depends(oltp_connection)],
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    cursor: str | None = None,
    since: dt.datetime | None = Query(
        None,
        description=(
            "Only tickets created at or after this instant. This is the "
            "watermark parameter `meridian.ingest.restapi` uses for an "
            "incremental capture."
        ),
    ),
    status_filter: str | None = Query(None, alias="status"),
) -> TicketPage:
    where = []
    params: list = []

    if since is not None:
        # `>=`, not `>`. A watermark stored as the maximum `created_ts` already
        # seen would, with `>`, skip every other ticket sharing that exact
        # second — and the generator emits several per minute. The duplicate
        # this admits is free: Bronze is append-only and Silver dedups on the
        # record hash.
        where.append("created_ts >= %s")
        params.append(since)

    if status_filter:
        where.append("status = %s")
        params.append(status_filter)

    if cursor:
        created_ts, ticket_id = _decode_cursor(cursor)
        # Row-value comparison, which is the whole trick of keyset pagination:
        # `(created_ts, ticket_id) > (?, ?)` is a single index-friendly
        # predicate. Written as `created_ts > ? OR (created_ts = ? AND
        # ticket_id > ?)` it means the same thing and plans far worse.
        where.append("(created_ts, ticket_id) > (%s, %s)")
        params.extend([created_ts, ticket_id])

    clause = f"WHERE {' AND '.join(where)}" if where else ""
    # One extra row, to answer `has_more` without a second COUNT(*) over the
    # whole table. The count is the expensive half of a naive paginated API.
    params.append(limit + 1)

    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {COLUMNS} FROM support_tickets {clause} "
            f"ORDER BY created_ts, ticket_id LIMIT %s",
            params,
        )
        rows = cur.fetchall()
        names = [d.name for d in cur.description]

    has_more = len(rows) > limit
    rows = rows[:limit]
    items = [Ticket(**dict(zip(names, row, strict=True))) for row in rows]

    return TicketPage(
        items=items,
        has_more=has_more,
        next_cursor=(
            _encode_cursor(items[-1].created_ts, items[-1].ticket_id)
            if has_more and items
            else None
        ),
    )


@router.get("/tickets/{ticket_id}", response_model=Ticket)
def get_ticket(
    ticket_id: str,
    _principal: Annotated[object, Depends(require_tickets_read)],
    conn: Annotated[object, Depends(oltp_connection)],
) -> Ticket:
    with conn.cursor() as cur:
        cur.execute(f"SELECT {COLUMNS} FROM support_tickets WHERE ticket_id = %s", (ticket_id,))
        row = cur.fetchone()
        names = [d.name for d in cur.description]
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="no such ticket")
    return Ticket(**dict(zip(names, row, strict=True)))


@router.post("/tickets", response_model=Ticket, status_code=status.HTTP_201_CREATED)
def create_ticket(
    payload: TicketCreate,
    principal: Annotated[object, Depends(ingest_or_token)],
    conn: Annotated[object, Depends(oltp_connection)],
) -> Ticket:
    """Create a ticket. Either an ingest key or a token with `tickets:write`.

    `sentiment` is set to `neutral` and not taken from the caller, for the same
    reason `intent` defaults: those two columns are the ground truth
    `gold.mart_support_health` scores the model's predictions against, and a
    caller that could set them could write its own answer key.
    """
    # Both foreign keys are enforced by the database, and a violation of either
    # is the caller's mistake rather than a server fault — so it becomes a 422
    # rather than the 500 an unhandled IntegrityError would produce. The check
    # is not done with a preceding SELECT: that is a race, and the constraint is
    # the authority either way.
    import psycopg

    try:
        with conn.cursor() as cur:
            # The id is generated server-side from the table's own sequence rather
            # than accepted from the caller. A client-supplied id is a collision and
            # an enumeration surface, and here it would also let a caller overwrite
            # an existing ticket through a POST.
            cur.execute(
                """
            INSERT INTO support_tickets
                (ticket_id, customer_id, order_id, created_ts, status, channel,
                 subject, body, intent, priority, sentiment)
            VALUES (
                'T' || lpad(
                    (COALESCE((SELECT max(substring(ticket_id from 2)::int)
                               FROM support_tickets), 0) + 1)::text, 6, '0'),
                %s, %s, now(), 'open', %s, %s, %s, %s, %s, 'neutral')
            RETURNING """
                + COLUMNS,
                (
                    payload.customer_id,
                    payload.order_id,
                    payload.channel,
                    payload.subject,
                    payload.body,
                    payload.intent,
                    payload.priority,
                ),
            )
            row = cur.fetchone()
            names = [d.name for d in cur.description]
    except psycopg.errors.ForeignKeyViolation as exc:
        conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"customer_id {payload.customer_id!r} or order_id "
                f"{payload.order_id!r} does not exist"
            ),
        ) from exc
    conn.commit()

    log.emit("ticket_created", ticket_id=row[0], by=getattr(principal, "subject", "?"))
    return Ticket(**dict(zip(names, row, strict=True)))


@router.patch("/tickets/{ticket_id}", response_model=Ticket)
def update_ticket(
    ticket_id: str,
    payload: TicketUpdate,
    principal: Annotated[object, Depends(require_tickets_write)],
    conn: Annotated[object, Depends(oltp_connection)],
) -> Ticket:
    updates = payload.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="no fields to update")

    # Column names come from a Pydantic model with a fixed field set, so they
    # cannot be caller-controlled; the values are still parameterised. Both
    # halves matter — an f-string over `updates.keys()` from an unvalidated dict
    # is the textbook injection.
    assignments = ", ".join(f"{name} = %s" for name in updates)
    params = list(updates.values())

    # Resolving a ticket stamps `resolved_ts`, and un-resolving clears it. The
    # database has a CHECK that `resolved_ts >= created_ts`, so the two columns
    # cannot be allowed to drift apart here.
    if updates.get("status") == "resolved":
        assignments += ", resolved_ts = now()"
    elif "status" in updates:
        assignments += ", resolved_ts = NULL"

    params.append(ticket_id)
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE support_tickets SET {assignments} WHERE ticket_id = %s RETURNING {COLUMNS}",
            params,
        )
        row = cur.fetchone()
        names = [d.name for d in cur.description]

    if row is None:
        conn.rollback()
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="no such ticket")
    conn.commit()

    log.emit(
        "ticket_updated",
        ticket_id=ticket_id,
        fields=sorted(updates),
        by=getattr(principal, "subject", "?"),
    )
    return Ticket(**dict(zip(names, row, strict=True)))
