"""Walk every endpoint against a running API, and print what happened.

    make api-demo

Deliberately hits the *network*, not `TestClient`. `tests/test_api.py` covers
behaviour in-process, which is faster and the right tool for assertions; this
covers the things in-process testing cannot see — that the port is bound, that
the container's environment carries the secrets, that a token minted by one
process verifies in another. Those are exactly the failures that only appear
after deployment.

Prints a table rather than asserting. It is a demonstration, and a failed step
here should show what the API actually returned, not a stack trace.
"""

from __future__ import annotations

import argparse
import os
import sys

from ..runlog import EXIT_ERROR, EXIT_OK, RunLogger
from .security import ALL_SCOPES, AuthError, issue_token


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--url", default=os.environ.get("MERIDIAN_API_URL", "http://localhost:8000")
    )
    args = parser.parse_args(argv)

    import httpx

    log = RunLogger("api.demo")
    base = args.url.rstrip("/")

    try:
        token, _ = issue_token("demo", ALL_SCOPES)
    except AuthError as exc:
        print(f"cannot mint a token: {exc}", file=sys.stderr)
        return EXIT_ERROR

    auth = {"Authorization": f"Bearer {token}"}
    ingest = {"X-Ingest-Key": os.environ.get("API_INGEST_KEY", "")}
    results: list[tuple[str, str, int, str]] = []

    def record(label: str, method: str, path: str, **kwargs) -> httpx.Response | None:
        try:
            response = client.request(method, path, **kwargs)
        except httpx.HTTPError as exc:
            results.append((label, f"{method} {path}", 0, f"{type(exc).__name__}: {exc}"))
            return None
        body = response.text[:90].replace("\n", " ")
        results.append((label, f"{method} {path}", response.status_code, body))
        return response

    with httpx.Client(base_url=base, timeout=60.0) as client:
        record("liveness", "GET", "/health")
        record("readiness", "GET", "/ready")

        # The negative cases first, because an API that returns 200 to an
        # unauthenticated caller is worth finding out about before the happy
        # path scrolls it off the screen.
        record("no token", "GET", "/v1/support/tickets")
        record("bad token", "GET", "/v1/support/tickets", headers={"Authorization": "Bearer nope"})
        record(
            "bad ingest key",
            "POST",
            "/v1/support/tickets",
            headers={"X-Ingest-Key": "wrong"},
            json={"customer_id": "C000001", "order_id": "O0000001", "subject": "x", "body": "y"},
        )

        page = record(
            "list tickets", "GET", "/v1/support/tickets", headers=auth, params={"limit": 3}
        )
        first = None
        if page is not None and page.status_code == 200 and page.json()["items"]:
            first = page.json()["items"][0]
            record("get one", "GET", f"/v1/support/tickets/{first['ticket_id']}", headers=auth)
            if page.json().get("next_cursor"):
                record(
                    "next page",
                    "GET",
                    "/v1/support/tickets",
                    headers=auth,
                    params={"limit": 3, "cursor": page.json()["next_cursor"]},
                )

        record(
            "bad cursor",
            "GET",
            "/v1/support/tickets",
            headers=auth,
            params={"cursor": "not-base64"},
        )

        if first:
            created = record(
                "create (ingest key)",
                "POST",
                "/v1/support/tickets",
                headers=ingest,
                json={
                    "customer_id": first["customer_id"],
                    "order_id": first["order_id"],
                    "subject": "Demo: where is my order?",
                    "body": "Placed a few days ago and nothing has moved.",
                    "channel": "web_form",
                    "priority": "P3",
                },
            )
            if created is not None and created.status_code == 201:
                ticket_id = created.json()["ticket_id"]
                record(
                    "resolve it",
                    "PATCH",
                    f"/v1/support/tickets/{ticket_id}",
                    headers=auth,
                    json={"status": "resolved"},
                )
            record(
                "bad enum",
                "POST",
                "/v1/support/tickets",
                headers=ingest,
                json={
                    "customer_id": first["customer_id"],
                    "order_id": first["order_id"],
                    "subject": "x",
                    "body": "y",
                    "priority": "P9",
                },
            )
            record(
                "unknown customer",
                "POST",
                "/v1/support/tickets",
                headers=ingest,
                json={
                    "customer_id": "C999999",
                    "order_id": first["order_id"],
                    "subject": "x",
                    "body": "y",
                },
            )

        record(
            "search",
            "GET",
            "/v1/ai/search",
            headers=auth,
            params={"q": "tracking has not updated in over a week", "top_k": 3},
        )
        record(
            "bad strategy",
            "GET",
            "/v1/ai/search",
            headers=auth,
            params={"q": "anything", "strategy": "magic"},
        )
        record(
            "ask",
            "POST",
            "/v1/ai/ask",
            headers=auth,
            json={"question": "why do customers ask for refunds?"},
        )
        record(
            "ask (abstains)",
            "POST",
            "/v1/ai/ask",
            headers=auth,
            json={"question": "what was the company share price last quarter?"},
        )

    print(f"{'step':22} {'request':42} {'code':>5}  body")
    print("-" * 118)
    for label, request, code, body in results:
        print(f"{label:22} {request:42} {code:>5}  {body}")

    failures = [r for r in results if r[2] == 0 or r[2] >= 500]
    log.emit(
        "done",
        steps=len(results),
        failures=len(failures),
        status="FAILED" if failures else "SUCCESS",
    )
    return EXIT_ERROR if failures else EXIT_OK


def cli() -> int:
    return main()


if __name__ == "__main__":
    raise SystemExit(cli())
