"""Print shell exports for a bearer token.

    eval $(make -s api-token)
    curl -H "Authorization: Bearer $MERIDIAN_API_TOKEN" localhost:8000/v1/support/tickets

Exists because the alternative is a README line telling people to paste a JSON
field into a shell variable by hand, and the REST ingestor needs the same token
to walk the live feed. Emitting `export` lines rather than a bare token means
one `eval` sets up both.

Mints the token in-process rather than calling `/v1/auth/token`, so it works
before the server is up and needs no password round trip — it has the signing
secret, which is the same thing the server has. It cannot mint a scope the
server would not: `decode_token` rejects unknown scopes on the way back in.
"""

from __future__ import annotations

import argparse
import sys

from ..runlog import EXIT_ERROR, EXIT_OK
from .security import ALL_SCOPES, AuthError, issue_token


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--subject", default="cli", help="Token subject")
    parser.add_argument(
        "--scopes",
        default=",".join(ALL_SCOPES),
        help=f"Comma-separated. One or more of {', '.join(ALL_SCOPES)}",
    )
    parser.add_argument("--raw", action="store_true", help="Print the bare token, without `export`")
    parser.add_argument(
        "--url",
        default="http://localhost:8000",
        help="Also export MERIDIAN_API_URL, which the REST ingestor reads",
    )
    args = parser.parse_args(argv)

    scopes = tuple(s.strip() for s in args.scopes.split(",") if s.strip())
    unknown = set(scopes) - set(ALL_SCOPES)
    if unknown:
        print(f"unknown scopes: {sorted(unknown)}", file=sys.stderr)
        return EXIT_ERROR

    try:
        token, expires_in = issue_token(args.subject, scopes)
    except AuthError as exc:
        print(str(exc), file=sys.stderr)
        return EXIT_ERROR

    if args.raw:
        print(token)
        return EXIT_OK

    print(f"export MERIDIAN_API_TOKEN={token}")
    print(f"export MERIDIAN_API_URL={args.url}")
    # A comment, so `eval` ignores it and a human running the command without
    # eval still learns when it stops working.
    print(f"# expires in {expires_in // 60} minutes; scopes: {', '.join(scopes)}")
    return EXIT_OK


def cli() -> int:
    return main()


if __name__ == "__main__":
    raise SystemExit(cli())
