"""Screenshot every tab of a running dashboard.

    make dashboard-shots

Two jobs, and the second is the one that justifies the dependency.

**Documentation.** A README that describes a dashboard is asking to be
believed. One with images of it is showing the thing.

**A smoke test the unit tests cannot be.** `tests/test_dashboard.py` checks the
metrics module and the no-SQL-in-the-app rule; neither notices that a Plotly
call raises on an empty frame, that a column was renamed, or that a tab throws
after two clicks. Streamlit renders an exception *into the page* rather than
failing the process, so a broken panel serves HTTP 200 all day. This clicks
every tab and fails if Streamlit's exception block appears anywhere — which is
the only automated way to find out.

Chromium is already installed under `PLAYWRIGHT_BROWSERS_PATH`. Do not run
`playwright install` — see `_executable_path` for why the browser is located by
search rather than by the version-pinned path Playwright expects.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from meridian.runlog import EXIT_ERROR, EXIT_OK, RunLogger

TABS = (
    "Revenue",
    "Customers",
    "Retention",
    "Products",
    "Payments",
    "Funnel",
    "Support & AI",
    "Operations",
)


def _executable_path() -> str | None:
    """Find the installed Chromium, or let Playwright use its default.

    Playwright resolves its browser by a build number baked into the Python
    package — `chromium_headless_shell-1234/...` for this version — and the
    image ships build 1194. The versions do not have to match for the browser
    to work; they only have to match for Playwright's *path arithmetic* to
    find it, and when it does not the error says "run `playwright install`",
    which in this environment is both wrong and a large download.

    So the directory is searched instead. Returning None when nothing is found
    keeps the default behaviour on a machine where the pinned build is present.
    """
    import os

    root = Path(os.environ.get("PLAYWRIGHT_BROWSERS_PATH", "/opt/pw-browsers"))
    if not root.is_dir():
        return None
    # Prefer the full browser over the headless shell: the shell cannot render
    # some fonts, and these images are documentation.
    for pattern in ("chromium-*/chrome-linux/chrome", "chromium_headless_shell-*/*/chrome*"):
        for candidate in sorted(root.glob(pattern), reverse=True):
            if candidate.is_file():
                return str(candidate)
    return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:8501")
    parser.add_argument("--out", default="docs/images")
    parser.add_argument(
        "--timeout", type=float, default=60.0, help="Seconds to wait for the first render"
    )
    args = parser.parse_args(argv)

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print(
            "playwright is not installed — `pip install -e '.[dashboard]'`",
            file=sys.stderr,
        )
        return EXIT_ERROR

    log = RunLogger("dashboard.screenshots")
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    failures: list[str] = []

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(executable_path=_executable_path())
        # A wide viewport, because the layout is `wide` and a 1280px shot of a
        # three-column metric row crops the third one out of the documentation.
        page = browser.new_page(viewport={"width": 1600, "height": 1200})

        page.goto(args.url, wait_until="networkidle", timeout=args.timeout * 1000)
        # Streamlit streams the page in over a websocket, so `networkidle` is
        # reached before the first widget exists. Waiting for a real element is
        # the only reliable signal that a render has happened.
        page.wait_for_selector("[data-testid='stTabs']", timeout=args.timeout * 1000)

        for index, label in enumerate(TABS):
            tab = page.get_by_role("tab", name=label)
            tab.click()
            # Plotly mounts asynchronously after the tab switches. Without this
            # the screenshot catches an empty div and the smoke test passes on
            # a page that rendered nothing.
            page.wait_for_timeout(2500)

            # Streamlit renders an uncaught exception into the DOM instead of
            # failing the process, so a broken tab serves HTTP 200 with a
            # traceback in it. This is the assertion.
            errors = page.locator("[data-testid='stException']")
            if errors.count():
                message = errors.first.inner_text()[:300].replace("\n", " ")
                failures.append(f"{label}: {message}")
                log.emit("tab_error", tab=label, error=message)

            name = label.lower().replace(" & ", "-").replace(" ", "-")
            path = out / f"dashboard-{index:02d}-{name}.png"
            page.screenshot(path=str(path), full_page=True)
            log.emit("captured", tab=label, path=str(path))

        browser.close()

    log.emit(
        "done",
        tabs=len(TABS),
        failures=len(failures),
        directory=str(out),
        status="FAILED" if failures else "SUCCESS",
    )
    for failure in failures:
        print(f"  RENDER ERROR — {failure}", file=sys.stderr)
    return EXIT_ERROR if failures else EXIT_OK


def cli() -> int:
    return main()


if __name__ == "__main__":
    raise SystemExit(cli())
