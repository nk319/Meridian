"""The FastAPI application.

    uvicorn meridian.api.main:app --reload
    make api            # the same, with the port from .env

Startup builds the retriever once — a 67 MB ONNX model that would otherwise be
loaded per request — and startup is allowed to *fail soft*: a missing vector
store leaves `/v1/ai/*` returning 503 while the ticket endpoints keep working.
The alternative, refusing to boot, means an unindexed corpus takes down the
source-system API with it, and those two have nothing to do with each other.

`/health` is liveness and `/ready` is readiness, and they are deliberately not
the same endpoint. Liveness answers "should this process be restarted"; a
restart does not fix an empty vector store, so a store check belongs in
readiness and would cause a restart loop in liveness. Conflating them is the
most common way a Kubernetes deployment ends up cycling healthy pods.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from ..db import server_reachable
from ..runlog import RunLogger
from ..settings import settings
from . import deps
from .models import HealthResponse
from .routers import ai, auth, tickets

log = RunLogger("api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    retriever = None
    try:
        from ..rag.retrieve import Retriever

        retriever = Retriever.open()
        deps.set_retriever(retriever)
        log.emit("retriever_ready")
    except Exception as exc:  # noqa: BLE001
        # Deliberately not fatal. See the module header: an unindexed corpus
        # must not take the ticket API down with it.
        log.emit(
            "retriever_unavailable",
            error=f"{type(exc).__name__}: {exc}",
            effect="/v1/ai/* will return 503 until `make rag-index` has run",
        )

    yield

    if retriever is not None:
        with suppress(Exception):
            retriever.close()


app = FastAPI(
    title="Meridian",
    version="1.0.0",
    summary="The platform's own source system, and the AI layer over its ticket corpus",
    description=__doc__,
    lifespan=lifespan,
)

app.include_router(auth.router)
app.include_router(tickets.router)
app.include_router(ai.router)


@app.exception_handler(Exception)
async def unhandled(request: Request, exc: Exception) -> JSONResponse:
    """One 500 shape, and never the exception text.

    A stack trace or an exception message in a response body leaks table names,
    file paths and sometimes parameter values. It is logged in full — where an
    operator can read it — and the caller gets the run id to quote.
    """
    log.emit(
        "unhandled",
        path=request.url.path,
        method=request.method,
        error=f"{type(exc).__name__}: {exc}",
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "internal error", "run_id": log.run_id},
    )


@app.get("/health", response_model=HealthResponse, tags=["ops"])
def health() -> HealthResponse:
    """Liveness. Answers only "is this process running", and nothing else.

    No database call. A `/health` that touches Postgres reports the database's
    outage as this process's, and an orchestrator responds by restarting a
    perfectly good API — repeatedly, for as long as the database is down.
    """
    return HealthResponse(status="ok", checks={"process": "ok"})


@app.get("/ready", response_model=HealthResponse, tags=["ops"])
def ready() -> HealthResponse:
    """Readiness. Whether this process can currently serve traffic.

    Reports `degraded` rather than failing when only the AI half is down,
    because the ticket endpoints are genuinely still serving. A binary
    ready/not-ready would take the whole API out of rotation over an empty
    vector store.
    """
    checks = {
        "postgres": "ok" if server_reachable() else "unreachable",
        "retriever": "ok" if deps._state.get("retriever") else "unavailable",
        "anthropic_key": "set" if settings().has_anthropic_key else "absent (extractive answers)",
    }
    healthy = checks["postgres"] == "ok" and checks["retriever"] == "ok"
    return HealthResponse(status="ok" if healthy else "degraded", checks=checks)


def main() -> int:
    """`python -m meridian.api.main`, for parity with every other entrypoint."""
    import uvicorn

    uvicorn.run(
        "meridian.api.main:app",
        host=os.environ.get("API_HOST", "0.0.0.0"),  # noqa: S104 — bound inside a container
        port=int(os.environ.get("API_PORT", "8000")),
        log_level="info",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
