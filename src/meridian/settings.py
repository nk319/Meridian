"""Configuration, resolved from the environment exactly once.

Everything configurable lives here, and every value comes from the environment
or from a `.env` file that is never committed. No module reads `os.environ`
directly — otherwise the set of knobs is whatever grep finds today, and adding
one is invisible in review.

Deliberately stdlib-only. Phase 6 brings FastAPI and pydantic-settings for the
API; making Phase 1's indexer depend on pydantic to read five strings would
mean the RAG layer cannot run without the web stack installed.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

# Roles from CONTRACTS.md §1, mapped to the environment variable holding each
# password. The mapping is here rather than at each call site so that "which
# role am I connecting as" is answerable by reading one table.
ROLE_PASSWORD_ENV = {
    "meridian_app": "MERIDIAN_APP_PASSWORD",
    "meridian_etl": "MERIDIAN_ETL_PASSWORD",
    "dbt_runner": "DBT_RUNNER_PASSWORD",
    "analytics_ro": "ANALYTICS_RO_PASSWORD",
    "rag_indexer": "RAG_INDEXER_PASSWORD",
}


def project_root() -> Path:
    """Where `seeds/`, `eval/` and `docs/governance/` live.

    Normally the repository root, found by walking up from this file to the
    nearest `pyproject.toml` — resolved from the module rather than the working
    directory, so the answer is the same for pytest, `make`, and an Airflow task
    with a working directory of its own.

    `MERIDIAN_ROOT` overrides it, and that is not a convenience knob. Once the
    package is installed rather than run from a checkout — as it is inside the
    Airflow image, in its own venv under /opt — there is no `pyproject.toml`
    above it and the walk falls through to `site-packages`. Every path derived
    from it then points somewhere plausible and empty, so the failure is
    "0 tickets found" rather than an error naming a directory.
    """
    override = os.environ.get("MERIDIAN_ROOT", "").strip()
    if override:
        return Path(override).resolve()

    here = Path(__file__).resolve()
    for candidate in here.parents:
        if (candidate / "pyproject.toml").is_file():
            return candidate
    return here.parents[2]


def load_dotenv(path: Path | None = None, *, override: bool = False) -> int:
    """Read KEY=VALUE lines from `.env` into the environment.

    Real environment variables win by default. That ordering matters in CI and
    in containers, where the environment is the source of truth and a stale
    `.env` left in a working copy must not quietly override it.
    """
    path = path or (project_root() / ".env")
    if not path.is_file():
        return 0

    loaded = 0
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if override or key not in os.environ:
            os.environ[key] = value
            loaded += 1
    return loaded


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    return int(raw) if raw else default


def _float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    return float(raw) if raw else default


@dataclass(frozen=True)
class Settings:
    pg_host: str
    pg_port: int
    pg_superuser: str
    pg_superuser_password: str

    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    lake_bucket: str

    embedding_model: str
    anthropic_model: str
    anthropic_api_key: str

    rrf_k: int
    candidate_pool: int
    top_k: int
    abstain_similarity: float

    root: Path

    # Database names are fixed by CONTRACTS.md §1, not configurable. Making them
    # settings would let two components disagree about which database the
    # warehouse is, which is the exact class of drift that document exists for.
    warehouse_db: str = "warehouse"
    oltp_db: str = "oltp"

    @property
    def seeds_dir(self) -> Path:
        return self.root / "seeds"

    @property
    def minio_host(self) -> str:
        """Endpoint without the scheme, which is the form DuckDB's S3 secret wants."""
        return self.minio_endpoint.replace("https://", "").replace("http://", "")

    @property
    def minio_use_ssl(self) -> bool:
        return self.minio_endpoint.startswith("https://")

    @property
    def has_anthropic_key(self) -> bool:
        return bool(self.anthropic_api_key.strip())

    def dsn(self, role: str, dbname: str | None = None) -> str:
        """libpq connection string for one of the five contract roles."""
        if role not in ROLE_PASSWORD_ENV:
            raise ValueError(
                f"unknown role {role!r}; CONTRACTS.md §1 defines {sorted(ROLE_PASSWORD_ENV)}"
            )
        env_name = ROLE_PASSWORD_ENV[role]
        password = os.environ.get(env_name, "")
        if not password:
            raise RuntimeError(
                f"{env_name} is not set, so no connection can be made as {role}. "
                f"Copy .env.example to .env and fill it in."
            )
        db = dbname or self.warehouse_db
        return (
            f"host={self.pg_host} port={self.pg_port} dbname={db} user={role} password={password}"
        )


@lru_cache(maxsize=1)
def settings() -> Settings:
    """The process-wide settings, loaded once."""
    load_dotenv()
    return Settings(
        pg_host=os.environ.get("POSTGRES_HOST", "localhost"),
        pg_port=_int("POSTGRES_PORT", 5432),
        pg_superuser=os.environ.get("POSTGRES_SUPERUSER", "postgres"),
        pg_superuser_password=os.environ.get("POSTGRES_SUPERUSER_PASSWORD", ""),
        minio_endpoint=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
        minio_access_key=os.environ.get("MINIO_ROOT_USER", ""),
        minio_secret_key=os.environ.get("MINIO_ROOT_PASSWORD", ""),
        lake_bucket=os.environ.get("LAKE_BUCKET", "meridian-lake"),
        embedding_model=os.environ.get("EMBEDDING_MODEL", "BAAI/bge-small-en-v1.5"),
        anthropic_model=os.environ.get("ANTHROPIC_MODEL", "claude-opus-5"),
        anthropic_api_key=os.environ.get("ANTHROPIC_API_KEY", ""),
        rrf_k=_int("RAG_RRF_K", 60),
        candidate_pool=_int("RAG_CANDIDATE_POOL", 50),
        top_k=_int("RAG_TOP_K", 5),
        abstain_similarity=_float("RAG_ABSTAIN_SIMILARITY", 0.68),
        root=project_root(),
    )
