"""What has to survive being installed rather than run from a checkout.

Every test in this suite, and every local `make` target, runs against the source
tree — where a `.sql` file sitting next to its module is simply present on disk.
An installed copy is different: setuptools ships Python files and nothing else
unless told otherwise, so a data file can be missing from the wheel while every
test passes and every local run works.

That is not hypothetical. `meridian.rag` was listed in package-data and
`meridian.warehouse` was added later without being listed, and nothing caught it
until an Airflow task ran from the installed copy inside the image and died on
`FileNotFoundError: .../meridian/warehouse/ddl.sql`.
"""

from __future__ import annotations

import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"


@pytest.fixture(scope="module")
def pyproject() -> dict:
    return tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))


def sql_files() -> list[Path]:
    return sorted(SRC.rglob("*.sql"))


def test_there_are_data_files_to_worry_about():
    """Guard: if no .sql files exist, the test below proves nothing."""
    found = sql_files()
    assert found, "no .sql files under src/ — this suite is asserting nothing"


def test_every_sql_file_is_covered_by_package_data(pyproject):
    """Each data file must be shipped by some package-data glob.

    A wildcard entry satisfies this for everything at once, which is the point:
    the per-package form is what went wrong, because adding a package is easy to
    do without remembering to also declare it here.
    """
    import fnmatch

    package_data = pyproject["tool"]["setuptools"]["package-data"]
    for path in sql_files():
        package = ".".join(path.relative_to(SRC).parent.parts)
        patterns = [
            pattern
            for key, globs in package_data.items()
            if key == "*" or key == package
            for pattern in globs
        ]
        assert patterns, f"{package} ships {path.name} but has no package-data entry"
        assert any(fnmatch.fnmatch(path.name, p) for p in patterns), (
            f"{path.name} in {package} matches none of {patterns}"
        )


def test_modules_that_read_a_sql_sibling_resolve_it_at_runtime():
    """The files are where the code that reads them expects.

    This passes in a checkout by construction, so it is not the test that would
    have caught the packaging bug — it is the one that catches a file being
    moved or renamed, which is a different mistake with the same symptom.
    """
    from meridian.rag import index as rag_index
    from meridian.warehouse import bootstrap as warehouse_bootstrap

    assert (Path(rag_index.__file__).parent / "ddl.sql").is_file()
    assert warehouse_bootstrap.DDL_PATH.is_file()


def test_declared_dependencies_cover_what_the_pipeline_imports(pyproject):
    """The lake cannot work without duckdb and pyarrow.

    They arrived as a development convenience — installed by hand while building
    Phase 2 — and only later became declared dependencies. An undeclared import
    works on the machine that installed it by hand and nowhere else.
    """
    declared = " ".join(pyproject["project"]["dependencies"]).lower()
    for package in ("psycopg", "pgvector", "pyyaml", "duckdb", "pyarrow"):
        assert package in declared, f"{package} is imported by the pipeline but not declared"


def test_rag_extra_is_optional(pyproject):
    """CONTRACTS.md keeps the platform runnable with no API key and no model.

    fastembed and anthropic belong in an extra, not in the base dependencies —
    otherwise generating seed data pulls in onnxruntime.
    """
    base = " ".join(pyproject["project"]["dependencies"]).lower()
    extra = " ".join(pyproject["project"]["optional-dependencies"]["rag"]).lower()
    assert "fastembed" in extra and "anthropic" in extra
    assert "fastembed" not in base and "anthropic" not in base
