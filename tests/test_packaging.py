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


def third_party_imports() -> dict[str, set[str]]:
    """Every non-stdlib, non-local top-level module `src/` imports.

    Derived by parsing the source rather than listed by hand. The hand-written
    version of the test below missed `pandera`, `numpy` and `pydantic` for
    months — the first was installed by hand into a local venv and again by hand
    into the Airflow image, and the other two arrived transitively through
    fastembed and fastapi. Every machine that mattered had all three, so nothing
    anywhere said they were required until CI ran on a clean runner.

    That is the failure mode a hardcoded list has: it only ever checks the
    dependencies somebody remembered, which are by definition not the ones that
    go missing.
    """
    import ast
    import sys

    stdlib = set(sys.stdlib_module_names)
    found: dict[str, set[str]] = {}

    for path in SRC.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name.split(".")[0] for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and not node.level:
                names = [(node.module or "").split(".")[0]]
            else:
                # `node.level` truthy means a relative import — `meridian`
                # importing itself, which is never a dependency.
                continue
            for name in names:
                if name and name not in stdlib and name != "meridian":
                    found.setdefault(name, set()).add(str(path.relative_to(SRC)))
    return found


def _normalise(name: str) -> str:
    return name.lower().replace("_", "-")


def test_declared_dependencies_cover_what_the_pipeline_imports(pyproject):
    """Every third-party import is declared somewhere in pyproject.

    Module names and distribution names differ often enough that matching them
    naively produces false passes: `yaml` ships in `PyYAML`, `jwt` in `PyJWT`,
    `confluent_kafka` in `confluent-kafka`. `packages_distributions()` maps the
    installed module back to the distribution that provided it, which is the
    only reliable direction — falling back to the module name for anything not
    installed, so a missing package fails rather than silently passing.
    """
    from importlib.metadata import packages_distributions

    modules_to_dists = packages_distributions()

    declared = " ".join(pyproject["project"]["dependencies"])
    for group in pyproject["project"].get("optional-dependencies", {}).values():
        declared += " " + " ".join(group)
    declared = _normalise(declared)

    undeclared = {}
    for module, importers in third_party_imports().items():
        candidates = {module, *modules_to_dists.get(module, [])}
        if not any(_normalise(candidate) in declared for candidate in candidates):
            undeclared[module] = sorted(importers)[:3]

    assert not undeclared, "imported by src/ but declared in no dependency group: " + "; ".join(
        f"{name} (from {', '.join(files)})" for name, files in undeclared.items()
    )


def test_the_import_scan_finds_something(pyproject):
    """Guard: if the scan returns nothing, the test above asserts nothing.

    The same reasoning as `test_there_are_data_files_to_worry_about`. A parser
    that silently stopped matching would turn the check above into a green tick
    over an empty set.
    """
    found = third_party_imports()
    assert len(found) > 5, f"the import scan found only {sorted(found)} — it is probably broken"
    # Three the pipeline certainly imports, as a canary on the parser itself.
    assert {"psycopg", "duckdb", "yaml"} <= set(found)


def test_rag_extra_is_optional(pyproject):
    """CONTRACTS.md keeps the platform runnable with no API key and no model.

    fastembed and anthropic belong in an extra, not in the base dependencies —
    otherwise generating seed data pulls in onnxruntime.
    """
    base = " ".join(pyproject["project"]["dependencies"]).lower()
    extra = " ".join(pyproject["project"]["optional-dependencies"]["rag"]).lower()
    assert "fastembed" in extra and "anthropic" in extra
    assert "fastembed" not in base and "anthropic" not in base


# ---------------------------------------------------------------------------
# `make env` — the first command a clone runs, and the one with no second chance
# ---------------------------------------------------------------------------


def test_generated_env_has_no_placeholder_left_and_no_duplicate_keys():
    """`scripts/write_env.py` substitutes in place. Both halves matter.

    `settings.load_dotenv` is first-wins — it assigns a key only
    `if override or key not in os.environ` — so a file that copies the template
    and *appends* overrides resolves to the placeholder, not the override. CI
    uses that shape safely only because it then exports a deduplicated last-wins
    copy into `$GITHUB_ENV`, and real environment variables beat the file.
    Nothing on a developer machine does that.

    The failure has no symptom, which is why it is asserted rather than trusted:
    `change_me_locally` is a *set* value, so `db/init/04_roles.sql` accepts it
    and the whole stack comes up correctly on a password published in a public
    template.
    """
    import importlib.util

    spec = importlib.util.spec_from_file_location("write_env", ROOT / "scripts" / "write_env.py")
    assert spec and spec.loader
    write_env = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(write_env)

    rendered, replaced = write_env.render((ROOT / ".env.example").read_text(encoding="utf-8"))

    assert replaced >= 10, (
        f"only {replaced} secrets were generated — the matcher is probably broken"
    )

    keys, values = [], {}
    for line in rendered.splitlines():
        stripped = line.strip()
        if stripped.startswith("#") or "=" not in stripped:
            continue
        key, _, value = stripped.partition("=")
        keys.append(key)
        values[key] = value.strip()

    duplicates = {k for k in keys if keys.count(k) > 1}
    assert not duplicates, (
        f"duplicate keys resolve first-wins, so these are ambiguous: {duplicates}"
    )

    survivors = {
        k
        for k, v in values.items()
        if v in write_env.PLACEHOLDERS or v.startswith(write_env.FERNET_PREFIX)
    }
    assert not survivors, f"placeholders survived into the generated file: {survivors}"

    # The JWT secret specifically: RFC 7518 §3.2 sets a 32-byte floor for HS256
    # and PyJWT only warns below it.
    assert len(values["API_JWT_SECRET"]) >= 32


def test_every_secret_in_the_template_is_one_this_generator_recognises():
    """The guard on the guard.

    The generator matches placeholders by *value*, so a secret added to
    `.env.example` with some new placeholder string would be copied through
    verbatim and silently become the deployed value. This is what turns that
    into a failing test rather than a quiet hole.
    """
    import re

    suspicious = re.compile(r"(change|generate|set-a|replace|your[-_]|xxx|todo)", re.I)
    offenders = []
    for line in (ROOT / ".env.example").read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("#") or "=" not in stripped:
            continue
        key, _, value = stripped.partition("=")
        value = value.strip()
        if value and suspicious.search(value):
            offenders.append((key, value))

    import importlib.util

    spec = importlib.util.spec_from_file_location("write_env", ROOT / "scripts" / "write_env.py")
    write_env = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(write_env)

    unhandled = [
        (k, v)
        for k, v in offenders
        if v not in write_env.PLACEHOLDERS and not v.startswith(write_env.FERNET_PREFIX)
    ]
    assert not unhandled, (
        f"these look like placeholders the generator will not replace: {unhandled}. "
        f"Add the value to PLACEHOLDERS in scripts/write_env.py."
    )
