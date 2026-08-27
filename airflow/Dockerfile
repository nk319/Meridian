# Airflow 3.1.3, with the pipeline living in its own interpreters.
#
# Three Python environments, and the separation is the point.
#
#   Airflow's own      — the scheduler and API server. Nothing else.
#   /opt/meridian-venv — the pipeline: meridian and every dependency it has.
#   /opt/dbt-venv      — dbt, alone.
#
# CONTRACTS.md §6 requires the dbt split and explains why: Airflow 3.1.3's
# constraints pin protobuf==4.25.8 while dbt-core requires protobuf>=6.0, so
# co-installing is an unresolvable resolver error rather than a preference.
#
# The same argument applies a second time, which §6 did not anticipate. The RAG
# layer pulls onnxruntime through fastembed, and onnxruntime has its own protobuf
# floor — installing it beside Airflow would break Airflow in exactly the way dbt
# would. So the pipeline gets the same treatment rather than a special case.
#
# The upshot is the property CONTRACTS.md §5 actually asked for: **Airflow calls
# these as commands and never imports pipeline internals.** Every task in every
# DAG is a subprocess invoked by absolute path. That is what lets `make pipeline`
# prove the platform works with the orchestrator switched off, and it means a
# dependency change in the pipeline cannot take the scheduler down.

FROM apache/airflow:3.1.3

USER root

# --------------------------------------------------------------------------
# Trust store, for networks that re-terminate TLS.
#
# Behind an inspecting egress proxy — as this project's build environment is —
# pip sees a self-signed certificate in the chain and refuses, which is correct
# behaviour and must not be "fixed" with --trusted-host. The proxy's CA is
# staged into airflow/certs/ by `make airflow-build` and installed here.
#
# The directory is committed with only a .gitkeep in it, so a clone on an
# ordinary network builds unchanged: the glob matches nothing, the branch is
# skipped, and pip uses the system trust store as usual. This layer sits before
# the source COPY so editing a DAG does not invalidate the dependency cache.
# --------------------------------------------------------------------------
COPY airflow/certs/ /tmp/build-certs/
RUN if ls /tmp/build-certs/*.crt >/dev/null 2>&1; then \
        cp /tmp/build-certs/*.crt /usr/local/share/ca-certificates/ && \
        update-ca-certificates && \
        echo "installed a local CA into the image trust store"; \
    else \
        echo "no local CA staged; using the system trust store"; \
    fi \
    && rm -rf /tmp/build-certs

# Point every client at the system bundle, which now includes the proxy CA if
# one was staged. Set unconditionally so the two cases differ only in the
# contents of that bundle, not in how the build behaves.
ENV PIP_CERT=/etc/ssl/certs/ca-certificates.crt \
    SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt \
    REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

# No apt layer. Every dependency below ships a manylinux wheel — psycopg[binary]
# bundles libpq, and duckdb, pyarrow, pandas and onnxruntime are all prebuilt —
# so there is nothing to compile. Installing a toolchain "just in case" would add
# a few hundred megabytes and a package index to be reachable at build time, in
# exchange for nothing. If a future dependency does need to compile, that is the
# moment to add it, and the build will say so.

# The pipeline source. Copied rather than mounted so the image is self-contained
# and a task cannot silently run a different version of the code than the one it
# was built with.
COPY pyproject.toml /opt/meridian/pyproject.toml
COPY src /opt/meridian/src

# The dbt project, alongside the pipeline rather than under /opt/airflow. Two
# reasons, both about paths. `meridian.dbt.results` reads run_results.json from
# `<project root>/dbt/target`, and the project root inside this image is
# MERIDIAN_ROOT=/opt/meridian — so putting the project here means the parse task
# needs no argument and cannot be pointed at a stale directory. And dbt writes
# target/ and logs/ into the project directory, so it has to be somewhere the
# airflow user can write, which a read-only bind mount of the repo would not be.
COPY dbt /opt/meridian/dbt

RUN python3 -m venv /opt/meridian-venv \
    && /opt/meridian-venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/meridian-venv/bin/pip install --no-cache-dir "/opt/meridian[rag,stream]" pandera pandas

RUN python3 -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-core>=1.9,<2" "dbt-postgres>=1.9,<2"

# Prove the separation held, at build time rather than at first DAG run. If a
# resolver decision quietly broke one of the three, the image fails to build
# instead of producing a scheduler that starts and then cannot run anything.
RUN /opt/meridian-venv/bin/python -c "import meridian, duckdb, psycopg, pandera, confluent_kafka, fastavro; print('meridian venv ok')" \
    && /opt/dbt-venv/bin/dbt --version > /dev/null && echo "dbt venv ok" \
    && python -c "import airflow; print('airflow', airflow.__version__)"

RUN chown -R airflow:root /opt/meridian /opt/meridian-venv /opt/dbt-venv

USER airflow
