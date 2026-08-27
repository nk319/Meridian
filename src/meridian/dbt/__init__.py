"""Running dbt as part of the pipeline, and getting its results back out.

dbt reports into its own artefacts. `meta.dq_check_results` is where every
other assertion in this platform lands (CONTRACTS.md §7: "Fed by Pandera,
custom checks, and parsed dbt `run_results.json` alike"), and a warehouse where
half the tests are queryable and half are in a JSON file on an Airflow worker's
disk cannot answer "what is currently failing" in one place.
"""
