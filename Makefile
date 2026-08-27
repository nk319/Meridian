.PHONY: help seed test lint clean

help:
	@grep -E '^[a-z-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

seed:  ## Generate all source data into seeds/
	PYTHONPATH=src python3 -m meridian.seed --out seeds/

test:  ## Run the test suite
	python3 -m pytest

lint:  ## Lint and format check
	ruff check src tests
	ruff format --check src tests

clean:  ## Remove generated data
	rm -rf seeds/ .pytest_cache/ .ruff_cache/
	find . -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null || true
