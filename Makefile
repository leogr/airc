CARGO ?= cargo
PYTHON ?= python3

.PHONY: check fmt lint test docs-check
check: fmt lint test docs-check

fmt:
	$(CARGO) fmt --all -- --check
lint:
	$(CARGO) clippy --locked --all-targets -- -D warnings
test:
	$(CARGO) test --locked --all-targets
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) -m unittest discover -s scripts -p 'test_airc_monitor.py'
docs-check:
	RUSTDOCFLAGS="-D warnings" $(CARGO) doc --locked --no-deps
	$(PYTHON) scripts/check-docs.py
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) -m unittest discover -s scripts -p 'test_check_docs.py'
