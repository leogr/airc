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
docs-check:
	RUSTDOCFLAGS="-D warnings" $(CARGO) doc --locked --no-deps
	$(PYTHON) scripts/check-docs.py
