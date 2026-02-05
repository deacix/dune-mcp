# =============================================================================
# Dune MCP Server Makefile
# Location: dune/mcp/
# =============================================================================

.PHONY: help install install-dev test test-cov lint format clean build
.PHONY: release release-patch release-minor release-major release-rc
.PHONY: version changelog

# Current version from pyproject.toml
VERSION := $(shell grep -m1 'version = ' pyproject.toml | cut -d'"' -f2)

help:
	@echo "Dune MCP Server Commands:"
	@echo ""
	@echo "  Setup:"
	@echo "    make install       Install production dependencies"
	@echo "    make install-dev   Install development dependencies"
	@echo ""
	@echo "  Testing:"
	@echo "    make test          Run tests with coverage (80% threshold)"
	@echo "    make test-cov      Run tests and generate HTML coverage report"
	@echo ""
	@echo "  Development:"
	@echo "    make lint          Run linting with ruff"
	@echo "    make format        Format code with ruff"
	@echo "    make clean         Remove cache and build files"
	@echo "    make build         Build package distribution"
	@echo ""
	@echo "  Release (gitflow):"
	@echo "    make version       Show current version"
	@echo "    make release-patch Create patch release (0.1.0 -> 0.1.1)"
	@echo "    make release-minor Create minor release (0.1.0 -> 0.2.0)"
	@echo "    make release-major Create major release (0.1.0 -> 1.0.0)"
	@echo "    make release-rc    Create release candidate (0.1.0 -> 0.2.0-rc.1)"
	@echo "    make changelog     Generate changelog from git commits"

# =============================================================================
# Setup
# =============================================================================

install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"

# =============================================================================
# Testing
# =============================================================================

test:
	pytest tests/ -v --cov=. --cov-report=term-missing --cov-fail-under=80

test-cov:
	pytest tests/ -v --cov=. --cov-report=term-missing --cov-report=html --cov-fail-under=80
	@echo "Coverage report: htmlcov/index.html"

# =============================================================================
# Development
# =============================================================================

lint:
	ruff check .

format:
	ruff format .

clean:
	rm -rf __pycache__
	rm -rf tests/__pycache__
	rm -rf .pytest_cache
	rm -rf .coverage
	rm -rf htmlcov
	rm -rf *.egg-info
	rm -rf dist
	rm -rf build
	find . -name "*.pyc" -delete

# =============================================================================
# Build
# =============================================================================

build: clean
	pip install build
	python -m build

# =============================================================================
# Release (Gitflow)
# =============================================================================

version:
	@echo "Current version: $(VERSION)"

# Helper to bump version in pyproject.toml
define bump_version
	@echo "Current version: $(VERSION)"
	@echo "New version: $(1)"
	@sed -i.bak 's/version = "$(VERSION)"/version = "$(1)"/' pyproject.toml && rm pyproject.toml.bak
	@echo "Updated pyproject.toml to version $(1)"
endef

# Create a release: bump version, commit, tag, push
define create_release
	$(call bump_version,$(1))
	@git add pyproject.toml
	@git commit -m "chore: bump version to $(1)"
	@git tag -a "v$(1)" -m "Release v$(1)"
	@echo ""
	@echo "✅ Created release v$(1)"
	@echo ""
	@echo "To publish, run:"
	@echo "  git push origin main --tags"
endef

release-patch:
	@$(eval MAJOR := $(shell echo $(VERSION) | cut -d. -f1))
	@$(eval MINOR := $(shell echo $(VERSION) | cut -d. -f2))
	@$(eval PATCH := $(shell echo $(VERSION) | cut -d. -f3 | cut -d- -f1))
	@$(eval NEW_PATCH := $(shell echo $$(($(PATCH) + 1))))
	@$(eval NEW_VERSION := $(MAJOR).$(MINOR).$(NEW_PATCH))
	$(call create_release,$(NEW_VERSION))

release-minor:
	@$(eval MAJOR := $(shell echo $(VERSION) | cut -d. -f1))
	@$(eval MINOR := $(shell echo $(VERSION) | cut -d. -f2))
	@$(eval NEW_MINOR := $(shell echo $$(($(MINOR) + 1))))
	@$(eval NEW_VERSION := $(MAJOR).$(NEW_MINOR).0)
	$(call create_release,$(NEW_VERSION))

release-major:
	@$(eval MAJOR := $(shell echo $(VERSION) | cut -d. -f1))
	@$(eval NEW_MAJOR := $(shell echo $$(($(MAJOR) + 1))))
	@$(eval NEW_VERSION := $(NEW_MAJOR).0.0)
	$(call create_release,$(NEW_VERSION))

release-rc:
	@$(eval MAJOR := $(shell echo $(VERSION) | cut -d. -f1))
	@$(eval MINOR := $(shell echo $(VERSION) | cut -d. -f2))
	@$(eval NEW_MINOR := $(shell echo $$(($(MINOR) + 1))))
	@$(eval RC_NUM := $(shell echo $(VERSION) | grep -oE 'rc\.[0-9]+' | cut -d. -f2 || echo 0))
	@$(eval NEW_RC := $(shell echo $$(($(RC_NUM) + 1))))
	@if echo "$(VERSION)" | grep -q "rc"; then \
		$(eval NEW_VERSION := $(MAJOR).$(MINOR).0-rc.$(NEW_RC)); \
	else \
		$(eval NEW_VERSION := $(MAJOR).$(NEW_MINOR).0-rc.1); \
	fi
	$(call create_release,$(NEW_VERSION))

changelog:
	@echo "# Changelog"
	@echo ""
	@echo "## Unreleased"
	@echo ""
	@git log --pretty=format:"- %s (%h)" $$(git describe --tags --abbrev=0 2>/dev/null || echo "")..HEAD 2>/dev/null || \
		git log --pretty=format:"- %s (%h)"
	@echo ""
