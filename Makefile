.PHONY: up down restart logs setup dbt-bootstrap dbt-deps dbt-run dbt-test dbt-compile dbt-shell clickhouse-client dagster clean help

# Detect docker compose command (plugin vs standalone)
DOCKER_COMPOSE := $(shell docker compose version >/dev/null 2>&1 && echo docker compose || echo docker-compose)

# ---------------------------------------------------------------------------
# Infrastructure
# ---------------------------------------------------------------------------

## Start ClickHouse
up:
	$(DOCKER_COMPOSE) up -d --build
	@echo ""
	@echo "=========================================="
	@echo "  ClickHouse starting up..."
	@echo "=========================================="
	@echo "  ClickHouse HTTP:   http://localhost:$${CLICKHOUSE_EXTERNAL_PORT:-8123}/play"
	@echo "  Dagster UI:        http://localhost:3000"
	@echo "=========================================="
	@echo ""
	@echo "  Raw data is in data/raw/ — inspect it before building your pipeline."
	@echo "  Run 'make setup' to install Python dependencies."
	@echo ""

## Stop all services and remove volumes (clean slate on next start)
down:
	$(DOCKER_COMPOSE) down -v

## Restart all services
restart: down up

## Follow logs for all services
logs:
	$(DOCKER_COMPOSE) logs -f

# ---------------------------------------------------------------------------
# Python setup
# ---------------------------------------------------------------------------

## Install Python dependencies (uses pip + pyproject.toml)
setup:
	pip install -e ".[dev]"

# ---------------------------------------------------------------------------
# dbt commands (run inside Docker)
# ---------------------------------------------------------------------------

## Install packages, run models, and compile artifacts in one step
dbt-bootstrap:
	$(DOCKER_COMPOSE) run --rm --build dbt deps
	$(DOCKER_COMPOSE) run --rm dbt run
	$(DOCKER_COMPOSE) run --rm dbt compile

## Install dbt packages
dbt-deps:
	$(DOCKER_COMPOSE) run --rm --build dbt deps

## Run all dbt models
dbt-run:
	$(DOCKER_COMPOSE) run --rm dbt run

## Run all dbt tests
dbt-test:
	$(DOCKER_COMPOSE) run --rm dbt test

## Compile dbt project
dbt-compile:
	$(DOCKER_COMPOSE) run --rm dbt compile

## Open a shell in the dbt container
dbt-shell:
	$(DOCKER_COMPOSE) run --rm --entrypoint /bin/bash dbt

# ---------------------------------------------------------------------------
# Dagster
# ---------------------------------------------------------------------------

## Start Dagster dev server (runs in Docker)
dagster:
	$(DOCKER_COMPOSE) up -d --build dagster
	@echo ""
	@echo "  Dagster UI: http://localhost:3000"
	@echo ""

# ---------------------------------------------------------------------------
# Database access
# ---------------------------------------------------------------------------

## Open ClickHouse client
clickhouse-client:
	$(DOCKER_COMPOSE) exec clickhouse clickhouse-client

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

## Remove all Docker volumes and build artifacts
clean:
	$(DOCKER_COMPOSE) down -v
	@docker run --rm -v "$$(pwd)/dbt:/work" alpine:3.20 sh -c "rm -rf /work/target /work/dbt_packages /work/logs" 2>/dev/null || true
	@rm -rf dbt/target dbt/dbt_packages dbt/logs 2>/dev/null || true
	@echo "Cleaned all volumes and build artifacts."

## Show available commands
help:
	@echo "Available targets:"
	@echo ""
	@grep -E '^## ' Makefile | sed 's/^## /  /'
	@echo ""
	@echo "Usage: make <target>"

