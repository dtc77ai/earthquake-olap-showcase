.PHONY: help install install-dev setup run test coverage clean benchmark docker-build docker-run lint format

# Default target
.DEFAULT_GOAL := help

# Variables
PYTHON := python3.11
PIP := $(PYTHON) -m pip
PYTEST := $(PYTHON) -m pytest
STREAMLIT := $(PYTHON) -m streamlit
DOCKER_COMPOSE := docker-compose

help: ## Show this help message
	@echo "🌍 Earthquake OLAP Showcase - Available Commands:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install production dependencies
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

install-dev: ## Install development dependencies
	$(PIP) install --upgrade pip
	$(PIP) install -e ".[dev]"

setup: ## Initial project setup (create directories, install deps)
	@echo "🔧 Setting up project..."
	mkdir -p data/{raw,processed,duckdb,cache}
	mkdir -p logs
	mkdir -p benchmark_results
	touch data/raw/.gitkeep data/processed/.gitkeep data/duckdb/.gitkeep
	$(MAKE) install-dev
	@echo "✅ Setup complete!"

run: ## Run the Streamlit application
	$(STREAMLIT) run src/app/main.py

etl: ## Run the ETL pipeline
	$(PYTHON) scripts/run_etl.py

benchmark: ## Run benchmarks
	$(PYTHON) scripts/run_benchmark.py

test: ## Run tests
	$(PYTEST)

test-unit: ## Run unit tests only
	$(PYTEST) -m unit

test-integration: ## Run integration tests only
	$(PYTEST) -m integration

coverage: ## Run tests with coverage report
	$(PYTEST) --cov=src --cov-report=html --cov-report=term
	@echo "📊 Coverage report generated in htmlcov/index.html"

lint: ## Run linter (ruff)
	$(PYTHON) -m ruff check src tests scripts

format: ## Format code with black
	$(PYTHON) -m black src tests scripts

format-check: ## Check code formatting
	$(PYTHON) -m black --check src tests scripts

clean: ## Clean up generated files
	@echo "🧹 Cleaning up..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.coverage" -delete
	rm -rf .pytest_cache
	rm -rf htmlcov
	rm -rf .ruff_cache
	rm -rf dist build *.egg-info
	@echo "✅ Cleanup complete!"

clean-data: ## Clean all data including raw downloads (WARNING: will re-download)
	@echo "⚠️  WARNING: This will delete ALL data files!"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		rm -rf data/raw/* data/processed/* data/duckdb/*.duckdb; \
		echo "✅ All data cleaned!"; \
	else \
		echo "❌ Cancelled"; \
	fi

clean-db: ## Clean database and processed data (keeps raw downloads)
	@echo "🗑️  Cleaning database and processed data..."
	rm -f data/duckdb/*.duckdb
	rm -f data/processed/*.parquet
	@echo "✅ Database and processed data cleaned"
	@echo "💡 Raw data preserved in data/raw/ for faster re-processing"

refresh-data: clean-data etl ## Clean database and re-run ETL pipeline
	@echo "🎉 Data refreshed successfully!"


# Docker targets
docker-build: ## Build Docker image
	$(DOCKER_COMPOSE) build

docker-up: ## Start Docker containers in detached mode
	$(DOCKER_COMPOSE) up -d

docker-run: ## Run application in Docker (foreground)
	$(DOCKER_COMPOSE) up

docker-down: ## Stop Docker containers
	$(DOCKER_COMPOSE) down

docker-logs: ## View Docker logs
	$(DOCKER_COMPOSE) logs -f

docker-restart: ## Restart Docker containers
	$(DOCKER_COMPOSE) restart

docker-clean: ## Remove Docker containers and volumes
	$(DOCKER_COMPOSE) down -v
	docker system prune -f

docker-shell: ## Open shell in running container
	$(DOCKER_COMPOSE) exec earthquake-olap /bin/bash

docker-rebuild: ## Rebuild and restart containers
	$(DOCKER_COMPOSE) down
	$(DOCKER_COMPOSE) build --no-cache
	$(DOCKER_COMPOSE) up -d

# Combined Docker workflow
docker-init: docker-build docker-up ## Build and start Docker containers
	@echo "🎉 Application started! Access at http://localhost:8501"
	@echo "📊 View logs with: make docker-logs"


notebook: ## Start Jupyter notebook
	$(PYTHON) -m jupyter notebook notebooks/

all: clean install-dev test lint ## Run all checks (clean, install, test, lint)

# Incremental ETL commands
etl-incremental: ## Run incremental ETL (only new years)
	$(PYTHON) scripts/run_etl_incremental.py

etl-status: ## Show loaded data status
	$(PYTHON) -c "from src.utils.data_manager import DataManager; dm = DataManager(); import json; print(json.dumps(dm.get_summary(), indent=2))"

etl-reset-year: ## Reset a specific year (usage: make etl-reset-year YEAR=2020)
	$(PYTHON) -c "from src.utils.data_manager import DataManager; dm = DataManager(); dm.clear_year($(YEAR)); print('Year $(YEAR) cleared')"

etl-reset-all: ## Reset all metadata (WARNING: will reprocess everything)
	$(PYTHON) -c "from src.utils.data_manager import DataManager; dm = DataManager(); dm.reset_all(); print('All metadata reset')"

etl-clean-incremental: ## Clean incremental ETL metadata and yearly tables
	@echo "🗑️ Cleaning incremental ETL data..."
	rm -f data/metadata.json
	@echo "✅ Metadata cleared. Yearly tables will be cleaned on next run."


init: setup etl ## Initialize project and run ETL
	@echo "🎉 Project initialized and data loaded!"

