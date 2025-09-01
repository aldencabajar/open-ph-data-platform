SHELL:=/bin/bash
.SHELLFLAGS:= -e -o pipefail -c
WORKING_DIR:= $(shell pwd)
BUILD_FOLDER=_build
BUILD_FOLDER_FULL_PATH:=$(WORKING_DIR)/$(BUILD_FOLDER)

# setting up ducklake metadata database variables
DL_METADATA_FILE=metadata.sqlite
DL_METADATA_FULL_PATH:=$(BUILD_FOLDER)/$(DL_METADATA_FILE)
DL_METADATA_CONN:=sqlite:$(DL_METADATA_FILE)

VENV_PYTHON:=$(WORKING_DIR)/.venv/bin/python
DUCKDB_BIN := $(HOME)/.local/bin/duckdb

$(DUCKDB_BIN):
	curl https://install.duckdb.org | sh

# Bootstrapping the duckdb database
$(DL_METADATA_FULL_PATH): $(DUCKDB_BIN)
	cd $(BUILD_FOLDER) && $(DUCKDB_BIN) < "$(WORKING_DIR)/scripts/startup.sql"

init-db: $(DL_METADATA_FULL_PATH)

.PHONY: init-db


# installing dependencies
$(VENV_PYTHON):
	poetry config virtualenvs.in-project true --local && poetry install
	

# ingestion of data sources
ingest: init-db $(VENV_PYTHON)
	WORKING_DIR=$(WORKING_DIR) BUILD_FOLDER=$(WORKING_DIR)/$(BUILD_FOLDER) DL_METADATA_CONN=$(DL_METADATA_CONN) VENV_PYTHON=$(VENV_PYTHON) \
	bash -c 'source scripts/build.sh && ingest'
.PHONY: ingest

# transformation using dbt
staging: ingest
	BUILD_FOLDER=$(WORKING_DIR)/$(BUILD_FOLDER) \
	  DBT_PATH=$(WORKING_DIR)/.venv/bin/dbt \
	  WORKING_DIR=$(WORKING_DIR) \
	bash -c 'source scripts/build.sh && staging'

.PHONY: staging

final: staging
	BUILD_FOLDER=$(WORKING_DIR)/$(BUILD_FOLDER) \
	  DBT_PATH=$(WORKING_DIR)/.venv/bin/dbt \
	  WORKING_DIR=$(WORKING_DIR) \
	bash -c 'source scripts/build.sh && final'

.PHONY: final

# running tests
test: final
	BUILD_FOLDER=$(WORKING_DIR)/$(BUILD_FOLDER) \
	DBT_PATH=$(WORKING_DIR)/.venv/bin/dbt \
      WORKING_DIR=$(WORKING_DIR) \
      bash -c 'source scripts/build.sh && test "$(SELECT)"'

.PHONY: test

destroy:
	./scripts/clean.sh

.PHONY: destroy


print-%:
	@echo '$*=$($*)'




