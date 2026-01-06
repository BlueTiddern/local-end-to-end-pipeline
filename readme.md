# Local End-to-End Pipeline

A modular, local end-to-end ETL pipeline implemented in Python.  
This repository organizes historical and daily data processing into extract → load → validate → transform stages and uses a Bronze → Silver → Gold layering approach for progressive refinement of data.

---

## Highlights / Core Functionality

- Historical ETL orchestration:
    - Extracts OHCLV (open/high/close/low/volume) market data, company metadata, exchange rates, and macroeconomic data.
    - Loads extracted data into a Bronze layer (MySQL) and applies validations.
    - Runs transformations: ranking/trimming on Bronze, creates Silver DDL, and performs Silver-layer loads.
    - Entry point: `src/historic_load_pipeline.py`.

- Daily pipeline structure:
    - Modular subpackages for daily extract, load, transform, and validation (under `src/daily`).
    - Designed for incremental / scheduled daily updates.

- Dagster-based orchestration:
    - Daily pipelines are orchestrated using Dagster (definitions live under `src/orchestration`).
    - Use Dagster to get reliable scheduling, observability (Dagit), and easy composition of solids/ops.
    - Future commits will add Dagster sensors to trigger pipelines from events (see "Dagster Sensors" below).

- Logging:
    - Centralized logging configuration via `src/logger.py`.
    - Writes pipeline logs and validation/execution logs to `logs/` with date-stamped filenames.

- Helpers and DB utilities:
    - `src/utils.py` contains utilities such as YAML loader, directory creation, MySQL engine/session helpers (SQLModel / SQLAlchemy), table recreation helpers, and data null-handling utilities.
    - Database creation helper: `mysql_connect_create_db(...)`
    - Engine/session helper: `get_engine_session(...)`

---

## Repository Layout

- `src/`
    - `historic_load_pipeline.py` — historical ETL orchestration script (main entrypoint for historical runs).
    - `logger.py` — standardized logging setup for all pipeline stages.
    - `utils.py` — common utilities (YAML loader, DB helpers, directory helpers, null handlers).
    - `daily/` — daily pipeline modules (extract, load, transform, validation).
    - `historical/` — historical extract/load/transform modules (OHCLV, metadata, exchange, macrodata).
    - `bronzeValidation/` — validation routines for Bronze layer.
    - `transform_gold/` — Gold-layer transformations and finalization (if present).
    - `models/` — SQLModel (or SQLAlchemy) models for database tables.
    - `orchestration/` — Dagster pipeline definitions and job/schedule/sensor modules for daily runs.
- `config/` — pipeline configuration files (YAML).
- `data/` — input or sample data files.
- `reports/` — output reports and pipeline artifacts.
- `logs/` — auto-created by logger and populated at runtime.

---

## Prerequisites

- Python 3.8+ (recommended 3.10+)
- MySQL server accessible locally (or remote host)
- System packages for `pymysql` and any other DB drivers as required

Python packages:
- pandas
- pyyaml
- numpy
- sqlmodel
- sqlalchemy
- pymysql
- dagster
- dagit

Install example:
```bash
python -m venv .venv
source .venv/bin/activate
pip install pandas pyyaml numpy sqlmodel sqlalchemy pymysql dagster dagit
```

---

## Configuration

- `config/` holds YAML configuration files used by the pipeline. Use `src/utils.load_yml(path)` to read configurations.
- Database credentials and connection details are required to create/connect the DB. Typical parameters:
    - DB_NAME, DB_USER, DB_HOST, DB_PORT, DB_PASSWORD
- The helper `mysql_connect_create_db(db_name, db_user, host, port, password, create_flag=True)` will create the database if it does not exist.
- `get_engine_session(db_name, db_user, host, port, password)` returns `(engine, session)` for DB operations.

Tip: store credentials in an environment file (`.env`) or in a secrets manager; pass them into your execution environment.

---

## Running the Historical Pipeline

The historical ETL is orchestrated by `src/historic_load_pipeline.py`. It configures logging, runs extracts, loads, validations, then transformations.

Example:
```bash
# from repository root
python src/historic_load_pipeline.py
```

What it does (high level):
- Extract: calls modules to extract OHCLV, company metadata, exchange rates, macro data.
- Load: loads extracted data into Bronze tables.
- Validate: runs validations on Bronze tables and a layer-wide statistic accumulation.
- Transform: runs Bronze ranking/trim, creates Silver DDL and performs Silver load operations.

Logs are written to `logs/pipeline/historical/pipeline_<YYYY-MM-DD>.log` and related validation/execution logs.

---

## Dagster Orchestration (Daily Pipelines)

- Dagster is used to orchestrate the repository's daily pipelines. Pipeline/job definitions, ops/solids, schedules, and (future) sensors are located under `src/orchestration`.
- Dagster provides:
    - a UI (Dagit) for exploration, runs, logs, and debugging,
    - a programmatic API for defining jobs, resources (e.g., DB connections), schedules, and sensors,
    - integration points for local and production deployments.


Resource wiring for improvements:
- Dagster resources should be used to provide DB sessions (using `src/utils.get_engine_session`) and configuration from `config/`.
- Ensure secrets and DB credentials are available to the Dagster process (env vars, vault, or config files).

---

## Dagster Sensors (Planned / Future)

- In a future commit, Dagster sensors will be added to enable event-driven pipeline kickoffs instead of relying only on schedules.
- Example sensor triggers to implement:
    - New-file-in-folder sensor: trigger daily job when a new file arrives in `data/incoming/`.
    - Message-queue sensor: trigger when an upstream system posts to Kafka / an HTTP endpoint.
    - Time-based sensor tied to a business calendar (e.g., skip market holidays).
- Implementation notes:
    - Sensors require the `dagster-daemon` to be running for continuous monitoring.
    - Sensor code will live under `src/orchestration/sensors/` and be registered in the Dagster workspace.
    - When implemented, sensors will:
        - perform lightweight checks for new events,
        - create run requests passing event metadata (file path, timestamp) into the corresponding Dagster job,
        - optionally mark artifacts or move files to processed locations after successful runs.

Planned developer tasks:
- Add sensor implementations and examples (file-based and message-based).
- Provide `workspace.yaml` and `dagster` run documentation with examples.
- Add tests for sensors and their run-request logic.

---


## Development and Troubleshooting

- Ensure `logs/` directory is writable — `src/logger.py` writes date-named logs under `logs/`.
- If a DB password is missing, `utils.mysql_connect_create_db` raises a `ValueError`. Provide DB credentials in the environment or config.
- Use `utils.recreate_table(engine, model)` to drop and recreate a specific table (useful during development).
- When running Dagster locally, check Dagit for step logs and the daemon for sensor/schedule logs.

---

## Extending the Pipeline

- Add new extractors under `src/historical/extract` or `src/daily/extract`.
- Add validation rules under `src/bronzeValidation` or `src/daily/validation`.
- Define/extend SQLModel models in `src/models/` and use `utils.recreate_table` for schema iterations.
- Add Dagster resources for DB connectivity and secrets, and add sensor definitions to `src/orchestration/sensors/`.
- Add unit tests and CI (GitHub Actions) for pipeline stages.

---


## Contributing

Contributions welcome. Please:
1. Open an issue to discuss major changes.
2. Create feature branches for changes.
3. Submit pull requests with clear descriptions and tests where applicable.

---

## License

Specify a license (e.g., MIT, Apache-2.0) in a top-level `LICENSE` file.