# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

MyAirWatch is a lakehouse-style data pipeline for Malaysia air quality analytics. It aggregates air quality data from OpenDOSM and OpenAQ APIs, processes it using local PySpark, stores in GCS-compatible local storage, and presents analytics via a Streamlit dashboard connected to BigQuery Sandbox.

## Architecture

The project follows a medallion architecture (bronze → silver → gold) with the following layers:

```
data/           → Storage layers (bronze/silver/gold in Parquet format)
docs/           → Documentation and architecture decisions
notebooks/      → Jupyter exploration and experimentation
src/            → Engineering layer (ETL pipelines)
streamlit_app/  → Presentation layer (dashboard)
sql/            → BigQuery warehouse logic
```

### Data Flow

- **Bronze**: Raw API data from OpenAQ and OpenDOSM
- **Silver**: Cleaned and deduplicated air quality readings
- **Gold**: Aggregated summaries (monthly state pollution, haze season, pollutant health risks)

### Tech Stack

- **Processing**: PySpark (local mode)
- **Storage**: Parquet files (GCS-compatible)
- **Warehouse**: BigQuery Sandbox
- **Dashboard**: Streamlit
- **APIs**: OpenAQ, OpenDOSM

## Common Commands

```bash
# Install dependencies (first time only)
.venv/bin/python3 -m pip install -r requirements.txt

# Run the Streamlit dashboard
streamlit run streamlit_app/app.py

# Run OpenAQ extraction (requires OPENAQ_API_KEY in .env)
python -m src.extract.openaq_extract

# Run OpenDOSM extraction
python -m src.extract.opendosm_extract

# Run a single notebook cell (in VS Code with Jupyter)
# Use the mcp__ide__executeCode tool

# Activate virtual environment
source .venv/bin/activate
```

## Environment Setup

Create a `.env` file based on `.env.example`:

```
OPENAQ_API_KEY=your_key_from_explore_openaq_org
OPENDOSM_API_KEY=your_key_if_needed
```

The extraction scripts automatically load `.env` via `load_dotenv()`.

## Code Structure

The `src/` directory follows a standard ETL pattern:

- `src/extract/` - Data extraction from APIs (openaq_extract.py, opendosm_extract.py)
- `src/transform/` - Data cleaning and transformation (clean_air_quality.py)
- `src/load/` - Data loading to storage layers
- `src/quality/` - Data quality checks
- `src/utils/` - Shared utilities

## Development Notes

- Cost control is enforced through: local Spark (no managed clusters), BigQuery Sandbox (free tier), Parquet format (columnar compression), and strategic partitioning
- Data is partitioned by date and/or state for query performance
- The project uses virtual environment at `.venv/` - use `.venv/bin/python3` directly or activate before running code
- API keys are stored in `.env` and loaded via `python-dotenv` - never commit actual keys to version control

## Key Files

- `README.md` - Project overview and high-level architecture
- `docs/architecture.md` - Detailed architecture decisions
- `docs/cost_control.md` - Cost optimization strategies
- `docs/reflection.md` - Lessons learned and challenges
