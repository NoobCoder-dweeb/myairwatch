# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 🧠 Workflow Orchestration

### 1. Plan Node Default

- Enter **plan mode** for ANY non-trivial task (3+ steps or architectural decisions)
- If something goes sideways, **STOP and re-plan immediately** — don't keep pushing
- Use plan mode for **verification steps**, not just building
- Write detailed specs upfront to reduce ambiguity
- Never start implementation without a written plan in `tasks/todo.md`

### 2. Subagent Strategy

- Use subagents **liberally** to keep the main context window clean
- Offload research, exploration, and parallel analysis to subagents
- For complex problems, throw more compute at it via subagents
- One task per subagent for focused execution

### 3. Self-Improvement Loop

- After **ANY correction** from the user: update `tasks/lessons.md` with the pattern
- Write rules for yourself that prevent the same mistake
- Ruthlessly iterate on these lessons until mistake rate drops
- Review `tasks/lessons.md` at session start for every relevant project

### 4. Verification Before Done

- **Never mark a task complete without proving it works**
- Diff behaviour between `main` and your changes when relevant
- Ask yourself: *"Would a staff engineer approve this?"*
- Run tests, check logs, demonstrate correctness before closing the task

### 5. Demand Elegance (Balanced)

- For non-trivial changes: pause and ask *"Is there a more elegant way?"*
- If a fix feels hacky: *"Knowing everything I know now, implement the elegant solution"*
- Skip this for simple, obvious fixes — don't over-engineer
- Challenge your own work before presenting it

### 6. Autonomous Bug Fixing

- When given a bug report: **just fix it** — don't ask for hand-holding
- Point at logs, errors, failing tests — then resolve them
- Zero context-switching required from the user
- Go fix failing CI tests without being told how

---

## ✅ Task Management Protocol

| Step | Action | Where |
| ------ | -------- | -------- |
| 1 | **Plan First** — write plan with checkable items | `tasks/todo.md` |
| 2 | **Verify Plan** — check in before starting implementation | (user confirmation) |
| 3 | **Track Progress** — mark items complete as you go | `tasks/todo.md` |
| 4 | **Explain Changes** — high-level summary at each step | inline comments |
| 5 | **Document Results** — add review section after completion | `tasks/todo.md` |
| 6 | **Capture Lessons** — update after any correction | `tasks/lessons.md` |

### `tasks/todo.md` format

```markdown
## Plan: [Task Name]
- [ ] Step 1 — description
- [ ] Step 2 — description
- [x] Step 3 — DONE

## Review
- What worked: ...
- What was tricky: ...
- Lessons learned: ...
```

### `tasks/lessons.md` format

```markdown
## Lesson [N] — [Date]
**Mistake:** What went wrong  
**Root Cause:** Why it happened  
**Rule:** Never do X; always do Y instead  
**Pattern:** Code or logic snippet showing the fix
```

---

## 🏗️ Core Principles

| Principle | Rule |
| ----------- | ------ |
| **Simplicity First** | Make every change as simple as possible. Impact minimal code. |
| **No Laziness** | Find root causes. No temporary fixes. Senior developer standards. |
| **Minimal Impact** | Changes should only touch what's necessary. Avoid introducing bugs. |
| **Prove It Works** | No task is done until tests pass and correctness is demonstrated. |
| **Leave It Better** | Every file you touch should be cleaner than you found it. |

---

## 🔍 Code Quality Gates

Before marking **any** task complete, run this checklist:

```text
[ ] Tests pass locally
[ ] No new linting errors introduced
[ ] Edge cases considered and handled
[ ] Error messages are actionable
[ ] No hardcoded secrets or environment values
[ ] Logging is sufficient to debug without a debugger
[ ] "Staff engineer approval" gut check passed
```

---

## 📁 Project File Conventions

```text
tasks/
  todo.md          ← active plan + progress tracking
  lessons.md       ← self-improvement log
  decisions.md     ← architectural decision records (ADRs)
src/               ← application code
tests/             ← test suite (mirrors src/ structure)
docs/              ← human-readable documentation
CLAUDE.md          ← this file (project root)
```

---

## ⚠️ Guardrails

- **Never delete** `tasks/lessons.md` — it is institutional memory
- **Never skip** plan mode because a task "seems simple"
- **Never assume** a fix works without running it
- **Never leave** a half-broken state — always restore to working before stopping
- **Never ignore** a failing test — fix it or explicitly document why it's skipped

---

## 🚀 Session Startup Checklist

When starting a new session on an existing project:

```text
1. Read CLAUDE.md (this file)
2. Read tasks/lessons.md — absorb all past lessons
3. Read tasks/todo.md — understand current state
4. Confirm scope with user before writing any code
5. Begin plan mode for the session's task
```

---

*This file is source-controlled. Update it when project conventions evolve.*

## Project Overview

MyAirWatch is a lakehouse-style data pipeline for Malaysia air quality analytics. It aggregates air quality data from OpenDOSM and OpenAQ APIs, processes it using local PySpark, stores in GCS-compatible local storage, and presents analytics via a Streamlit dashboard connected to BigQuery Sandbox.

## Architecture

The project follows a medallion architecture (bronze → silver → gold) with the following layers:

```text
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

```text
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
