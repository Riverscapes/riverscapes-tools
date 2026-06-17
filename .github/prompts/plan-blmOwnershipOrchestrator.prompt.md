## Plan: BLM Ownership Step Orchestrator

Create a BLM ownership workflow orchestrator that supports both interactive numbered-step execution and non-interactive CLI flags, with a JSON run manifest as the canonical state store and a markdown runlog summary generated from that manifest. Reuse existing vector_prep orchestration and reporting patterns while adding explicit step lifecycle tracking for manual and automated execution.

**Steps**
1. Phase 1 - Discovery Baseline and Contracts
1. Define the step contract for this iteration using your confirmed scope: download data, extract metadata, build layer definitions, run vector_prep, intersect with counties, QA/report, upload.
2. Assign stable step IDs and explicit dependencies so execution order is deterministic. Example dependency chain is linear for v1, with optional future branching and loooping (e.g. iterative improvement) support.
3. Define strict schema and vocabulary for two artifacts:
	- step-catalog.json as the static workflow definition (step_id, order, name, depends_on, mode).
	- step-history.json as an append-only execution ledger (one row/entity per step execution with run_id, step_id, status, execution_mode, executed_at, notes, artifacts, error summary, actor).
4. Produce Phase 1 contract outputs:
	- step-catalog.json and step-catalog.schema.json.
	- step-history.json and step-history.schema.json.
5. Decide run identity behavior: each orchestrator session gets a run_id; current step status shown in CLI is derived from the latest history record per step_id.

2. Phase 2 - Manifest and Runlog Infrastructure
1. Add a small manifest I/O module for BLM ownership orchestration that reads/writes canonical JSON atomically and validates required fields.
2. Add status transition helpers to prevent invalid state transitions and normalize timestamps.
3. Add markdown runlog renderer that creates/updates a summary section from manifest state (source of truth remains JSON).
4. Ensure markdown includes numbered steps, current status, last execution date, and operator notes.

3. Phase 3 - Step Implementations and Wrappers
1. Implement step wrapper functions for all seven scoped steps. Each wrapper records start/completion/failure into manifest and stores notes/artifacts.
2. Wire automated logic by reusing existing vector_prep utilities and metadata scripts where available; for manual/external steps, provide guided prompts and status update pathways.
3. Make each step invocable independently from CLI and callable from interactive menu.
4. Add idempotency safeguards per step (precondition checks, output existence checks, and clear rerun behavior).

4. Phase 4 - Entry Point UX (Interactive + Flags)
1. Build a main entry script for blm_ownership with both modes:
2. Interactive mode: shows numbered steps with most recent status/date and prompts user for next step.
3. Flag mode: supports selecting one or more steps, optional force rerun, optional notes, and non-interactive status updates for manual steps.
4. Add concise terminal output that mirrors manifest updates and points to generated markdown summary.

5. Phase 5 - Validation and Hardening
1. Add unit tests for manifest schema validation, status transitions, timestamp handling, and runlog rendering.
2. Add integration-style tests for at least one automated step execution path and one manual step status update path.
3. Run project test commands and perform a dry run in the blm_ownership directory to verify state persistence and menu behavior.
4. Document usage and operator workflow in layer README notes, including examples for interactive and flag-based usage.

**Relevant files**
- /workspaces/riverscapes-tools/packages/vector_prep/vector_prep/vector_prep_cli.py — reuse questionary-based selector/prompt interaction patterns for interactive menu UX.
- /workspaces/riverscapes-tools/packages/vector_prep/vector_prep/orchestrate-nhdplushr.py — reuse argparse structure, selective step execution patterns, and orchestration style.
- /workspaces/riverscapes-tools/packages/vector_prep/vector_prep/orchestrate.py — reuse step function decomposition and logger setup pattern.
- /workspaces/riverscapes-tools/packages/vector_prep/vector_prep/lib/report.py — reuse markdown report generation style for runlog summary rendering.
- /workspaces/riverscapes-tools/packages/vector_prep/vector_prep/layers/blm_ownership/inputs.json — stabilize inputs and align orchestration parameters for this layer.
- /workspaces/riverscapes-tools/packages/vector_prep/vector_prep/layers/blm_ownership/orchestrate-blm-ownership.py — new primary entry point for interactive and flag modes.
- /workspaces/riverscapes-tools/packages/vector_prep/vector_prep/layers/blm_ownership/step-catalog.json — workflow definition (stable step metadata and dependencies).
- /workspaces/riverscapes-tools/packages/vector_prep/vector_prep/layers/blm_ownership/step-catalog.schema.json — schema for step-catalog.json.
- /workspaces/riverscapes-tools/packages/vector_prep/vector_prep/layers/blm_ownership/step-history.json — append-only step execution ledger.
- /workspaces/riverscapes-tools/packages/vector_prep/vector_prep/layers/blm_ownership/step-history.schema.json — schema for step-history.json.
- /workspaces/riverscapes-tools/packages/vector_prep/vector_prep/layers/blm_ownership/runlog-blm-ownership.md — generated/updated markdown status summary from manifest.
- /workspaces/riverscapes-tools/packages/vector_prep/tests/test_blm_ownership_orchestrator.py — new tests for manifest, transitions, and command routing.

**Verification**
1. Validate step-catalog and step-history schemas plus transition rules with unit tests via uv run pytest targeting new test module.
2. Execute dry-run command in flag mode to list steps/statuses without mutating outputs and verify displayed most-recent status/date.
3. Execute one automated step (for example metadata extraction) and confirm step-history appends a new record with status, timestamp, notes, and artifacts.
4. Execute one manual step update path and confirm it records status using done state plus execution_mode=manual.
5. Regenerate markdown runlog summary and verify it reflects the latest derived state from step-catalog plus step-history for all seven steps.
6. Re-run selected step with force option and verify a new history record is appended while previous records remain unchanged.

**Decisions**
- Canonical state model: split JSON artifacts in the blm_ownership layer folder:
	- step-catalog.json for workflow definition.
	- step-history.json as append-only execution ledger.
- Manual and automated completion share done status; distinction is captured via execution_mode field.
- Entry point supports both interactive menu and non-interactive CLI flags.
- Markdown runlog is generated/updated from structured catalog plus history and is not the source of truth.
- In-scope v1 steps: download, metadata extraction, layer definition build, vector_prep run, county intersection, QA/report, upload.
- Out-of-scope for v1: cross-layer generalized orchestrator framework and migration of legacy runlogs for other layers.

**Further Considerations**
1. County intersection implementation source: Option A reuse existing geoprocess helper if present; Option B embed layer-specific geopandas logic in this orchestrator; recommendation is Option A if helper exists to reduce duplication.
2. Upload step semantics: Option A status-only manual checkpoint; Option B optional scripted upload adapter behind feature flag; recommendation is Option A for v1 to keep credentials/security out of initial scope.
3. Step-history retention policy: Option A keep full append-only history; Option B archive old records periodically; recommendation is Option A initially, with archival only if file size becomes operationally problematic.
