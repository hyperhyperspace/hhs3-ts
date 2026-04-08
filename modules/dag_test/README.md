# DAG Test

Shared test suites for the **`dag`** module, reusable across storage backends. Provides:

- **`createBackendTestSuite(factory)`** — Tests basic append/load, frontier tracking, fork analysis, and filtered cover finding against any `DagFactory`.
- **`createParitySuite(factory)`** — Verifies that a backend produces identical results to the in-memory reference implementation on pseudo-randomly generated DAGs.
- **DAG creation helpers** — `createD1`, `createD3`, `createRandomDag`, `createRandomDags`, `createRandomBranchingDags` for building deterministic test fixtures.

Used by `dag` (for in-memory stores), `dag_sqlite` (for SQL-backed stores), and any future backend.
