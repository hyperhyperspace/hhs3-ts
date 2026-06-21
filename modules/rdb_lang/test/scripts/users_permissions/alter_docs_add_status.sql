ALTER SCHEMA docs_schema AS (
  ADD COLUMN docs.status string DEFAULT 'draft'
);
