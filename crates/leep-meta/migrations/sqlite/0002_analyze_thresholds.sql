-- Postgres sets per-table autovacuum analyze thresholds in this migration
-- (ADR 0042). SQLite has no autovacuum or per-table statistics settings, so
-- this step only keeps the two backends' version numbers in step.
SELECT 1;
