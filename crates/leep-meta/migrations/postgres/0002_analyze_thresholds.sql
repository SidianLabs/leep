-- Re-analyze the two large tables after a bounded amount of change instead
-- of the default 50 rows plus 10% of the table (ADR 0042). Storage
-- parameters only: no data, column or index changes, and `ALTER TABLE ...
-- SET` takes a lock that does not block reads or writes.
--
-- `derived` holds every kind in one table, so a new kind (an index being
-- built, say) is a small share of it until it is complete. On an 11M-row
-- table the default waits for 1.1M changed rows; a 512k-row kind never got
-- there, the planner kept estimating a row or two for it, and lookups by key
-- walked the whole kind. 1% plus 10,000 rows re-analyzes after about 120k
-- rows there, while a new kind is still being written. ANALYZE reads a fixed
-- sample (30,000 rows at the default statistics target), so the cost per run
-- does not grow with the table, and autovacuum runs it at most once per
-- `autovacuum_naptime` (1 minute by default).
ALTER TABLE derived SET (autovacuum_analyze_scale_factor = 0.01,
    autovacuum_analyze_threshold = 10000);
-- `changesets` only grows. Its ids are uniformly distributed hashes, so the
-- statistics that matter are the row count and the id histogram's range;
-- 2% plus 10,000 rows (40k changesets on a 1.5M-changeset history) keeps
-- them current through an import without analyzing on every few lands.
ALTER TABLE changesets SET (autovacuum_analyze_scale_factor = 0.02,
    autovacuum_analyze_threshold = 10000);
