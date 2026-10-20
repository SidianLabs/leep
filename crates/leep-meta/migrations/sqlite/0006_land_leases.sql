-- Land leases (ADR 0056): which server coordinates the lands of a ref.
-- Times are milliseconds by the database's clock. A lease is only a hint
-- about where lands are batched; refs still move only by compare-and-swap.
-- repo_id is the default repository (ADR 0054) until repositories exist.
-- Old servers never read this table, so it tolerates them (ADR 0023).
CREATE TABLE land_leases (
    repo_id INTEGER NOT NULL DEFAULT 1,
    ref_name TEXT NOT NULL,
    holder TEXT NOT NULL,
    address TEXT NOT NULL,
    protocol INTEGER NOT NULL,
    epoch INTEGER NOT NULL,
    renewed_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    PRIMARY KEY (repo_id, ref_name)
);
