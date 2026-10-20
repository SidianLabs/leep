-- Leases (ADR 0056): which server does one kind of work. `scope` names the
-- kind ('land': the server that batches a ref's lands, name = the ref,
-- shard 0); `repo_id` is the default repository (ADR 0054) until
-- repositories exist; `shard` splits one name's work. Times are milliseconds
-- by the database's clock. A lease is only a hint about where work is done;
-- refs still move only by compare-and-swap. Old servers never read this
-- table, so it tolerates them (ADR 0023).
CREATE TABLE leases (
    scope TEXT NOT NULL,
    repo_id BIGINT NOT NULL DEFAULT 1,
    name TEXT NOT NULL,
    shard INTEGER NOT NULL DEFAULT 0,
    holder TEXT NOT NULL,
    address TEXT NOT NULL,
    protocol INTEGER NOT NULL,
    meta TEXT NOT NULL DEFAULT '',
    epoch BIGINT NOT NULL,
    renewed_at BIGINT NOT NULL,
    expires_at BIGINT NOT NULL,
    PRIMARY KEY (scope, repo_id, name, shard)
);
