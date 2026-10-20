-- Format features the store's rows use (ADR 0055). A row names something
-- servers must understand to read the store; a server refuses to open a
-- store naming a feature it does not know. `leep-server compact-changesets`
-- adds 'changeset-zstd-dict' before it stores a dictionary. A store that
-- already has one (compacted before this step) gets the row here.
CREATE TABLE store_features (name TEXT PRIMARY KEY, added_at INTEGER NOT NULL);
INSERT INTO store_features (name, added_at)
    SELECT 'changeset-zstd-dict', CAST(strftime('%s', 'now') AS INTEGER)
    WHERE EXISTS (SELECT 1 FROM changeset_dicts);
