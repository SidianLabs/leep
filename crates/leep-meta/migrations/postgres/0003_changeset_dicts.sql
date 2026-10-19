-- Compression dictionaries for changeset rows (ADR 0052). A `changesets`
-- row whose data is a zstd frame was compressed with the dictionary whose
-- id the frame names; the dictionary's own header carries the same id.
-- `leep-server compact-changesets` adds rows; nothing changes or deletes
-- them, since a row's changesets cannot be read without it. The table starts
-- empty, and until a dictionary exists servers store changesets as before,
-- which is why this step needs no data change and no stopped servers.
CREATE TABLE changeset_dicts (id BIGINT PRIMARY KEY, created_at BIGINT NOT NULL,
    data BYTEA NOT NULL);
