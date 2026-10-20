-- Client land ids (ADR 0057). A land that names itself stores its id and the
-- changeset it asked for in the log entry of the ref move it made, in the
-- same transaction, so a retry finds the original result at any age. The
-- index makes an id unique per ref and is the lookup path. Servers of v4 do
-- not write these columns, and leave them NULL.
--
-- `IF NOT EXISTS`, unlike other migrations after 0001: building the index
-- here holds a SHARE lock on ref_log, which blocks ref moves for the whole
-- build. A large deployment adds the columns and builds the index by hand
-- first, with CREATE UNIQUE INDEX CONCURRENTLY (OPERATIONS.md, "Upgrading"),
-- and this migration then finds them done.
ALTER TABLE ref_log ADD COLUMN IF NOT EXISTS land_id BYTEA;
ALTER TABLE ref_log ADD COLUMN IF NOT EXISTS land_changeset BYTEA;
CREATE UNIQUE INDEX IF NOT EXISTS ref_log_land_id ON ref_log (name, land_id) WHERE land_id IS NOT NULL;
