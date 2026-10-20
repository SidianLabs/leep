-- Client land ids (ADR 0057). A land that names itself stores its id and the
-- changeset it asked for in the log entry of the ref move it made, in the
-- same transaction, so a retry finds the original result at any age. The
-- index makes an id unique per ref and is the lookup path. Servers of v4 do
-- not write these columns, and leave them NULL.
ALTER TABLE ref_log ADD COLUMN land_id BLOB;
ALTER TABLE ref_log ADD COLUMN land_changeset BLOB;
CREATE UNIQUE INDEX ref_log_land_id ON ref_log (name, land_id) WHERE land_id IS NOT NULL;
