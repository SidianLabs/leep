-- Sessions (ADR 0095): coordination state, advisory and expiring. One row
-- per (repository, principal, agent session): what an agent run is doing
-- and where. `origin` is 'declared' or 'inferred'; `state` is 'active',
-- 'landed', 'abandoned' or 'expired'. Nothing here may change a land's
-- answer: dropping these tables loses no history. Old servers never read
-- them, so they tolerate them (ADR 0023).
CREATE TABLE sessions (
    id BIGSERIAL PRIMARY KEY,
    repo_id BIGINT NOT NULL DEFAULT 1,
    principal TEXT NOT NULL,
    agent_session TEXT NOT NULL,
    agent_model TEXT,
    title TEXT NOT NULL DEFAULT '',
    approach TEXT NOT NULL DEFAULT '',
    task_key TEXT,
    target_ref TEXT,
    base BYTEA,
    head BYTEA,
    origin TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'active',
    created BIGINT NOT NULL,
    ended BIGINT,
    landed_as BYTEA
);
CREATE UNIQUE INDEX sessions_key ON sessions (repo_id, principal, agent_session);
CREATE INDEX sessions_state ON sessions (repo_id, state, id);
CREATE INDEX sessions_task ON sessions (repo_id, task_key, id);
CREATE INDEX sessions_ended ON sessions (ended);
-- A session's scope, claims and touched paths. Paths compare bytewise
-- (COLLATE "C") so "who works on or below DIR" is one range scan on
-- (repo_id, path) between DIR || '/' and DIR || '0'.
CREATE TABLE session_paths (
    repo_id BIGINT NOT NULL DEFAULT 1,
    session_id BIGINT NOT NULL,
    kind TEXT NOT NULL,
    path TEXT COLLATE "C" NOT NULL,
    PRIMARY KEY (session_id, kind, path)
);
CREATE INDEX session_paths_path ON session_paths (repo_id, path);
-- When each session was last heard from, apart from the wide row so a
-- heartbeat rewrites a narrow one. UNLOGGED: a crash empties it, which only
-- makes sessions look as old as their creation until they are heard from
-- again.
CREATE UNLOGGED TABLE session_beats (
    session_id BIGINT PRIMARY KEY,
    at BIGINT NOT NULL
);
CREATE INDEX session_beats_at ON session_beats (at);
