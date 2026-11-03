-- Review requests (ADR 0086): one row per request, opened by a principal
-- (`requester`, a token name) for a changeset it means to land on
-- `target_ref`, and closed when it lands or is withdrawn. `state` is
-- 'open', 'landed' or 'withdrawn'. The reviewers asked are rows of
-- `review_request_reviewers`; a request with none is open to any reviewer.
-- `repo_id` is the default repository (ADR 0054) until repositories exist.
-- Old servers never read these tables, so they tolerate them (ADR 0023).
CREATE TABLE review_requests (
    id BIGSERIAL PRIMARY KEY,
    repo_id BIGINT NOT NULL DEFAULT 1,
    changeset BYTEA NOT NULL,
    target_ref TEXT NOT NULL,
    requester TEXT NOT NULL,
    message TEXT NOT NULL DEFAULT '',
    created BIGINT NOT NULL,
    state TEXT NOT NULL DEFAULT 'open',
    closed BIGINT,
    landed_as BYTEA
);
CREATE INDEX review_requests_open ON review_requests (state, target_ref, id);
CREATE INDEX review_requests_requester ON review_requests (requester, id);
CREATE INDEX review_requests_changeset ON review_requests (changeset);
CREATE TABLE review_request_reviewers (
    request_id BIGINT NOT NULL,
    reviewer TEXT NOT NULL,
    PRIMARY KEY (request_id, reviewer)
);
CREATE INDEX review_request_reviewers_reviewer ON review_request_reviewers (reviewer, request_id);
