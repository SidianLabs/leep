//! Sessions on Postgres and SQLite (ADR 0095): a `sessions` row per
//! (repository, principal, agent session), its scope, claims and touched
//! paths as `session_paths` rows, and when it was last heard from in the
//! narrow `session_beats` table, so a heartbeat never rewrites the wide row.
//!
//! An upsert is one transaction that holds the session's row (Postgres
//! `FOR UPDATE`, SQLite `BEGIN IMMEDIATE`) while it merges the update, so
//! concurrent upserts never lose each other's paths. An end is one
//! conditional update from `active`. These stores share the meta store's
//! data pool: coordination statements never take the connections reserved
//! for land leaders (ADR 0067).

use std::collections::BTreeMap;

use async_trait::async_trait;
use leep_core::coord::{
    ancestors, apply_update, check_end, Beat, CoordStore, NearQuery, NearRow, PathKind, Session,
    SessionEnd, SessionFilter, SessionOrigin, SessionState, SessionUpdate, SweepReport,
    EXPIRE_AFTER_SECS, PATHS_RETENTION_SECS, SESSION_RETENTION_SECS, TOO_MANY_SESSIONS_MARKER,
};
use leep_core::{ChangesetId, Error, Result};
use sqlx::{PgPool, Postgres, QueryBuilder, Row, Sqlite, SqlitePool};

use crate::{storage, to_id};

/// The schema version that added the tables.
pub(crate) const SESSIONS_SCHEMA_VERSION: i64 = 9;

/// Beats written per SQLite transaction.
const BEATS_PER_TRANSACTION: usize = 500;

const COLUMNS: &str = "s.id, s.principal, s.agent_session, s.agent_model, s.title, s.approach, \
     s.task_key, s.target_ref, s.base, s.head, s.origin, s.state, s.created, s.ended, \
     s.landed_as, b.at AS beat";
const FROM: &str = "FROM sessions s LEFT JOIN session_beats b ON b.session_id = s.id";

fn to_u64(v: i64) -> Result<u64> {
    u64::try_from(v).map_err(|_| storage("negative session id in db"))
}

fn to_i64(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

fn bytes(id: Option<ChangesetId>) -> Option<Vec<u8>> {
    id.map(|c| c.0.as_bytes().to_vec())
}

fn too_many(principal: &str, max: usize) -> Error {
    Error::Invalid(format!(
        "{principal} has {max} {TOO_MANY_SESSIONS_MARKER} ({max}); end some (`leep session end`) \
         or let them expire"
    ))
}

trait SessionRow {
    fn session(&self) -> Result<Session>;
    /// The `path_kind` and `path_text` columns of a `near_paths` row.
    fn path_row(&self) -> Result<(String, String)>;
}

macro_rules! session_row {
    ($row:ty) => {
        impl SessionRow for $row {
            fn session(&self) -> Result<Session> {
                let id = |c: &str| -> Result<Option<ChangesetId>> {
                    let v: Option<Vec<u8>> = self.try_get(c).map_err(storage)?;
                    v.as_deref().map(to_id).transpose()
                };
                let created: i64 = self.try_get("created").map_err(storage)?;
                let beat: Option<i64> = self.try_get("beat").map_err(storage)?;
                Ok(Session {
                    id: to_u64(self.try_get("id").map_err(storage)?)?,
                    principal: self.try_get("principal").map_err(storage)?,
                    agent_session: self.try_get("agent_session").map_err(storage)?,
                    agent_model: self.try_get("agent_model").map_err(storage)?,
                    title: self.try_get("title").map_err(storage)?,
                    approach: self.try_get("approach").map_err(storage)?,
                    task_key: self.try_get("task_key").map_err(storage)?,
                    target_ref: self.try_get("target_ref").map_err(storage)?,
                    base: id("base")?,
                    head: id("head")?,
                    origin: SessionOrigin::parse(
                        &self.try_get::<String, _>("origin").map_err(storage)?,
                    )
                    .map_err(storage)?,
                    state: SessionState::parse(
                        &self.try_get::<String, _>("state").map_err(storage)?,
                    )
                    .map_err(storage)?,
                    created,
                    last_seen: beat.unwrap_or(created).max(created),
                    ended: self.try_get("ended").map_err(storage)?,
                    landed_as: id("landed_as")?,
                    scope: Vec::new(),
                    claims: Vec::new(),
                    touched: Vec::new(),
                })
            }

            fn path_row(&self) -> Result<(String, String)> {
                Ok((
                    self.try_get("path_kind").map_err(storage)?,
                    self.try_get("path_text").map_err(storage)?,
                ))
            }
        }
    };
}
session_row!(sqlx::postgres::PgRow);
session_row!(sqlx::sqlite::SqliteRow);

/// Put each path row on its session, in path order.
fn attach(mut sessions: Vec<Session>, rows: Vec<(i64, String, String)>) -> Result<Vec<Session>> {
    let mut by: BTreeMap<u64, Vec<(PathKind, String)>> = BTreeMap::new();
    for (id, kind, path) in rows {
        let kind = PathKind::parse(&kind).map_err(storage)?;
        by.entry(id as u64).or_default().push((kind, path));
    }
    for s in &mut sessions {
        if let Some(mut paths) = by.remove(&s.id) {
            paths.sort();
            for (kind, path) in paths {
                match kind {
                    PathKind::Scope => s.scope.push(path),
                    PathKind::Claim => s.claims.push(path),
                    PathKind::Touched => s.touched.push(path),
                }
            }
        }
    }
    Ok(sessions)
}

/// The path kinds whose lists differ between `old` and `new`, with the new
/// list: what an upsert rewrites.
fn changed_paths<'a>(old: Option<&Session>, new: &'a Session) -> Vec<(PathKind, &'a [String])> {
    let lists = |s: &Session| [s.scope.clone(), s.claims.clone(), s.touched.clone()];
    let before = old.map(lists).unwrap_or_default();
    [
        (PathKind::Scope, new.scope.as_slice()),
        (PathKind::Claim, new.claims.as_slice()),
        (PathKind::Touched, new.touched.as_slice()),
    ]
    .into_iter()
    .zip(before)
    .filter(|((_, now), was)| *now != was.as_slice())
    .map(|(x, _)| x)
    .collect()
}

/// Newest beat per session key: one batch statement may not touch a row
/// twice.
fn newest(beats: &[Beat]) -> Vec<(String, String, i64)> {
    let mut by: BTreeMap<(&str, &str), i64> = BTreeMap::new();
    for b in beats {
        let at = by
            .entry((b.principal.as_str(), b.agent_session.as_str()))
            .or_insert(b.at);
        *at = (*at).max(b.at);
    }
    by.into_iter()
        .map(|((p, k), at)| (p.to_string(), k.to_string(), at))
        .collect()
}

// --- Postgres -----------------------------------------------------------------

/// Sessions in the meta store's Postgres database.
pub struct PgCoordStore {
    pub(crate) pool: PgPool,
}

impl PgCoordStore {
    fn pool(&self, op: &'static str) -> &PgPool {
        metrics::counter!("leep_meta_statements_total", "op" => op, "pool" => "data").increment(1);
        &self.pool
    }
}

async fn pg_paths(conn: &mut sqlx::PgConnection, sessions: Vec<Session>) -> Result<Vec<Session>> {
    if sessions.is_empty() {
        return Ok(sessions);
    }
    let ids: Vec<i64> = sessions.iter().map(|s| to_i64(s.id)).collect();
    let rows: Vec<(i64, String, String)> = sqlx::query_as(
        "SELECT session_id, kind, path FROM session_paths WHERE session_id = ANY($1)",
    )
    .bind(ids)
    .fetch_all(conn)
    .await
    .map_err(storage)?;
    attach(sessions, rows)
}

#[async_trait]
impl CoordStore for PgCoordStore {
    async fn upsert_session(&self, u: &SessionUpdate) -> Result<(Session, bool)> {
        u.check()?;
        let mut tx = self.pool("session_upsert").begin().await.map_err(storage)?;
        let lock = format!(
            "SELECT {COLUMNS} {FROM} WHERE s.repo_id = 1 AND s.principal = $1 \
             AND s.agent_session = $2 FOR UPDATE OF s"
        );
        let mut tries = 0;
        let (old, id) = loop {
            tries += 1;
            let row = sqlx::query(sqlx::AssertSqlSafe(lock.clone()))
                .bind(&u.principal)
                .bind(&u.agent_session)
                .fetch_optional(&mut *tx)
                .await
                .map_err(storage)?;
            if let Some(row) = row {
                let old = pg_paths(&mut tx, vec![row.session()?]).await?.remove(0);
                let id = old.id;
                break (Some(old), id);
            }
            // Absent: insert a bare row, unless a concurrent upsert did
            // first, then take its lock like any update.
            let id: Option<i64> = sqlx::query_scalar(
                "INSERT INTO sessions (principal, agent_session, origin, created) \
                 VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (repo_id, principal, agent_session) DO NOTHING RETURNING id",
            )
            .bind(&u.principal)
            .bind(&u.agent_session)
            .bind(u.origin.as_str())
            .bind(u.at)
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage)?;
            match id {
                Some(id) => break (None, to_u64(id)?),
                None if tries < 3 => continue,
                None => return Err(storage("session row neither found nor inserted")),
            }
        };
        let starting = old.as_ref().is_none_or(|s| s.state.is_ended());
        if starting {
            let active: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM (SELECT 1 FROM sessions WHERE repo_id = 1 \
                 AND principal = $1 AND state = 'active' AND id <> $2 LIMIT $3) t",
            )
            .bind(&u.principal)
            .bind(to_i64(id))
            .bind(to_i64(u.max_active as u64))
            .fetch_one(&mut *tx)
            .await
            .map_err(storage)?;
            if active as usize >= u.max_active {
                // Dropping the transaction rolls back the bare row.
                return Err(too_many(&u.principal, u.max_active));
            }
        }
        let s = apply_update(old.as_ref(), u, id);
        sqlx::query(
            "UPDATE sessions SET agent_model = $1, title = $2, approach = $3, task_key = $4, \
             target_ref = $5, base = $6, head = $7, origin = $8, state = $9, ended = $10, \
             landed_as = $11, created = $12 WHERE id = $13",
        )
        .bind(&s.agent_model)
        .bind(&s.title)
        .bind(&s.approach)
        .bind(&s.task_key)
        .bind(&s.target_ref)
        .bind(bytes(s.base))
        .bind(bytes(s.head))
        .bind(s.origin.as_str())
        .bind(s.state.as_str())
        .bind(s.ended)
        .bind(bytes(s.landed_as))
        .bind(s.created)
        .bind(to_i64(id))
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        for (kind, paths) in changed_paths(old.as_ref(), &s) {
            sqlx::query("DELETE FROM session_paths WHERE session_id = $1 AND kind = $2")
                .bind(to_i64(id))
                .bind(kind.as_str())
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
            if !paths.is_empty() {
                let mut q = QueryBuilder::<Postgres>::new(
                    "INSERT INTO session_paths (session_id, kind, path) ",
                );
                q.push_values(paths, |mut b, p| {
                    b.push_bind(to_i64(id))
                        .push_bind(kind.as_str())
                        .push_bind(p);
                });
                q.build().execute(&mut *tx).await.map_err(storage)?;
            }
        }
        sqlx::query(
            "INSERT INTO session_beats (session_id, at) VALUES ($1, $2) ON CONFLICT (session_id) \
             DO UPDATE SET at = GREATEST(session_beats.at, excluded.at)",
        )
        .bind(to_i64(id))
        .bind(s.last_seen)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        tx.commit().await.map_err(storage)?;
        Ok((s, starting))
    }

    async fn get_session(&self, id: u64) -> Result<Option<Session>> {
        let mut conn = self.pool("session_get").acquire().await.map_err(storage)?;
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {COLUMNS} {FROM} WHERE s.id = $1 AND s.repo_id = 1"
        )))
        .bind(to_i64(id))
        .fetch_optional(&mut *conn)
        .await
        .map_err(storage)?;
        let Some(row) = row else { return Ok(None) };
        Ok(pg_paths(&mut conn, vec![row.session()?]).await?.pop())
    }

    async fn list_sessions(&self, f: &SessionFilter) -> Result<Vec<Session>> {
        if f.limit == 0 {
            return Ok(Vec::new());
        }
        let mut q =
            QueryBuilder::<Postgres>::new(format!("SELECT {COLUMNS} {FROM} WHERE s.repo_id = 1"));
        if let Some(p) = &f.path {
            q.push(
                " AND s.id IN (SELECT session_id FROM session_paths WHERE repo_id = 1 \
                 AND (path = ",
            )
            .push_bind(p.clone())
            .push(" OR (path > ")
            .push_bind(format!("{p}/"))
            .push(" AND path < ")
            .push_bind(format!("{p}0"))
            .push(") OR path = ANY(")
            .push_bind(
                ancestors(p)
                    .into_iter()
                    .map(String::from)
                    .collect::<Vec<_>>(),
            )
            .push(")))");
        }
        if let Some(k) = &f.task_key {
            q.push(" AND s.task_key = ").push_bind(k.clone());
        }
        if let Some(p) = &f.principal {
            q.push(" AND s.principal = ").push_bind(p.clone());
        }
        if let Some(k) = &f.agent_session {
            q.push(" AND s.agent_session = ").push_bind(k.clone());
        }
        if let Some(o) = f.origin {
            q.push(" AND s.origin = ").push_bind(o.as_str());
        }
        if !f.states.is_empty() {
            let states: Vec<&str> = f.states.iter().map(|s| s.as_str()).collect();
            q.push(" AND s.state = ANY(").push_bind(states).push(")");
        }
        if let Some(b) = f.before {
            q.push(" AND s.id < ").push_bind(to_i64(b));
        }
        q.push(" ORDER BY s.id DESC LIMIT ")
            .push_bind(to_i64(f.limit as u64));
        let mut conn = self.pool("session_list").acquire().await.map_err(storage)?;
        let rows = q.build().fetch_all(&mut *conn).await.map_err(storage)?;
        let sessions = rows
            .iter()
            .map(SessionRow::session)
            .collect::<Result<Vec<_>>>()?;
        if f.skip_paths {
            return Ok(sessions);
        }
        pg_paths(&mut conn, sessions).await
    }

    async fn end_session(&self, id: u64, end: &SessionEnd) -> Result<bool> {
        check_end(end)?;
        let n = sqlx::query(
            "UPDATE sessions SET state = $1, ended = $2, landed_as = $3 \
             WHERE id = $4 AND repo_id = 1 AND state = 'active' \
             AND ($5::text IS NULL OR principal = $5) AND ($6::bytea IS NULL OR head = $6)",
        )
        .bind(end.state.as_str())
        .bind(end.at)
        .bind(bytes(end.landed_as))
        .bind(to_i64(id))
        .bind(end.principal.as_deref())
        .bind(bytes(end.if_head))
        .execute(self.pool("session_end"))
        .await
        .map_err(storage)?
        .rows_affected();
        Ok(n == 1)
    }

    async fn beat_sessions(&self, beats: &[Beat]) -> Result<u64> {
        if beats.is_empty() {
            return Ok(0);
        }
        let (mut ps, mut ks, mut ats) = (Vec::new(), Vec::new(), Vec::new());
        for (p, k, at) in newest(beats) {
            ps.push(p);
            ks.push(k);
            ats.push(at);
        }
        // Rows in id order, so two servers' batches lock them in one order
        // and never deadlock.
        let n = sqlx::query(
            "INSERT INTO session_beats (session_id, at) \
             SELECT s.id, b.at FROM sessions s \
             JOIN UNNEST($1::text[], $2::text[], $3::bigint[]) AS b(principal, agent_session, at) \
             ON s.principal = b.principal AND s.agent_session = b.agent_session \
             WHERE s.repo_id = 1 AND s.state = 'active' ORDER BY s.id \
             ON CONFLICT (session_id) DO UPDATE SET at = GREATEST(session_beats.at, excluded.at)",
        )
        .bind(ps)
        .bind(ks)
        .bind(ats)
        .execute(self.pool("session_beat"))
        .await
        .map_err(storage)?
        .rows_affected();
        Ok(n)
    }

    async fn sweep_sessions(&self, now: i64, limit: usize) -> Result<SweepReport> {
        let limit = to_i64(limit as u64);
        // `session_beats` is UNLOGGED: after a crash it is empty. An active
        // session without a beat counts as heard from now, so a crash
        // never expires every live session at once.
        sqlx::query(
            "INSERT INTO session_beats (session_id, at) \
             SELECT s.id, $1 FROM sessions s WHERE s.repo_id = 1 AND s.state = 'active' \
             AND NOT EXISTS (SELECT 1 FROM session_beats b WHERE b.session_id = s.id) \
             ORDER BY s.id LIMIT $2 ON CONFLICT (session_id) DO NOTHING",
        )
        .bind(now)
        .bind(limit)
        .execute(self.pool("session_sweep"))
        .await
        .map_err(storage)?;
        // Expiry reads the beats index: old beats are only those of live
        // sessions and of sessions that ended within a day.
        let expired = sqlx::query(
            "UPDATE sessions SET state = 'expired', ended = $1 WHERE state = 'active' AND id IN \
             (SELECT b.session_id FROM session_beats b JOIN sessions s ON s.id = b.session_id \
              WHERE b.at < $2 AND s.state = 'active' AND s.repo_id = 1 LIMIT $3)",
        )
        .bind(now)
        .bind(now - EXPIRE_AFTER_SECS)
        .bind(limit)
        .execute(self.pool("session_sweep"))
        .await
        .map_err(storage)?
        .rows_affected();
        let paths_deleted = sqlx::query(
            "DELETE FROM session_paths WHERE (session_id, kind, path) IN \
             (SELECT p.session_id, p.kind, p.path FROM session_paths p \
              JOIN sessions s ON s.id = p.session_id \
              WHERE s.ended IS NOT NULL AND s.ended < $1 LIMIT $2)",
        )
        .bind(now - PATHS_RETENTION_SECS)
        .bind(limit)
        .execute(self.pool("session_sweep"))
        .await
        .map_err(storage)?
        .rows_affected();
        let beats_deleted = sqlx::query(
            "DELETE FROM session_beats WHERE session_id IN \
             (SELECT b.session_id FROM session_beats b JOIN sessions s ON s.id = b.session_id \
              WHERE s.ended IS NOT NULL AND s.ended < $1 LIMIT $2)",
        )
        .bind(now - PATHS_RETENTION_SECS)
        .bind(limit)
        .execute(self.pool("session_sweep"))
        .await
        .map_err(storage)?
        .rows_affected();
        // One statement: the delete re-checks `ended` on each row it
        // locks, so a session revived since the scan is kept.
        let sessions_deleted: i64 = sqlx::query_scalar(
            "WITH gone AS (DELETE FROM sessions WHERE id IN \
               (SELECT id FROM sessions WHERE ended IS NOT NULL AND ended < $1 LIMIT $2) \
               AND ended IS NOT NULL AND ended < $1 RETURNING id), \
             p AS (DELETE FROM session_paths WHERE session_id IN (SELECT id FROM gone)), \
             b AS (DELETE FROM session_beats WHERE session_id IN (SELECT id FROM gone)) \
             SELECT COUNT(*) FROM gone",
        )
        .bind(now - SESSION_RETENTION_SECS)
        .bind(limit)
        .fetch_one(self.pool("session_sweep"))
        .await
        .map_err(storage)?;
        Ok(SweepReport {
            expired,
            paths_deleted,
            beats_deleted,
            sessions_deleted: sessions_deleted as u64,
        })
    }

    async fn count_sessions(&self, state: SessionState, cap: usize) -> Result<u64> {
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM (SELECT 1 FROM sessions WHERE repo_id = 1 AND state = $1 LIMIT $2) t",
        )
        .bind(state.as_str())
        .bind(to_i64(cap as u64))
        .fetch_one(self.pool("session_count"))
        .await
        .map_err(storage)?;
        Ok(n as u64)
    }

    async fn near_paths(&self, q: &NearQuery) -> Result<Vec<NearRow>> {
        if q.limit == 0 || q.paths.is_empty() {
            return Ok(Vec::new());
        }
        let (exact, ranges) = q.probes();
        let (lo, hi): (Vec<String>, Vec<String>) = ranges.into_iter().unzip();
        // One statement: the exact probes, then each range under its own
        // LIMIT, so one crowded directory cannot crowd out the rest.
        let sql = format!(
            "SELECT {COLUMNS}, x.kind AS path_kind, x.path AS path_text FROM ( \
               SELECT p.session_id, p.kind, p.path FROM session_paths p \
                 WHERE p.repo_id = 1 AND p.path = ANY($1) \
               UNION \
               SELECT y.session_id, y.kind, y.path \
                 FROM unnest($2::text[], $3::text[]) AS r(lo, hi) \
                 CROSS JOIN LATERAL ( \
                   SELECT q.session_id, q.kind, q.path FROM session_paths q \
                     JOIN sessions t ON t.id = q.session_id AND t.state = 'active' \
                     WHERE q.repo_id = 1 AND q.path > r.lo COLLATE \"C\" \
                       AND q.path < r.hi COLLATE \"C\" AND q.session_id <> $4 \
                     ORDER BY q.path LIMIT $5) y \
             ) x JOIN sessions s ON s.id = x.session_id \
             LEFT JOIN session_beats b ON b.session_id = s.id \
             WHERE s.repo_id = 1 AND s.state = 'active' AND s.id <> $4 \
             ORDER BY x.path, s.id LIMIT $6"
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(exact)
            .bind(lo)
            .bind(hi)
            .bind(q.exclude.map_or(-1, to_i64))
            .bind(to_i64(q.per_range as u64))
            .bind(to_i64(q.limit as u64))
            .fetch_all(self.pool("session_near"))
            .await
            .map_err(storage)?;
        near_rows(&rows)
    }
}

/// `near_paths` rows, each once.
fn near_rows<R: SessionRow>(rows: &[R]) -> Result<Vec<NearRow>> {
    let mut seen = std::collections::BTreeSet::new();
    let mut out = Vec::new();
    for r in rows {
        let session = r.session()?;
        let (kind, path) = r.path_row()?;
        if seen.insert((session.id, kind.clone(), path.clone())) {
            out.push(NearRow {
                session,
                kind: PathKind::parse(&kind).map_err(storage)?,
                path,
            });
        }
    }
    Ok(out)
}

// --- SQLite -------------------------------------------------------------------

/// Sessions in the meta store's SQLite database.
pub struct SqliteCoordStore {
    pub(crate) pool: SqlitePool,
}

impl SqliteCoordStore {
    fn pool(&self, op: &'static str) -> &SqlitePool {
        metrics::counter!("leep_meta_statements_total", "op" => op, "pool" => "data").increment(1);
        &self.pool
    }
}

async fn sqlite_paths(
    conn: &mut sqlx::SqliteConnection,
    sessions: Vec<Session>,
) -> Result<Vec<Session>> {
    if sessions.is_empty() {
        return Ok(sessions);
    }
    let mut q = QueryBuilder::<Sqlite>::new(
        "SELECT session_id, kind, path FROM session_paths WHERE session_id IN (",
    );
    let mut sep = q.separated(", ");
    for s in &sessions {
        sep.push_bind(to_i64(s.id));
    }
    q.push(")");
    let rows: Vec<(i64, String, String)> =
        q.build_query_as().fetch_all(conn).await.map_err(storage)?;
    attach(sessions, rows)
}

#[async_trait]
impl CoordStore for SqliteCoordStore {
    async fn upsert_session(&self, u: &SessionUpdate) -> Result<(Session, bool)> {
        u.check()?;
        // IMMEDIATE takes the write lock first, so the read below is of the
        // row as this transaction leaves it.
        let mut tx = self
            .pool("session_upsert")
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(storage)?;
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {COLUMNS} {FROM} WHERE s.repo_id = 1 AND s.principal = ? \
             AND s.agent_session = ?"
        )))
        .bind(&u.principal)
        .bind(&u.agent_session)
        .fetch_optional(&mut *tx)
        .await
        .map_err(storage)?;
        let old = match row {
            Some(r) => Some(sqlite_paths(&mut tx, vec![r.session()?]).await?.remove(0)),
            None => None,
        };
        let starting = old.as_ref().is_none_or(|s| s.state.is_ended());
        if starting {
            let active: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM (SELECT 1 FROM sessions WHERE repo_id = 1 \
                 AND principal = ? AND state = 'active' LIMIT ?)",
            )
            .bind(&u.principal)
            .bind(to_i64(u.max_active as u64))
            .fetch_one(&mut *tx)
            .await
            .map_err(storage)?;
            if active as usize >= u.max_active {
                return Err(too_many(&u.principal, u.max_active));
            }
        }
        let id = match &old {
            Some(o) => o.id,
            None => to_u64(
                sqlx::query_scalar(
                    "INSERT INTO sessions (principal, agent_session, origin, created) \
                     VALUES (?, ?, ?, ?) RETURNING id",
                )
                .bind(&u.principal)
                .bind(&u.agent_session)
                .bind(u.origin.as_str())
                .bind(u.at)
                .fetch_one(&mut *tx)
                .await
                .map_err(storage)?,
            )?,
        };
        let s = apply_update(old.as_ref(), u, id);
        sqlx::query(
            "UPDATE sessions SET agent_model = ?, title = ?, approach = ?, task_key = ?, \
             target_ref = ?, base = ?, head = ?, origin = ?, state = ?, ended = ?, \
             landed_as = ?, created = ? WHERE id = ?",
        )
        .bind(&s.agent_model)
        .bind(&s.title)
        .bind(&s.approach)
        .bind(&s.task_key)
        .bind(&s.target_ref)
        .bind(bytes(s.base))
        .bind(bytes(s.head))
        .bind(s.origin.as_str())
        .bind(s.state.as_str())
        .bind(s.ended)
        .bind(bytes(s.landed_as))
        .bind(s.created)
        .bind(to_i64(id))
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        for (kind, paths) in changed_paths(old.as_ref(), &s) {
            sqlx::query("DELETE FROM session_paths WHERE session_id = ? AND kind = ?")
                .bind(to_i64(id))
                .bind(kind.as_str())
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
            // SQLite allows at most 32766 bound values per statement.
            for chunk in paths.chunks(1000) {
                let mut q = QueryBuilder::<Sqlite>::new(
                    "INSERT INTO session_paths (session_id, kind, path) ",
                );
                q.push_values(chunk, |mut b, p| {
                    b.push_bind(to_i64(id))
                        .push_bind(kind.as_str())
                        .push_bind(p);
                });
                q.build().execute(&mut *tx).await.map_err(storage)?;
            }
        }
        sqlx::query(
            "INSERT INTO session_beats (session_id, at) VALUES (?, ?) ON CONFLICT (session_id) \
             DO UPDATE SET at = MAX(at, excluded.at)",
        )
        .bind(to_i64(id))
        .bind(s.last_seen)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        tx.commit().await.map_err(storage)?;
        Ok((s, starting))
    }

    async fn get_session(&self, id: u64) -> Result<Option<Session>> {
        let mut conn = self.pool("session_get").acquire().await.map_err(storage)?;
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {COLUMNS} {FROM} WHERE s.id = ? AND s.repo_id = 1"
        )))
        .bind(to_i64(id))
        .fetch_optional(&mut *conn)
        .await
        .map_err(storage)?;
        let Some(row) = row else { return Ok(None) };
        Ok(sqlite_paths(&mut conn, vec![row.session()?]).await?.pop())
    }

    async fn list_sessions(&self, f: &SessionFilter) -> Result<Vec<Session>> {
        if f.limit == 0 {
            return Ok(Vec::new());
        }
        let mut q =
            QueryBuilder::<Sqlite>::new(format!("SELECT {COLUMNS} {FROM} WHERE s.repo_id = 1"));
        if let Some(p) = &f.path {
            q.push(
                " AND s.id IN (SELECT session_id FROM session_paths WHERE repo_id = 1 \
                 AND (path = ",
            )
            .push_bind(p.clone())
            .push(" OR (path > ")
            .push_bind(format!("{p}/"))
            .push(" AND path < ")
            .push_bind(format!("{p}0"))
            .push(")");
            let up = ancestors(p);
            if !up.is_empty() {
                q.push(" OR path IN (");
                let mut sep = q.separated(", ");
                for a in up {
                    sep.push_bind(a.to_string());
                }
                q.push(")");
            }
            q.push("))");
        }
        if let Some(k) = &f.task_key {
            q.push(" AND s.task_key = ").push_bind(k.clone());
        }
        if let Some(p) = &f.principal {
            q.push(" AND s.principal = ").push_bind(p.clone());
        }
        if let Some(k) = &f.agent_session {
            q.push(" AND s.agent_session = ").push_bind(k.clone());
        }
        if let Some(o) = f.origin {
            q.push(" AND s.origin = ").push_bind(o.as_str());
        }
        if !f.states.is_empty() {
            q.push(" AND s.state IN (");
            let mut sep = q.separated(", ");
            for s in &f.states {
                sep.push_bind(s.as_str());
            }
            q.push(")");
        }
        if let Some(b) = f.before {
            q.push(" AND s.id < ").push_bind(to_i64(b));
        }
        q.push(" ORDER BY s.id DESC LIMIT ")
            .push_bind(to_i64(f.limit as u64));
        let mut conn = self.pool("session_list").acquire().await.map_err(storage)?;
        let rows = q.build().fetch_all(&mut *conn).await.map_err(storage)?;
        let sessions = rows
            .iter()
            .map(SessionRow::session)
            .collect::<Result<Vec<_>>>()?;
        if f.skip_paths {
            return Ok(sessions);
        }
        sqlite_paths(&mut conn, sessions).await
    }

    async fn end_session(&self, id: u64, end: &SessionEnd) -> Result<bool> {
        check_end(end)?;
        let n = sqlx::query(
            "UPDATE sessions SET state = ?, ended = ?, landed_as = ? \
             WHERE id = ? AND repo_id = 1 AND state = 'active' \
             AND (?5 IS NULL OR principal = ?5) AND (?6 IS NULL OR head = ?6)",
        )
        .bind(end.state.as_str())
        .bind(end.at)
        .bind(bytes(end.landed_as))
        .bind(to_i64(id))
        .bind(end.principal.as_deref())
        .bind(bytes(end.if_head))
        .execute(self.pool("session_end"))
        .await
        .map_err(storage)?
        .rows_affected();
        Ok(n == 1)
    }

    async fn beat_sessions(&self, beats: &[Beat]) -> Result<u64> {
        let mut n = 0;
        // SQLite has one writer, which lands need too: hold it for at most
        // this many beats at a time.
        for chunk in newest(beats).chunks(BEATS_PER_TRANSACTION) {
            let mut tx = self
                .pool("session_beat")
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(storage)?;
            for (p, k, at) in chunk {
                n += sqlx::query(
                    "INSERT INTO session_beats (session_id, at) \
                     SELECT id, ? FROM sessions WHERE repo_id = 1 AND principal = ? \
                     AND agent_session = ? AND state = 'active' \
                     ON CONFLICT (session_id) DO UPDATE SET at = MAX(at, excluded.at)",
                )
                .bind(at)
                .bind(p)
                .bind(k)
                .execute(&mut *tx)
                .await
                .map_err(storage)?
                .rows_affected();
            }
            tx.commit().await.map_err(storage)?;
        }
        Ok(n)
    }

    async fn sweep_sessions(&self, now: i64, limit: usize) -> Result<SweepReport> {
        let limit = to_i64(limit as u64);
        let mut r = SweepReport::default();
        // Each write is guarded by a read, so an idle sweep never takes
        // SQLite's single write lock away from lands.
        let any = |sql: &'static str, at: i64| async move {
            let found: Option<i64> = sqlx::query_scalar(sql)
                .bind(at)
                .fetch_optional(self.pool("session_sweep"))
                .await
                .map_err(storage)?;
            Ok::<bool, leep_core::Error>(found.is_some())
        };
        if any(
            "SELECT s.id FROM sessions s WHERE s.repo_id = 1 AND s.state = 'active' \
             AND NOT EXISTS (SELECT 1 FROM session_beats b WHERE b.session_id = s.id) \
             AND ? IS NOT NULL LIMIT 1",
            now,
        )
        .await?
        {
            // A beat for every active session that lacks one, so a lost
            // beat never expires a live session.
            sqlx::query(
                "INSERT INTO session_beats (session_id, at) \
                 SELECT s.id, ? FROM sessions s WHERE s.repo_id = 1 AND s.state = 'active' \
                 AND NOT EXISTS (SELECT 1 FROM session_beats b WHERE b.session_id = s.id) \
                 LIMIT ? ON CONFLICT (session_id) DO NOTHING",
            )
            .bind(now)
            .bind(limit)
            .execute(self.pool("session_sweep"))
            .await
            .map_err(storage)?;
        }
        if any(
            "SELECT b.session_id FROM session_beats b JOIN sessions s ON s.id = b.session_id \
             WHERE b.at < ? AND s.state = 'active' LIMIT 1",
            now - EXPIRE_AFTER_SECS,
        )
        .await?
        {
            r.expired = sqlx::query(
                "UPDATE sessions SET state = 'expired', ended = ? WHERE state = 'active' AND id IN \
                 (SELECT b.session_id FROM session_beats b JOIN sessions s \
                  ON s.id = b.session_id WHERE b.at < ? AND s.state = 'active' \
                  AND s.repo_id = 1 LIMIT ?)",
            )
            .bind(now)
            .bind(now - EXPIRE_AFTER_SECS)
            .bind(limit)
            .execute(self.pool("session_sweep"))
            .await
            .map_err(storage)?
            .rows_affected();
        }
        if any(
            "SELECT p.session_id FROM session_paths p JOIN sessions s ON s.id = p.session_id \
             WHERE s.ended IS NOT NULL AND s.ended < ? LIMIT 1",
            now - PATHS_RETENTION_SECS,
        )
        .await?
        {
            r.paths_deleted = sqlx::query(
                "DELETE FROM session_paths WHERE rowid IN \
                 (SELECT p.rowid FROM session_paths p JOIN sessions s ON s.id = p.session_id \
                  WHERE s.ended IS NOT NULL AND s.ended < ? LIMIT ?)",
            )
            .bind(now - PATHS_RETENTION_SECS)
            .bind(limit)
            .execute(self.pool("session_sweep"))
            .await
            .map_err(storage)?
            .rows_affected();
        }
        if any(
            "SELECT b.session_id FROM session_beats b JOIN sessions s ON s.id = b.session_id \
             WHERE s.ended IS NOT NULL AND s.ended < ? LIMIT 1",
            now - PATHS_RETENTION_SECS,
        )
        .await?
        {
            r.beats_deleted = sqlx::query(
                "DELETE FROM session_beats WHERE session_id IN \
                 (SELECT b.session_id FROM session_beats b JOIN sessions s \
                  ON s.id = b.session_id WHERE s.ended IS NOT NULL AND s.ended < ? LIMIT ?)",
            )
            .bind(now - PATHS_RETENTION_SECS)
            .bind(limit)
            .execute(self.pool("session_sweep"))
            .await
            .map_err(storage)?
            .rows_affected();
        }
        // Read first, outside a write transaction.
        let ids: Vec<i64> = sqlx::query_scalar(
            "SELECT id FROM sessions WHERE ended IS NOT NULL AND ended < ? LIMIT ?",
        )
        .bind(now - SESSION_RETENTION_SECS)
        .bind(limit)
        .fetch_all(self.pool("session_sweep"))
        .await
        .map_err(storage)?;
        if ids.is_empty() {
            return Ok(r);
        }
        let mut tx = self
            .pool("session_sweep")
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(storage)?;
        for id in ids {
            // Still ended that long ago: an upsert since may have revived it.
            let gone = sqlx::query(
                "DELETE FROM sessions WHERE id = ? AND ended IS NOT NULL AND ended < ?",
            )
            .bind(id)
            .bind(now - SESSION_RETENTION_SECS)
            .execute(&mut *tx)
            .await
            .map_err(storage)?
            .rows_affected();
            if gone == 0 {
                continue;
            }
            r.sessions_deleted += gone;
            for table in ["session_paths", "session_beats"] {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "DELETE FROM {table} WHERE session_id = ?"
                )))
                .bind(id)
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
            }
        }
        tx.commit().await.map_err(storage)?;
        Ok(r)
    }

    async fn count_sessions(&self, state: SessionState, cap: usize) -> Result<u64> {
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM (SELECT 1 FROM sessions WHERE repo_id = 1 AND state = ? LIMIT ?)",
        )
        .bind(state.as_str())
        .bind(to_i64(cap as u64))
        .fetch_one(self.pool("session_count"))
        .await
        .map_err(storage)?;
        Ok(n as u64)
    }
    async fn near_paths(&self, q: &NearQuery) -> Result<Vec<NearRow>> {
        if q.limit == 0 || q.paths.is_empty() {
            return Ok(Vec::new());
        }
        let (exact, ranges) = q.probes();
        let head = format!(
            "SELECT {COLUMNS}, p.kind AS path_kind, p.path AS path_text FROM session_paths p \
             JOIN sessions s ON s.id = p.session_id \
             LEFT JOIN session_beats b ON b.session_id = s.id \
             WHERE p.repo_id = 1 AND s.state = 'active' AND s.id <> "
        );
        let exclude = q.exclude.map_or(-1, to_i64);
        let mut conn = self.pool("session_near").acquire().await.map_err(storage)?;
        let mut rows = Vec::new();
        // SQLite is local: one short statement per probe costs little.
        for chunk in exact.chunks(500) {
            let mut b = QueryBuilder::<Sqlite>::new(head.clone());
            b.push_bind(exclude).push(" AND p.path IN (");
            let mut sep = b.separated(", ");
            for p in chunk {
                sep.push_bind(p.clone());
            }
            b.push(") LIMIT ").push_bind(to_i64(q.limit as u64));
            rows.extend(b.build().fetch_all(&mut *conn).await.map_err(storage)?);
        }
        for (lo, hi) in ranges {
            let mut b = QueryBuilder::<Sqlite>::new(head.clone());
            b.push_bind(exclude)
                .push(" AND p.path > ")
                .push_bind(lo)
                .push(" AND p.path < ")
                .push_bind(hi)
                .push(" ORDER BY p.path LIMIT ")
                .push_bind(to_i64(q.per_range as u64));
            rows.extend(b.build().fetch_all(&mut *conn).await.map_err(storage)?);
        }
        let mut out = near_rows(&rows)?;
        out.sort_by(|a, b| (&a.path, a.session.id).cmp(&(&b.path, b.session.id)));
        out.truncate(q.limit);
        Ok(out)
    }
}
