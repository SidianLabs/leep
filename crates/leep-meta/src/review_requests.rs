//! Review requests on Postgres and SQLite (ADR 0086): a `review_requests`
//! row per request and a `review_request_reviewers` row per reviewer asked,
//! so listing by reviewer is an index lookup. A request and its reviewers
//! are inserted in one transaction; a close is one conditional update from
//! `open`, so two closers never both win.

use std::collections::BTreeMap;

use leep_core::review_request::{
    NewReviewRequest, ReviewRequest, ReviewRequestFilter, ReviewRequestState,
};
use leep_core::store::check_close_state;
use leep_core::{ChangesetId, Result};
use sqlx::{PgPool, Postgres, QueryBuilder, Row, Sqlite, SqlitePool};

use crate::{storage, to_id};

/// The schema version that added the tables.
pub(crate) const REVIEW_REQUESTS_SCHEMA_VERSION: i64 = 8;

const COLUMNS: &str =
    "id, changeset, target_ref, requester, message, created, state, closed, landed_as";

fn to_u64(v: i64) -> Result<u64> {
    u64::try_from(v).map_err(|_| storage("negative review request id in db"))
}

fn to_i64(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

trait RequestRow {
    fn request(&self) -> Result<ReviewRequest>;
}

macro_rules! request_row {
    ($row:ty) => {
        impl RequestRow for $row {
            fn request(&self) -> Result<ReviewRequest> {
                let landed: Option<Vec<u8>> = self.try_get("landed_as").map_err(storage)?;
                Ok(ReviewRequest {
                    id: to_u64(self.try_get("id").map_err(storage)?)?,
                    changeset: to_id(&self.try_get::<Vec<u8>, _>("changeset").map_err(storage)?)?,
                    target_ref: self.try_get("target_ref").map_err(storage)?,
                    requester: self.try_get("requester").map_err(storage)?,
                    reviewers: Vec::new(),
                    message: self.try_get("message").map_err(storage)?,
                    created: self.try_get("created").map_err(storage)?,
                    state: ReviewRequestState::parse(
                        &self.try_get::<String, _>("state").map_err(storage)?,
                    )
                    .map_err(storage)?,
                    closed: self.try_get("closed").map_err(storage)?,
                    landed_as: landed.as_deref().map(to_id).transpose()?,
                })
            }
        }
    };
}
request_row!(sqlx::postgres::PgRow);
request_row!(sqlx::sqlite::SqliteRow);

/// Put each reviewer row on its request, in reviewer order.
fn attach(mut reqs: Vec<ReviewRequest>, reviewers: Vec<(i64, String)>) -> Vec<ReviewRequest> {
    let mut by: BTreeMap<u64, Vec<String>> = BTreeMap::new();
    for (id, name) in reviewers {
        by.entry(id as u64).or_default().push(name);
    }
    for r in &mut reqs {
        if let Some(mut names) = by.remove(&r.id) {
            names.sort();
            r.reviewers = names;
        }
    }
    reqs
}

fn states(f: &ReviewRequestFilter) -> Vec<&'static str> {
    f.states.iter().map(|s| s.as_str()).collect()
}

// --- Postgres -----------------------------------------------------------------

pub(crate) async fn pg_put(pool: &PgPool, req: &NewReviewRequest) -> Result<ReviewRequest> {
    let mut tx = pool.begin().await.map_err(storage)?;
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO review_requests (changeset, target_ref, requester, message, created) \
         VALUES ($1, $2, $3, $4, $5) RETURNING id",
    )
    .bind(req.changeset.0.as_bytes().as_slice())
    .bind(&req.target_ref)
    .bind(&req.requester)
    .bind(&req.message)
    .bind(req.created)
    .fetch_one(&mut *tx)
    .await
    .map_err(storage)?;
    if !req.reviewers.is_empty() {
        let mut q = QueryBuilder::<Postgres>::new(
            "INSERT INTO review_request_reviewers (request_id, reviewer) ",
        );
        q.push_values(&req.reviewers, |mut b, name| {
            b.push_bind(id).push_bind(name);
        });
        q.push(" ON CONFLICT DO NOTHING");
        q.build().execute(&mut *tx).await.map_err(storage)?;
    }
    tx.commit().await.map_err(storage)?;
    Ok(created(to_u64(id)?, req))
}

fn created(id: u64, req: &NewReviewRequest) -> ReviewRequest {
    let mut reviewers = req.reviewers.clone();
    reviewers.sort();
    reviewers.dedup();
    ReviewRequest {
        id,
        changeset: req.changeset,
        target_ref: req.target_ref.clone(),
        requester: req.requester.clone(),
        reviewers,
        message: req.message.clone(),
        created: req.created,
        state: ReviewRequestState::Open,
        closed: None,
        landed_as: None,
    }
}

pub(crate) async fn pg_list(pool: &PgPool, f: &ReviewRequestFilter) -> Result<Vec<ReviewRequest>> {
    if f.limit == 0 {
        return Ok(Vec::new());
    }
    let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {COLUMNS} FROM review_requests r WHERE repo_id = 1 \
         AND ($1::text IS NULL OR EXISTS (SELECT 1 FROM review_request_reviewers v \
              WHERE v.request_id = r.id AND v.reviewer = $1) \
              OR (r.requester <> $1 AND NOT EXISTS (SELECT 1 FROM review_request_reviewers v \
              WHERE v.request_id = r.id))) \
         AND ($2::text IS NULL OR r.requester = $2) \
         AND ($3::bytea IS NULL OR r.changeset = $3) \
         AND ($4::text IS NULL OR r.target_ref = $4) \
         AND (cardinality($5::text[]) = 0 OR r.state = ANY($5)) \
         AND ($6::bigint IS NULL OR r.id < $6) \
         ORDER BY r.id DESC LIMIT $7"
    )))
    .bind(f.reviewer.as_deref())
    .bind(f.requester.as_deref())
    .bind(f.changeset.map(|c| c.0.as_bytes().to_vec()))
    .bind(f.target_ref.as_deref())
    .bind(states(f))
    .bind(f.before.map(to_i64))
    .bind(to_i64(f.limit as u64))
    .fetch_all(pool)
    .await
    .map_err(storage)?;
    let reqs = rows
        .iter()
        .map(RequestRow::request)
        .collect::<Result<Vec<_>>>()?;
    pg_with_reviewers(pool, reqs).await
}

async fn pg_with_reviewers(pool: &PgPool, reqs: Vec<ReviewRequest>) -> Result<Vec<ReviewRequest>> {
    if reqs.is_empty() {
        return Ok(reqs);
    }
    let ids: Vec<i64> = reqs.iter().map(|r| to_i64(r.id)).collect();
    let names: Vec<(i64, String)> = sqlx::query_as(
        "SELECT request_id, reviewer FROM review_request_reviewers WHERE request_id = ANY($1)",
    )
    .bind(ids)
    .fetch_all(pool)
    .await
    .map_err(storage)?;
    Ok(attach(reqs, names))
}

pub(crate) async fn pg_get(pool: &PgPool, id: u64) -> Result<Option<ReviewRequest>> {
    let row = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {COLUMNS} FROM review_requests WHERE id = $1 AND repo_id = 1"
    )))
    .bind(to_i64(id))
    .fetch_optional(pool)
    .await
    .map_err(storage)?;
    let Some(row) = row else { return Ok(None) };
    Ok(pg_with_reviewers(pool, vec![row.request()?]).await?.pop())
}

pub(crate) async fn pg_close(
    pool: &PgPool,
    id: u64,
    state: ReviewRequestState,
    landed_as: Option<ChangesetId>,
    closed: i64,
) -> Result<bool> {
    check_close_state(state)?;
    let n = sqlx::query(
        "UPDATE review_requests SET state = $1, closed = $2, landed_as = $3 \
         WHERE id = $4 AND repo_id = 1 AND state = 'open'",
    )
    .bind(state.as_str())
    .bind(closed)
    .bind(landed_as.map(|c| c.0.as_bytes().to_vec()))
    .bind(to_i64(id))
    .execute(pool)
    .await
    .map_err(storage)?
    .rows_affected();
    Ok(n == 1)
}

// --- SQLite -------------------------------------------------------------------

pub(crate) async fn sqlite_put(pool: &SqlitePool, req: &NewReviewRequest) -> Result<ReviewRequest> {
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.map_err(storage)?;
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO review_requests (changeset, target_ref, requester, message, created) \
         VALUES (?, ?, ?, ?, ?) RETURNING id",
    )
    .bind(req.changeset.0.as_bytes().as_slice())
    .bind(&req.target_ref)
    .bind(&req.requester)
    .bind(&req.message)
    .bind(req.created)
    .fetch_one(&mut *tx)
    .await
    .map_err(storage)?;
    if !req.reviewers.is_empty() {
        let mut q = QueryBuilder::<Sqlite>::new(
            "INSERT OR IGNORE INTO review_request_reviewers (request_id, reviewer) ",
        );
        q.push_values(&req.reviewers, |mut b, name| {
            b.push_bind(id).push_bind(name);
        });
        q.build().execute(&mut *tx).await.map_err(storage)?;
    }
    tx.commit().await.map_err(storage)?;
    Ok(created(to_u64(id)?, req))
}

pub(crate) async fn sqlite_list(
    pool: &SqlitePool,
    f: &ReviewRequestFilter,
) -> Result<Vec<ReviewRequest>> {
    if f.limit == 0 {
        return Ok(Vec::new());
    }
    let mut q = QueryBuilder::<Sqlite>::new(format!(
        "SELECT {COLUMNS} FROM review_requests r WHERE repo_id = 1"
    ));
    if let Some(x) = &f.reviewer {
        q.push(
            " AND (EXISTS (SELECT 1 FROM review_request_reviewers v \
             WHERE v.request_id = r.id AND v.reviewer = ",
        )
        .push_bind(x)
        .push(") OR (r.requester <> ")
        .push_bind(x)
        .push(
            " AND NOT EXISTS (SELECT 1 FROM review_request_reviewers v \
             WHERE v.request_id = r.id)))",
        );
    }
    if let Some(x) = &f.requester {
        q.push(" AND r.requester = ").push_bind(x);
    }
    if let Some(c) = f.changeset {
        q.push(" AND r.changeset = ")
            .push_bind(c.0.as_bytes().to_vec());
    }
    if let Some(t) = &f.target_ref {
        q.push(" AND r.target_ref = ").push_bind(t);
    }
    if !f.states.is_empty() {
        q.push(" AND r.state IN (");
        let mut sep = q.separated(", ");
        for s in states(f) {
            sep.push_bind(s);
        }
        q.push(")");
    }
    if let Some(b) = f.before {
        q.push(" AND r.id < ").push_bind(to_i64(b));
    }
    q.push(" ORDER BY r.id DESC LIMIT ")
        .push_bind(to_i64(f.limit as u64));
    let rows = q.build().fetch_all(pool).await.map_err(storage)?;
    let reqs = rows
        .iter()
        .map(RequestRow::request)
        .collect::<Result<Vec<_>>>()?;
    sqlite_with_reviewers(pool, reqs).await
}

async fn sqlite_with_reviewers(
    pool: &SqlitePool,
    reqs: Vec<ReviewRequest>,
) -> Result<Vec<ReviewRequest>> {
    if reqs.is_empty() {
        return Ok(reqs);
    }
    let mut q = QueryBuilder::<Sqlite>::new(
        "SELECT request_id, reviewer FROM review_request_reviewers WHERE request_id IN (",
    );
    let mut sep = q.separated(", ");
    for r in &reqs {
        sep.push_bind(to_i64(r.id));
    }
    q.push(")");
    let names: Vec<(i64, String)> = q.build_query_as().fetch_all(pool).await.map_err(storage)?;
    Ok(attach(reqs, names))
}

pub(crate) async fn sqlite_get(pool: &SqlitePool, id: u64) -> Result<Option<ReviewRequest>> {
    let row = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {COLUMNS} FROM review_requests WHERE id = ? AND repo_id = 1"
    )))
    .bind(to_i64(id))
    .fetch_optional(pool)
    .await
    .map_err(storage)?;
    let Some(row) = row else { return Ok(None) };
    Ok(sqlite_with_reviewers(pool, vec![row.request()?])
        .await?
        .pop())
}

pub(crate) async fn sqlite_close(
    pool: &SqlitePool,
    id: u64,
    state: ReviewRequestState,
    landed_as: Option<ChangesetId>,
    closed: i64,
) -> Result<bool> {
    check_close_state(state)?;
    let n = sqlx::query(
        "UPDATE review_requests SET state = ?, closed = ?, landed_as = ? \
         WHERE id = ? AND repo_id = 1 AND state = 'open'",
    )
    .bind(state.as_str())
    .bind(closed)
    .bind(landed_as.map(|c| c.0.as_bytes().to_vec()))
    .bind(to_i64(id))
    .execute(pool)
    .await
    .map_err(storage)?
    .rows_affected();
    Ok(n == 1)
}
