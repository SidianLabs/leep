//! Leases on Postgres and SQLite (ADR 0056): one table for every kind of
//! work a server can hold (`scope`), keyed by scope, repository, name and
//! shard. Each call is one conditional statement (plus a read of the row it
//! settled), so two servers claiming a lease at once cannot both get it,
//! and a live lease of another holder is never taken. Postgres judges time
//! by its own clock; SQLite serves the processes of one host, so the process
//! clock is the store's.

use std::time::Duration;

use leep_core::store::lease_remaining_ms;
use leep_core::{Lease, LeaseClaim, LeaseKey, LeaseTiming, Result};
use sqlx::{AssertSqlSafe, PgPool, Row, SqlitePool};

use crate::storage;

fn ms(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

fn epoch(e: u64) -> i64 {
    i64::try_from(e).unwrap_or(i64::MAX)
}

fn lease(key: &LeaseKey, row: &impl LeaseRow, now: i64, timing: LeaseTiming) -> Lease {
    Lease {
        key: key.clone(),
        holder: row.holder(),
        address: row.address(),
        protocol: u32::try_from(row.protocol()).unwrap_or(0),
        meta: row.meta(),
        epoch: u64::try_from(row.epoch()).unwrap_or(0),
        remaining: Duration::from_millis(lease_remaining_ms(
            now,
            row.renewed_at(),
            row.expires_at(),
            timing,
        ) as u64),
    }
}

trait LeaseRow {
    fn holder(&self) -> String;
    fn address(&self) -> String;
    fn protocol(&self) -> i64;
    fn meta(&self) -> String;
    fn epoch(&self) -> i64;
    fn renewed_at(&self) -> i64;
    fn expires_at(&self) -> i64;
}

macro_rules! lease_row {
    ($row:ty, $int:ty) => {
        impl LeaseRow for $row {
            fn holder(&self) -> String {
                self.get("holder")
            }
            fn address(&self) -> String {
                self.get("address")
            }
            fn protocol(&self) -> i64 {
                i64::from(self.get::<$int, _>("protocol"))
            }
            fn meta(&self) -> String {
                self.get("meta")
            }
            fn epoch(&self) -> i64 {
                self.get("epoch")
            }
            fn renewed_at(&self) -> i64 {
                self.get("renewed_at")
            }
            fn expires_at(&self) -> i64 {
                self.get("expires_at")
            }
        }
    };
}
lease_row!(sqlx::postgres::PgRow, i32);
lease_row!(sqlx::sqlite::SqliteRow, i64);

const PG_NOW: &str = "(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT";
const COLUMNS: &str = "holder, address, protocol, meta, epoch, renewed_at, expires_at";

pub(crate) async fn pg_get(
    pool: &PgPool,
    key: &LeaseKey,
    timing: LeaseTiming,
) -> Result<Option<Lease>> {
    let row = sqlx::query(AssertSqlSafe(format!(
        "SELECT {COLUMNS}, {PG_NOW} AS now FROM leases \
         WHERE scope = $1 AND repo_id = $2 AND name = $3 AND shard = $4"
    )))
    .bind(&key.scope)
    .bind(key.repo_id)
    .bind(&key.name)
    .bind(key.shard)
    .fetch_optional(pool)
    .await
    .map_err(storage)?;
    Ok(row.map(|r| lease(key, &r, r.get("now"), timing)))
}

pub(crate) async fn pg_acquire(
    pool: &PgPool,
    claim: &LeaseClaim<'_>,
    timing: LeaseTiming,
) -> Result<Lease> {
    let key = claim.key;
    sqlx::query(AssertSqlSafe(format!(
        "WITH now AS (SELECT {PG_NOW} AS ms) \
         INSERT INTO leases AS l (scope, repo_id, name, shard, {COLUMNS}) \
         SELECT $1, $2, $3, $4, $5, $6, $7, $8, 1, now.ms, now.ms + $9 FROM now \
         ON CONFLICT (scope, repo_id, name, shard) DO UPDATE SET \
           epoch = CASE WHEN l.holder = EXCLUDED.holder THEN l.epoch ELSE l.epoch + 1 END, \
           holder = EXCLUDED.holder, address = EXCLUDED.address, \
           protocol = EXCLUDED.protocol, meta = EXCLUDED.meta, \
           renewed_at = EXCLUDED.renewed_at, expires_at = EXCLUDED.expires_at \
         WHERE l.holder = EXCLUDED.holder OR l.expires_at <= EXCLUDED.renewed_at \
           OR l.renewed_at + $10 <= EXCLUDED.renewed_at"
    )))
    .bind(&key.scope)
    .bind(key.repo_id)
    .bind(&key.name)
    .bind(key.shard)
    .bind(claim.holder)
    .bind(claim.address)
    .bind(i32::try_from(claim.protocol).unwrap_or(i32::MAX))
    .bind(claim.meta)
    .bind(ms(timing.ttl))
    .bind(ms(timing.stale_after))
    .execute(pool)
    .await
    .map_err(storage)?;
    pg_get(pool, key, timing)
        .await?
        .ok_or_else(|| storage("lease vanished after it was claimed"))
}

pub(crate) async fn pg_renew(
    pool: &PgPool,
    holder: &str,
    held: &[(LeaseKey, u64)],
    timing: LeaseTiming,
) -> Result<Vec<Option<Duration>>> {
    if held.is_empty() {
        return Ok(Vec::new());
    }
    let scopes: Vec<&str> = held.iter().map(|(k, _)| k.scope.as_str()).collect();
    let repos: Vec<i64> = held.iter().map(|(k, _)| k.repo_id).collect();
    let names: Vec<&str> = held.iter().map(|(k, _)| k.name.as_str()).collect();
    let shards: Vec<i32> = held.iter().map(|(k, _)| k.shard).collect();
    let epochs: Vec<i64> = held.iter().map(|(_, e)| epoch(*e)).collect();
    let rows = sqlx::query(AssertSqlSafe(format!(
        "WITH now AS (SELECT {PG_NOW} AS ms), \
              want AS (SELECT * FROM UNNEST($2::TEXT[], $3::BIGINT[], $4::TEXT[], \
                       $5::INTEGER[], $6::BIGINT[]) AS w(scope, repo_id, name, shard, epoch)) \
         UPDATE leases AS l SET renewed_at = now.ms, expires_at = now.ms + $7 \
         FROM now, want \
         WHERE l.holder = $1 AND l.scope = want.scope AND l.repo_id = want.repo_id \
           AND l.name = want.name AND l.shard = want.shard AND l.epoch = want.epoch \
         RETURNING l.scope, l.repo_id, l.name, l.shard, l.holder, l.address, l.protocol, \
           l.meta, l.epoch, l.renewed_at, l.expires_at, now.ms AS now"
    )))
    .bind(holder)
    .bind(&scopes)
    .bind(&repos)
    .bind(&names)
    .bind(&shards)
    .bind(&epochs)
    .bind(ms(timing.ttl))
    .fetch_all(pool)
    .await
    .map_err(storage)?;
    let mut out = vec![None; held.len()];
    for r in &rows {
        let key = LeaseKey {
            scope: r.get("scope"),
            repo_id: r.get("repo_id"),
            name: r.get("name"),
            shard: r.get("shard"),
        };
        if let Some(i) = held.iter().position(|(k, _)| *k == key) {
            out[i] = Some(lease(&key, r, r.get("now"), timing).remaining);
        }
    }
    Ok(out)
}

pub(crate) async fn pg_release(
    pool: &PgPool,
    key: &LeaseKey,
    holder: &str,
    e: u64,
) -> Result<bool> {
    let done = sqlx::query(
        "UPDATE leases SET renewed_at = 0, expires_at = 0 \
         WHERE scope = $1 AND repo_id = $2 AND name = $3 AND shard = $4 \
           AND holder = $5 AND epoch = $6",
    )
    .bind(&key.scope)
    .bind(key.repo_id)
    .bind(&key.name)
    .bind(key.shard)
    .bind(holder)
    .bind(epoch(e))
    .execute(pool)
    .await
    .map_err(storage)?;
    Ok(done.rows_affected() == 1)
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

pub(crate) async fn sqlite_get(
    pool: &SqlitePool,
    key: &LeaseKey,
    timing: LeaseTiming,
) -> Result<Option<Lease>> {
    let row = sqlx::query(AssertSqlSafe(format!(
        "SELECT {COLUMNS} FROM leases \
         WHERE scope = ? AND repo_id = ? AND name = ? AND shard = ?"
    )))
    .bind(&key.scope)
    .bind(key.repo_id)
    .bind(&key.name)
    .bind(key.shard)
    .fetch_optional(pool)
    .await
    .map_err(storage)?;
    Ok(row.map(|r| lease(key, &r, now_ms(), timing)))
}

pub(crate) async fn sqlite_acquire(
    pool: &SqlitePool,
    claim: &LeaseClaim<'_>,
    timing: LeaseTiming,
) -> Result<Lease> {
    let key = claim.key;
    let now = now_ms();
    sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO leases (scope, repo_id, name, shard, {COLUMNS}) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 1, ?9, ?9 + ?10) \
         ON CONFLICT (scope, repo_id, name, shard) DO UPDATE SET \
           epoch = CASE WHEN leases.holder = excluded.holder \
                   THEN leases.epoch ELSE leases.epoch + 1 END, \
           holder = excluded.holder, address = excluded.address, \
           protocol = excluded.protocol, meta = excluded.meta, \
           renewed_at = excluded.renewed_at, expires_at = excluded.expires_at \
         WHERE leases.holder = excluded.holder \
           OR leases.expires_at <= excluded.renewed_at \
           OR leases.renewed_at + ?11 <= excluded.renewed_at"
    )))
    .bind(&key.scope)
    .bind(key.repo_id)
    .bind(&key.name)
    .bind(key.shard)
    .bind(claim.holder)
    .bind(claim.address)
    .bind(i64::from(claim.protocol))
    .bind(claim.meta)
    .bind(now)
    .bind(ms(timing.ttl))
    .bind(ms(timing.stale_after))
    .execute(pool)
    .await
    .map_err(storage)?;
    sqlite_get(pool, key, timing)
        .await?
        .ok_or_else(|| storage("lease vanished after it was claimed"))
}

pub(crate) async fn sqlite_renew(
    pool: &SqlitePool,
    holder: &str,
    held: &[(LeaseKey, u64)],
    timing: LeaseTiming,
) -> Result<Vec<Option<Duration>>> {
    let now = now_ms();
    let mut tx = pool.begin().await.map_err(storage)?;
    let mut out = Vec::with_capacity(held.len());
    for (key, e) in held {
        let done = sqlx::query(
            "UPDATE leases SET renewed_at = ?1, expires_at = ?1 + ?2 \
             WHERE scope = ?3 AND repo_id = ?4 AND name = ?5 AND shard = ?6 \
               AND holder = ?7 AND epoch = ?8",
        )
        .bind(now)
        .bind(ms(timing.ttl))
        .bind(&key.scope)
        .bind(key.repo_id)
        .bind(&key.name)
        .bind(key.shard)
        .bind(holder)
        .bind(epoch(*e))
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        out.push((done.rows_affected() == 1).then(|| {
            Duration::from_millis(lease_remaining_ms(
                now,
                now,
                now.saturating_add(ms(timing.ttl)),
                timing,
            ) as u64)
        }));
    }
    tx.commit().await.map_err(storage)?;
    Ok(out)
}

pub(crate) async fn sqlite_release(
    pool: &SqlitePool,
    key: &LeaseKey,
    holder: &str,
    e: u64,
) -> Result<bool> {
    let done = sqlx::query(
        "UPDATE leases SET renewed_at = 0, expires_at = 0 \
         WHERE scope = ? AND repo_id = ? AND name = ? AND shard = ? \
           AND holder = ? AND epoch = ?",
    )
    .bind(&key.scope)
    .bind(key.repo_id)
    .bind(&key.name)
    .bind(key.shard)
    .bind(holder)
    .bind(epoch(e))
    .execute(pool)
    .await
    .map_err(storage)?;
    Ok(done.rows_affected() == 1)
}
