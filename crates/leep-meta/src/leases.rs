//! Land leases on Postgres and SQLite (ADR 0056). Each call is one
//! conditional statement (plus a read of the row it settled), so two
//! servers claiming a ref at once cannot both get it, and a live lease of
//! another holder is never taken. Postgres judges time by its own clock;
//! SQLite serves the processes of one host, so the process clock is the
//! store's.

use std::time::Duration;

use leep_core::store::lease_remaining_ms;
use leep_core::{LandLease, LeaseClaim, LeaseTiming, Result};
use sqlx::{PgPool, Row, SqlitePool};

use crate::storage;

fn ms(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

fn lease(ref_name: &str, row: &impl LeaseRow, now: i64, timing: LeaseTiming) -> LandLease {
    LandLease {
        ref_name: ref_name.to_string(),
        holder: row.holder(),
        address: row.address(),
        protocol: u32::try_from(row.protocol()).unwrap_or(0),
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

pub(crate) async fn pg_get(
    pool: &PgPool,
    ref_name: &str,
    timing: LeaseTiming,
) -> Result<Option<LandLease>> {
    let row = sqlx::query(&format!(
        "SELECT holder, address, protocol, epoch, renewed_at, expires_at, {PG_NOW} AS now \
         FROM land_leases WHERE repo_id = 1 AND ref_name = $1"
    ))
    .bind(ref_name)
    .fetch_optional(pool)
    .await
    .map_err(storage)?;
    Ok(row.map(|r| lease(ref_name, &r, r.get("now"), timing)))
}

pub(crate) async fn pg_acquire(
    pool: &PgPool,
    claim: &LeaseClaim<'_>,
    timing: LeaseTiming,
) -> Result<LandLease> {
    sqlx::query(&format!(
        "WITH now AS (SELECT {PG_NOW} AS ms) \
         INSERT INTO land_leases AS l \
           (ref_name, holder, address, protocol, epoch, renewed_at, expires_at) \
         SELECT $1, $2, $3, $4, 1, now.ms, now.ms + $5 FROM now \
         ON CONFLICT (repo_id, ref_name) DO UPDATE SET \
           epoch = CASE WHEN l.holder = EXCLUDED.holder THEN l.epoch ELSE l.epoch + 1 END, \
           holder = EXCLUDED.holder, address = EXCLUDED.address, \
           protocol = EXCLUDED.protocol, renewed_at = EXCLUDED.renewed_at, \
           expires_at = EXCLUDED.expires_at \
         WHERE l.holder = EXCLUDED.holder OR l.expires_at <= EXCLUDED.renewed_at \
           OR l.renewed_at + $6 <= EXCLUDED.renewed_at"
    ))
    .bind(claim.ref_name)
    .bind(claim.holder)
    .bind(claim.address)
    .bind(i32::try_from(claim.protocol).unwrap_or(i32::MAX))
    .bind(ms(timing.ttl))
    .bind(ms(timing.stale_after))
    .execute(pool)
    .await
    .map_err(storage)?;
    pg_get(pool, claim.ref_name, timing)
        .await?
        .ok_or_else(|| storage("land lease vanished after it was claimed"))
}

pub(crate) async fn pg_renew(
    pool: &PgPool,
    holder: &str,
    held: &[(String, u64)],
    timing: LeaseTiming,
) -> Result<Vec<Option<Duration>>> {
    if held.is_empty() {
        return Ok(Vec::new());
    }
    let names: Vec<&str> = held.iter().map(|(n, _)| n.as_str()).collect();
    let epochs: Vec<i64> = held
        .iter()
        .map(|(_, e)| i64::try_from(*e).unwrap_or(i64::MAX))
        .collect();
    let rows = sqlx::query(&format!(
        "WITH now AS (SELECT {PG_NOW} AS ms), \
              want AS (SELECT * FROM UNNEST($2::TEXT[], $3::BIGINT[]) AS w(ref_name, epoch)) \
         UPDATE land_leases AS l SET renewed_at = now.ms, expires_at = now.ms + $4 \
         FROM now, want \
         WHERE l.repo_id = 1 AND l.holder = $1 AND l.ref_name = want.ref_name \
           AND l.epoch = want.epoch \
         RETURNING l.ref_name, l.holder, l.address, l.protocol, l.epoch, l.renewed_at, \
           l.expires_at, now.ms AS now"
    ))
    .bind(holder)
    .bind(&names)
    .bind(&epochs)
    .bind(ms(timing.ttl))
    .fetch_all(pool)
    .await
    .map_err(storage)?;
    let mut out = vec![None; held.len()];
    for r in &rows {
        let name: String = r.get("ref_name");
        if let Some(i) = names.iter().position(|n| *n == name) {
            out[i] = Some(lease(&name, r, r.get("now"), timing).remaining);
        }
    }
    Ok(out)
}

pub(crate) async fn pg_release(
    pool: &PgPool,
    ref_name: &str,
    holder: &str,
    epoch: u64,
) -> Result<bool> {
    let done = sqlx::query(
        "UPDATE land_leases SET renewed_at = 0, expires_at = 0 \
         WHERE repo_id = 1 AND ref_name = $1 AND holder = $2 AND epoch = $3",
    )
    .bind(ref_name)
    .bind(holder)
    .bind(i64::try_from(epoch).unwrap_or(i64::MAX))
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
    ref_name: &str,
    timing: LeaseTiming,
) -> Result<Option<LandLease>> {
    let row = sqlx::query(
        "SELECT holder, address, protocol, epoch, renewed_at, expires_at \
         FROM land_leases WHERE repo_id = 1 AND ref_name = ?",
    )
    .bind(ref_name)
    .fetch_optional(pool)
    .await
    .map_err(storage)?;
    Ok(row.map(|r| lease(ref_name, &r, now_ms(), timing)))
}

pub(crate) async fn sqlite_acquire(
    pool: &SqlitePool,
    claim: &LeaseClaim<'_>,
    timing: LeaseTiming,
) -> Result<LandLease> {
    let now = now_ms();
    sqlx::query(
        "INSERT INTO land_leases \
           (ref_name, holder, address, protocol, epoch, renewed_at, expires_at) \
         VALUES (?1, ?2, ?3, ?4, 1, ?5, ?5 + ?6) \
         ON CONFLICT (repo_id, ref_name) DO UPDATE SET \
           epoch = CASE WHEN land_leases.holder = excluded.holder \
                   THEN land_leases.epoch ELSE land_leases.epoch + 1 END, \
           holder = excluded.holder, address = excluded.address, \
           protocol = excluded.protocol, renewed_at = excluded.renewed_at, \
           expires_at = excluded.expires_at \
         WHERE land_leases.holder = excluded.holder \
           OR land_leases.expires_at <= excluded.renewed_at \
           OR land_leases.renewed_at + ?7 <= excluded.renewed_at",
    )
    .bind(claim.ref_name)
    .bind(claim.holder)
    .bind(claim.address)
    .bind(i64::from(claim.protocol))
    .bind(now)
    .bind(ms(timing.ttl))
    .bind(ms(timing.stale_after))
    .execute(pool)
    .await
    .map_err(storage)?;
    sqlite_get(pool, claim.ref_name, timing)
        .await?
        .ok_or_else(|| storage("land lease vanished after it was claimed"))
}

pub(crate) async fn sqlite_renew(
    pool: &SqlitePool,
    holder: &str,
    held: &[(String, u64)],
    timing: LeaseTiming,
) -> Result<Vec<Option<Duration>>> {
    let now = now_ms();
    let mut tx = pool.begin().await.map_err(storage)?;
    let mut out = Vec::with_capacity(held.len());
    for (name, epoch) in held {
        let done = sqlx::query(
            "UPDATE land_leases SET renewed_at = ?1, expires_at = ?1 + ?2 \
             WHERE repo_id = 1 AND ref_name = ?3 AND holder = ?4 AND epoch = ?5",
        )
        .bind(now)
        .bind(ms(timing.ttl))
        .bind(name)
        .bind(holder)
        .bind(i64::try_from(*epoch).unwrap_or(i64::MAX))
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
    ref_name: &str,
    holder: &str,
    epoch: u64,
) -> Result<bool> {
    let done = sqlx::query(
        "UPDATE land_leases SET renewed_at = 0, expires_at = 0 \
         WHERE repo_id = 1 AND ref_name = ? AND holder = ? AND epoch = ?",
    )
    .bind(ref_name)
    .bind(holder)
    .bind(i64::try_from(epoch).unwrap_or(i64::MAX))
    .execute(pool)
    .await
    .map_err(storage)?;
    Ok(done.rows_affected() == 1)
}
