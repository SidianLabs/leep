//! The phase 1 coordination demo as a test: twenty agents through the real
//! binaries, on SQLite and (with `LEEP_TEST_POSTGRES_URL`) Postgres. The
//! same scenario prints a narrated transcript as the `coord_demo` example
//! (`scripts/demo-coordination.sh`).

#[path = "../common/mod.rs"]
mod common;
mod scenario;

use common::{serial, PgDb, Stores};

#[test]
fn twenty_agents_coordinate_through_the_real_binaries() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let stores = Stores::sqlite(&tmp.path().join("data"));
    let n = scenario::Narrator::new(false);
    scenario::run(&stores, tmp.path(), &n).assert_all();
}

#[test]
fn twenty_agents_coordinate_through_the_real_binaries_postgres() {
    let _g = serial();
    let Some(db) = PgDb::create("leep_e2e") else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    let stores = Stores::with_meta(&tmp.path().join("data"), db.url.clone());
    let n = scenario::Narrator::new(false);
    scenario::run(&stores, tmp.path(), &n).assert_all();
}
