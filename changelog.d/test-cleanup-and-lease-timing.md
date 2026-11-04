### Lease claims under load, and Postgres tests that clean up after themselves
- **Fixed.** A lease claim on Postgres or SQLite counted the time left from a
  second read after the claim committed. Under database load that read could
  come after `stale_after`, so a lease the server had just taken looked free
  already, and the indexer treated an index lease it held as not held. The
  claim and its read now share one transaction and one clock reading.
- **Tests.** Every test that makes a Postgres schema or database (leep-meta,
  the leep-server Postgres tests, the end-to-end suite) drops it when it
  ends, a failing test's included. Before this, each leep-meta run left
  about thirty `leep_test_*` schemas behind. `leep-soak` also drops its
  database when it stops on an error before the run ends. It still keeps
  the database after a failed run, or with `--keep-data`.
