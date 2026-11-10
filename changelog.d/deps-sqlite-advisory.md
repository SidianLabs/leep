### Record the bundled SQLite advisory

- **Changed.** `deny.toml` records RUSTSEC-2026-0459 (bundled SQLite via
  `sqlx-sqlite` 0.9) with its reason and a removal date: Leep opens only
  the operator's own database files, and the fixed `libsqlite3-sys` is
  outside `sqlx` 0.9's range.
