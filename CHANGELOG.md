# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-09-27

Several fixes below change runtime behaviour: options that were silently
ignored now take effect, and `N1ql.where()` operators are now honoured. Code
that worked against the old behaviour — knowingly or not — should be reviewed
before upgrading.

### Fixed

- **`CouchbaseHelper` operation options were never applied.** All key-value
  methods (`insert`, `upsert`, `replace`, `get`, `remove` and their `*_multi`
  variants) passed their options to the SDK as the keyword `opts=...`, which
  the SDK treats as a single unknown option rather than as operation options.
  As a result `expiry`, the session's KV timeout, `per_key_opts` and any other
  option (e.g. durability) had no effect. They are now passed correctly.
  - Documents written with an `expiry` now actually expire.
  - KV operations now use the session's `Timeout.kv` (default 5 s) instead of
    the SDK's default timeouts.
- **`N1ql.where()` / `or_where()` ignored the operator in the key.** Every
  condition was emitted as `=`, so e.g. `where("age>", 18)` queried
  `age = 18`. Operators are now parsed and kept. Supported: `=`, `==`, `!=`,
  `<>`, `<`, `<=`, `>`, `>=`, `LIKE`, `NOT LIKE`, `IS NULL`, `IS NOT NULL`
  (case-insensitive).
- **`N1ql.where(key, None)` never produced `IS NULL`.** A key without an
  operator and a `None` value now becomes `key IS NULL`. `IS NULL` and
  `IS NOT NULL` conditions no longer get a positional parameter, which
  previously produced invalid SQL++ (`key IS NULL $1`). An explicit operator
  with `None`, e.g. `where("key=", None)`, still compares against a `None`
  parameter.
- **`N1ql` query consistency and timeout were discarded.** `_execute()` set
  them on an `N1QLQuery` object that was never used. `consistency` is now sent
  as `scan_consistency`, and queries use the session's `Timeout.query`.
- **`CouchbaseHelper.view_query()` returned `None` for successful queries.**
  Rows were only returned when no metadata was available or when `skip`
  exceeded the total row count. It now always returns the query's rows.
- **`CouchbaseHelper.view_query()` could raise from its error handler.**
  Logging an exception raised without arguments caused an `IndexError`.

### Changed

- `N1ql` now logs query errors through its logger (`logger.error`) instead of
  printing them to stdout.

### Removed

- `couchbase_helper.converters` (`escape_string`, `escape_str`). The module
  was unused by the package; import it from
  [PyMySQL](https://github.com/PyMySQL/PyMySQL) if you relied on it.

### Development

- Test coverage of the package raised from 72 % to 99 % (100 % of lines),
  with branch coverage enabled. The previous 83 % badge also counted the test
  files themselves.
- Coverage is now measured and enforced in every tox/CI test environment,
  configured in `pyproject.toml` (package only, branch coverage,
  `fail_under = 98`).
- Replaced the `fake-couchbase` test dependency with lightweight stubs
  (`tests/stubs.py`). Calls recorded by the stubs are checked against the real
  SDK's method signatures and option handling, so options the SDK would ignore
  are caught — which is how the options bug above was found.
- Removed the `fake-couchbase` and `pytest-order` test requirements; tests no
  longer depend on execution order.
- New tests for `retry`, `options`, `timeout` and `_compat` version detection,
  and extended tests for `Session`, `CouchbaseHelper` and `N1ql`, including
  exact SQL++ statement assertions.

## [0.1.5] and earlier

See the [git history](https://github.com/sitzz/python-couchbase-helper/commits/v0.1.5).

[Unreleased]: https://github.com/sitzz/python-couchbase-helper/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/sitzz/python-couchbase-helper/compare/v0.1.5...v0.2.0
[0.1.5]: https://github.com/sitzz/python-couchbase-helper/releases/tag/v0.1.5
