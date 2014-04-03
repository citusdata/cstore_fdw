Requested Features
------------------

* Improve query cost estimation
* Improve write performance
* Improve read performance
* Improve memory usage
* Add checksum logic
* Add new compression methods
* Enable ALTER FOREIGN TABLE ADD COLUMN
* Enable ALTER FOREIGN TABLE DROP COLUMN
* Enable users other than superuser to safely create columnar tables (permissions)
* Transactional semantics
* Add config setting to make pg_fsync() optional


Known Issues
------------

* Reading from an empty table currently errors out. It should return a table with
  no rows.
* Copy command ignores NOT NULL constraints.
* Planning functions don't take into account average column width.
* Planning functions don't correctly take into account block skipping benefits.
* On 32-bit platforms, when file size is outside the 32-bit signed range, EXPLAIN
  command prints incorrect file size.
* If two different columnar tables are configured to point to the same file,
  writes to the underlying file aren't protected from each other.
* Hstore and json types work. However, constructors for hstore and json types
  applied on a tuple return NULL.
* When a data load is in progress, concurrent reads on the table overestimate the
  page count.
* We have a minor memory leak in CStoreEndWrite. We need to also free the
  comparisonFunctionArray.
* block_filtering test fails on Ubuntu because the "da_DK" locale is not enabled
  by default.
* We don't yet incorporate the compression method's impact on disk I/O into cost
  estimates.
* During CREATE FOREIGN TABLE, we don't check if we have access to the given 
  filename's directory path.
* If CitusDB applies a broadcast table join, and if the columnar table is one of
  the smaller tables, we error out.
