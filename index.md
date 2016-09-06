---
title: cstore_fdw
tagline: Fast columnar store for analytics with PostgreSQL
layout: default
zip_url: https://github.com/citusdata/cstore_fdw/archive/v1.5.0.zip
tar_url: https://github.com/citusdata/cstore_fdw/archive/v1.5.0.tar.gz
---

Introducing `cstore_fdw`, the first [column-oriented][] store available for PostgreSQL. Using it will let you:

  * Leverage typical analytics benefits of columnar stores
  * Deploy on stock PostgreSQL or scale-out PostgreSQL ([CitusDB][citus])

Download and get started at {{site.github.repository_url}}.

# Highlights

Key areas improved by this extension:

  * **[Faster Analytics](#toc_1)** — Reduce analytics query disk and memory use by 10x
  * **[Lower Storage](#toc_2)** — Compress data by 3x
  * **[Easy Setup](#toc_3)** — Deploy as standard PostgreSQL extension
  * **[Flexibility](#toc_4)** — Mix row- and column-based tables in the same DB
  * **[Community](#toc_5)** — Benefit from PostgreSQL compatibility and open development

Learn more on our [blog post][cstore blog].

# Faster Analytics

`cstore_fdw` brings substantial performance benefits to analytics-heavy workloads:

  * Column projections: only read columns relevant to the query
  * Compressed data: higher data density reduces disk I/O
  * Skip indexes: row group stats permit skipping irrelevant rows
  * Stats collections: integrates with PostgreSQL's own query optimizer
  * PostgreSQL-native formats: no deserialization overhead at query time

| Table Type  | TPC-H 3 | TPC-H 5 | TPC-H 6 | TPC-H 10 |
| ----------- | ------- | ------- | ------- | -------- |
| PostgreSQL  |    4444 |    4444 |    3512 |     4433 |
| cstore      |     786 |     754 |     756 |      869 |
| cstore (LZ) |     322 |     346 |     269 |      302 |

<figure class='chart' title='I/O Utilization'>
  <div title='Disk I/O (MiB)'></div>
  <figcaption>4GB data using PostgreSQL 9.3 on m1.xlarge</figcaption>
</figure>

# Lower Storage

Cleanly implements full-table compression:

  * Uses PostgreSQL's own [LZ family compression technique][pg_lzcompress]
  * Only decompresses columns needed by the query
  * Extensible to support different codecs

# Easy Setup

If you know how to use PostgreSQL extensions, you know how to use `cstore_fdw`:

  * Deploy as standard PostgreSQL extension
  * Simply specify table type at creation time using [FDW][] commands
  * Copy data into your tables using standard PostgreSQL [`COPY`][sql copy] command

# Flexibility

Have the best of all worlds… mix row- and column-based tables in the same DB:

```sql
CREATE FOREIGN TABLE cstore_table
  (num integer, name text)
SERVER cstore_server;

CREATE TABLE plain_table
  (num integer, name text);

COPY cstore_table FROM STDIN (FORMAT csv);
-- 1, foo
-- 2, bar
-- 3, baz
-- \.

COPY plain_table FROM STDIN (FORMAT csv);
-- 4, foo
-- 5, bar
-- 6, baz
-- \.

SELECT * FROM cstore_table c, plain_table p WHERE c.name=p.name;
-- num | name | num | name 
-------+------+-----+------
--   1 |  foo |   4 |  foo
--   2 |  bar |   5 |  bar
--   3 |  baz |   6 |  baz
```

# Community

Join the [cstore users Google Group][cstore-users].

Full integration with rich PostgreSQL ecosystem:

  * Compatible with all existing PostgreSQL [data types][]
  * Leverage semi-structured data using [`hstore`][hstore] or [`json`][json]
  * Quickly keep track of distinct values with [HyperLogLog][hll]

Learn more about the [Optimized Row Column (ORC) file format][ORC] , which influenced the layout used by `cstore_fdw`, or dive into [the code][cstore repo].

[column-oriented]: http://en.wikipedia.org/w/index.php?oldid=598438648
[citus]: http://citusdata.com/
[cstore blog]: http://www.citusdata.com/blog/76-postgresql-columnar-store-for-analytics
[fdw]: http://www.postgresql.org/docs/current/static/fdwhandler.html
[pg_lzcompress]: http://www.postgresql.org/docs/current/static/storage-toast.html
[ORC]: https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=31818911
[cstore repo]: {{site.github.repository_url}}
[FDW]: http://www.postgresql.org/docs/current/static/sql-createforeigndatawrapper.html
[sql copy]: http://www.postgresql.org/docs/current/static/sql-copy.html
[data types]: http://www.postgresql.org/docs/current/static/datatype.html
[hstore]: http://www.postgresql.org/docs/current/static/hstore.html
[json]: http://www.postgresql.org/docs/current/static/datatype-json.html
[hll]: https://github.com/aggregateknowledge/postgresql-hll
[cstore-users]: https://groups.google.com/forum/#!forum/cstore-users
