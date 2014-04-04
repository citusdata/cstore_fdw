---
title: cstore_fdw
tagline: Fast columnar store for analytics with PostgreSQL
layout: default
zip_url: https://github.com/citusdata/cstore_fdw/archive/master.zip
tar_url: https://github.com/citusdata/cstore_fdw/archive/master.tar.gz
---

`cstore_fdw` is an open source PostgreSQL extension providing a [column-oriented][] storage backend based on the ORC file format. By leveraging the powerful [foreign data wrapper][fdw] feature introduced in PostgreSQL 9.1, `cstore_fdw` cleanly supplies an alternate storage format while seamlessly integrating with all existing PostgreSQL data types, including extension types such as `hstore` and `json`.

# Overview

The extension has several strengths:

  1. Faster Analytics — Fit more data in memory and reduce disk I/O
  2. Reduced Storage Needs — Use compression to save space
  3. Flexibility — Mix row- and column-based tables in the same DB
  4. Community — Benefit from PostgreSQL compatibility and open development

`cstore_fdw` integrates perfectly whether you're using stock PostgreSQL or our distributed solution, [CitusDB][citus]. Learn more about it on our [blog post][cstore blog], or dive into the [extension code][cstore repo] yourself.

# Faster Analytics

The extension avoids reading data from any columns unrelated to a query. In fact, it will even attempt to avoid reading data _within_ a relevant column if it can determine this is possible using its skip list indices. Because of this, `cstore_fdw` is best for analytics workloads making heavy use of specific columns.

These optimizations achieve a fivefold reduction in disk I/O in many cases, but if disk I/O is unavoidable, `cstore_fdw` lets your database know its best guess of these costs. PostgreSQL will use these estimates for accurate planning even when joining a `cstore_fdw` table with traditional tables in the same query.

| Table Type  | TPC-H 3 | TPC-H 5 | TPC-H 6 | TPC-H 10 |
| ----------- | ------- | ------- | ------- | -------- |
| PostgreSQL  |    4444 |    4444 |    3512 |     4433 |
| cstore      |     786 |     754 |     756 |      869 |
| cstore (LZ) |     322 |     346 |     269 |      302 |

<figure class='chart' title='I/O Utilization'>
  <div title='Disk I/O (MiB)'></div>
  <figcaption>4GB data using PostgreSQL 9.3 on m1.xlarge</figcaption>
</figure>

| Table Type  | TPC-H 3 | TPC-H 5 | TPC-H 6 | TPC-H 10 |
| ----------- | ------- | ------- | ------- | -------- |
| PostgreSQL  |    42.4 |    42.3 |    33.7 |     42.7 |
| cstore      |    24.7 |    23.5 |    14.5 |     23.9 |
| cstore (LZ) |    22.1 |    22.3 |    12.6 |       20 |

<figure class='chart' title='Query Speed'>
  <div title='Duration (seconds)'></div>
  <figcaption>4GB data using PostgreSQL 9.3 on m1.xlarge</figcaption>
</figure>

While not identical, the _Optimized Row Columnar_ (_ORC_) file format uses methods similar to `cstore_fdw`. Reading the Apache Software Foundation's [description of that format][ORC format] may shed some light on our own implementation.

# Lower Storage

Additionally, compression can be enabled on a table to achieve much denser storage: data size is typically reduced by another factor of two or four. `cstore_fdw` uses PostgreSQL's own implementation of an LZ family compression technique ([the same][pg_lzcompress] used to compress TOASTed values) to achieve moderate compression with fast decompression speed. And because of the columnar nature of the storage, only columns related to the query need be decompressed.

| Table Type  | lineitem | orders | part | customer |
| ----------- | -------- | ------ | ---- | -------- |
| PostgreSQL  |     3515 |    816 |  128 |      112 |
| cstore      |     3708 |    751 |  129 |      108 |
| cstore (LZ) |     1000 |    235 |   29 |       45 |

<figure class='chart' title='Disk Space'>
  <div title='Disk Space (MiB)'></div>
  <figcaption>4GB data using PostgreSQL 9.3 on m1.xlarge</figcaption>
</figure>

# Flexibility

While some workloads will benefit from columnar storage, not all will. Fortunately, it's easy to mix table types within a single database: because `cstore_fdw` is a foreign data wrapper, it's just another one of the many primitives PostgreSQL provides to do your job the way you want. For instance, the following is allowed:

```sql
CREATE FOREIGN TABLE cstore_table
  (num integer, name text)
SERVER cstore_server
OPTIONS (filename '/var/tmp/testing.cstore');

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

For now, `COPY` is the only supported way to get data into a `cstore_fdw`-backed table. See the [PostgreSQL documentation][sql copy] for more details.

# Community

`cstore_fdw` fully integrates with your existing PostgreSQL installations. Create tables and copy data into column-oriented files when needed. Use normal PostgreSQL tables where that fits best. Even add other foreign data wrappers where it makes sense: PostgreSQL's modularity lets you design your system the way you want.

`cstore_fdw` has been extensively tested with every one of the [native data types][data types] supported by stock PostgreSQL as well as the [`hstore`][hstore] and [HyperLogLog][hll] extension types, so don't feel hesitant to try it with any schemas that might feel "exotic" in another RDBMS. This support is backed using PostgreSQL's own binary formats to avoid extra (de)serialization costs at query time, so all the types you already know are here at their native speeds.

Last but not least, the extension is open source. The folks at Citus Data have released it under an open source license, and look forward to addressing any issues you may have.

[column-oriented]: http://en.wikipedia.org/w/index.php?oldid=598438648
[fdw]: http://www.postgresql.org/docs/current/static/fdwhandler.html
[citus]: http://www.citusdata.com
[cstore blog]: http://www.citusdata.com/blog/76-postgresql-columnar-store-for-analytics
[cstore repo]: {{site.github.repository_url}}
[ORC format]: https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=31818911
[pg_lzcompress]: http://www.postgresql.org/docs/current/static/storage-toast.html
[sql copy]: http://www.postgresql.org/docs/current/static/sql-copy.html
[data types]: http://www.postgresql.org/docs/current/static/datatype.html
[hstore]: http://www.postgresql.org/docs/current/static/hstore.html
[hll]: https://github.com/aggregateknowledge/postgresql-hll
