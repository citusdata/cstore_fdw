---
title: cstore_fdw
tagline: Fast columnar store for analytics with PostgreSQL
layout: default
zip_url: https://github.com/citusdata/cstore_fdw/archive/master.zip
tar_url: https://github.com/citusdata/cstore_fdw/archive/master.tar.gz
---

`cstore_fdw` is an open source PostgreSQL extension providing a [column-oriented][] storage backend based on the ORC file format. By leveraging the powerful [foreign data wrapper][fdw] feature introduced in PostgreSQL 9.1 it cleanly supplies an alternate storage format while seamlessly integrating with all existing PostgreSQL data types, including extension types such as `hstore` and `json`.

Operation is similarly seamless: simply install the extension and create a foreign table using the wrapper. `COPY` data into your table and you're ready to start analyzing!

# Overview

The extension has several strengths:

  1. Faster Analytics — Fit more data in memory and reduce disk I/O
  2. Lower Storage — Use compression to save space
  3. Flexibility — Mix row- and column-based tables in the same DB
  4. Community — Benefit from PostgreSQL compatibility and open development

`cstore_fdw` integrates perfectly whether you're using stock PostgreSQL or our distributed solution, CitusDB. Learn more about it on our [blog post][cstore blog], or dive into the [extension code][cstore repo] yourself.

# Faster Analytics

`cstore_fdw` is best for analytics workloads making heavy use of specific columns. In particular, it avoids reading data from any columns unrelated to a query—in fact, it will even attempt to avoid reading data _within_ a relevant column if it can determine this is possible using its skip list indices.

These optimizations can add up: a fivefold reduction in disk I/O is common in certain cases. And being a good PostgreSQL extension, `cstore_fdw` lets your database know its best guess of these costs. PostgreSQL will use these estimates for accurate planning even when joining a `cstore_fdw` table with traditional tables in the same query.

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

While not identical, the _Optimized Row Columnar_ (_ORC_) file format uses methods similar to `cstore_fdw`, so their [format description][ORC format] may shed some light on our own implementation.

# Lower Storage

If that's not enough, compression can be enabled on a table to trade some CPU cycles for much denser storage (data size is typically reduced by another factor of two or four). `cstore_fdw` uses PostgreSQL's own implementation of an LZ family compression technique (indeed [the same][pg_lzcompress] used to compress TOASTed values) to achieve moderate compression with fast decompression speed. And because of the columnar nature of the storage, only columns related to the query need be decompressed.

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

Just because you have some queries that could benefit from a columnar layout doesn't mean all of them will. Lucky for you there's no need to choose: because `cstore_fdw` is a foreign data wrapper, it's just another one of the many primitives PostgreSQL provides to do your job the way you want. For instance, the following is allowed:

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

For now, `COPY` is the only supported way to get data into a `cstore_fdw`-backed table, so [read up][sql copy] if you're rusty.

# Community

`cstore_fdw` fully integrates with your existing PostgreSQL installations. Create tables and copy data into column-oriented files when needed. Use normal PostgreSQL tables where that fits best. Even add other foreign data wrappers where it makes sense: PostgreSQL's modularity lets you design your system the way you want.

`cstore_fdw` has been extensively tested with every one of the [native data types][data types] supported by stock PostgreSQL as well as the [`hstore`][hstore] extension types so don't feel hesitant to try it with any schemas that might feel "exotic" in another RDBMS. This support is backed using PostgreSQL's own binary formats to avoid extra (de)serialization costs at query time, so all the types you already know are here at their native speeds.

And of course, the extension is open source. The folks at Citus Data have released it under an open source license, and look forward to addressing any issues you may have.

[column-oriented]: http://en.wikipedia.org/w/index.php?oldid=598438648
[fdw]: http://www.postgresql.org/docs/current/static/fdwhandler.html
[cstore blog]: http://citusdata.com/blog
[cstore repo]: {{site.github.repository_url}}
[ORC format]: https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=31818911
[pg_lzcompress]: http://www.postgresql.org/docs/current/static/storage-toast.html
[sql copy]: http://www.postgresql.org/docs/current/static/sql-copy.html
[data types]: http://www.postgresql.org/docs/current/static/datatype.html
[hstore]: http://www.postgresql.org/docs/current/static/hstore.html
