--
-- Test the TRUNCATE TABLE command for cstore_fdw tables.
--

-- CREATE a cstore_fdw table, fill with some data --
CREATE FOREIGN TABLE cstore_truncate_test (a int, b int) SERVER cstore_server;
CREATE FOREIGN TABLE cstore_truncate_test_second (a int, b int) SERVER cstore_server;
CREATE FOREIGN TABLE cstore_truncate_test_compressed (a int, b int) SERVER cstore_server OPTIONS (compression 'pglz');
CREATE TABLE cstore_truncate_test_regular (a int, b int);

-- record created cstore relation oids as text
SELECT array_agg(relation_name::regclass::oid::text) as relation_oids
	FROM (
		SELECT unnest(ARRAY[
			'cstore_truncate_test',
			'cstore_truncate_test_second',
			'cstore_truncate_test_compressed']) relation_name) l
	\gset

-- determine base directory where tables files are created
SELECT ('base/' || databaseoid)::text as databasedir
	FROM (
		SELECT oid::text databaseoid
			FROM pg_database
			WHERE datname = current_database()) pgdir
	\gset
	
INSERT INTO cstore_truncate_test select a, a from generate_series(1, 10) a;

INSERT INTO cstore_truncate_test_compressed select a, a from generate_series(1, 10) a;
INSERT INTO cstore_truncate_test_compressed select a, a from generate_series(1, 10) a;

-- query rows
SELECT * FROM cstore_truncate_test;

TRUNCATE TABLE cstore_truncate_test;

SELECT * FROM cstore_truncate_test;

SELECT COUNT(*) from cstore_truncate_test;
SELECT count(*) FROM cstore_truncate_test_compressed;
TRUNCATE TABLE cstore_truncate_test_compressed;
SELECT count(*) FROM cstore_truncate_test_compressed;
SELECT cstore_table_size('cstore_truncate_test_compressed');

TRUNCATE TABLE cstore_truncate_test_second;

-- make sure data files still present (expect total 3 files, 1 per relation)
SELECT count(*) FROM (
	SELECT * FROM pg_ls_dir(:'databasedir') as file_name
	WHERE (regexp_split_to_array(file_name, '_'))[1] IN 
		(SELECT * FROM unnest(:'relation_oids'::text[])) ) q1;

INSERT INTO cstore_truncate_test select a, a from generate_series(1, 10) a;
INSERT INTO cstore_truncate_test_regular select a, a from generate_series(10, 20) a;
INSERT INTO cstore_truncate_test_second select a, a from generate_series(20, 30) a;

SELECT * from cstore_truncate_test;

SELECT * from cstore_truncate_test_second;

SELECT * from cstore_truncate_test_regular;

-- make sure multi truncate works
-- notice that the same table might be repeated
TRUNCATE TABLE cstore_truncate_test,
			   cstore_truncate_test_regular,
			   cstore_truncate_test_second,
   			   cstore_truncate_test;

SELECT * from cstore_truncate_test;
SELECT * from cstore_truncate_test_second;
SELECT * from cstore_truncate_test_regular;
SELECT * from cstore_truncate_test_compressed;

-- test if truncate on empty table works
TRUNCATE TABLE cstore_truncate_test;
SELECT * from cstore_truncate_test;

DROP FOREIGN TABLE cstore_truncate_test, cstore_truncate_test_second, cstore_truncate_test_compressed;
DROP TABLE cstore_truncate_test_regular;

-- test truncate with schema
CREATE SCHEMA truncate_schema;
CREATE FOREIGN TABLE truncate_schema.truncate_tbl (id int) SERVER cstore_server OPTIONS(compression 'pglz');
INSERT INTO truncate_schema.truncate_tbl SELECT generate_series(1, 100);
SELECT COUNT(*) FROM truncate_schema.truncate_tbl;

TRUNCATE TABLE truncate_schema.truncate_tbl;
SELECT COUNT(*) FROM truncate_schema.truncate_tbl;

INSERT INTO truncate_schema.truncate_tbl SELECT generate_series(1, 100);

-- create a user that can not truncate
CREATE USER truncate_user;
GRANT USAGE ON SCHEMA truncate_schema TO truncate_user;
GRANT SELECT ON TABLE truncate_schema.truncate_tbl TO truncate_user;
REVOKE TRUNCATE ON TABLE truncate_schema.truncate_tbl FROM truncate_user;

SELECT current_user \gset

\c - truncate_user
-- verify truncate command fails and check number of rows
SELECT count(*) FROM truncate_schema.truncate_tbl;
TRUNCATE TABLE truncate_schema.truncate_tbl;
SELECT count(*) FROM truncate_schema.truncate_tbl;

-- switch to super user, grant truncate to truncate_user 
\c - :current_user
GRANT TRUNCATE ON TABLE truncate_schema.truncate_tbl TO truncate_user;

-- verify truncate_user can truncate now
\c - truncate_user
SELECT count(*) FROM truncate_schema.truncate_tbl;
TRUNCATE TABLE truncate_schema.truncate_tbl;
SELECT count(*) FROM truncate_schema.truncate_tbl;

\c - :current_user

-- cleanup
DROP SCHEMA truncate_schema CASCADE;
DROP USER truncate_user;


-- make sure data files are removed
-- 1 file per relation is kept by postgresql
SELECT count(*) FROM (
	SELECT * FROM pg_ls_dir(:'databasedir') as file_name
	WHERE (regexp_split_to_array(file_name, '_'))[1] IN 
		(SELECT * FROM unnest(:'relation_oids'::text[])) ) q1;

-- checkpoint would remove unused 0 sized files
CHECKPOINT;
SELECT count(*) FROM (
	SELECT * FROM pg_ls_dir(:'databasedir') as file_name
	WHERE (regexp_split_to_array(file_name, '_'))[1] IN 
		(SELECT * FROM unnest(:'relation_oids'::text[])) ) q1;

