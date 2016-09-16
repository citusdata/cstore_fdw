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

-- make sure data files still present (expect total 4 files 2 per relation)
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
TRUNCATE TABLE cstore_truncate_test, 
			   cstore_truncate_test_regular,
			   cstore_truncate_test_second;

SELECT * from cstore_truncate_test;
SELECT * from cstore_truncate_test_second;
SELECT * from cstore_truncate_test_regular;
SELECT * from cstore_truncate_test_compressed;

-- test if truncate on empty table works
TRUNCATE TABLE cstore_truncate_test;
SELECT * from cstore_truncate_test;

DROP FOREIGN TABLE cstore_truncate_test, cstore_truncate_test_second, cstore_truncate_test_compressed;
DROP TABLE cstore_truncate_test_regular;

-- make sure data files are removed
-- 1 file per relation is kept by postgresql
SELECT count(*) FROM (
	SELECT * FROM pg_ls_dir(:'databasedir') as file_name
	WHERE (regexp_split_to_array(file_name, '_'))[1] IN 
		(SELECT * FROM unnest(:'relation_oids'::text[])) ) q1;
