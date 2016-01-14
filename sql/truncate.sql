--
-- Test the TRUNCATE TABLE command for cstore_fdw tables.
--

-- Check that files for the automatically managed table exist in the
-- cstore_fdw/{databaseoid} directory.
SELECT count(*) FROM (
	SELECT pg_ls_dir('cstore_fdw/' || databaseoid ) FROM (
	SELECT oid::text databaseoid FROM pg_database WHERE datname = current_database()
	) AS q1) AS q2;

-- CREATE a cstore_fdw table, fill with some data --
CREATE FOREIGN TABLE cstore_truncate_test (a int, b int) SERVER cstore_server;
CREATE FOREIGN TABLE cstore_truncate_test_second (a int, b int) SERVER cstore_server;
CREATE TABLE cstore_truncate_test_regular (a int, b int);

INSERT INTO cstore_truncate_test select a, a from generate_series(1, 10) a;

-- query rows
SELECT * FROM cstore_truncate_test;

TRUNCATE TABLE cstore_truncate_test;

SELECT * FROM cstore_truncate_test;

SELECT COUNT(*) from cstore_truncate_test;

-- make sure data files still present 
SELECT count(*) FROM (
	SELECT pg_ls_dir('cstore_fdw/' || databaseoid ) FROM (
	SELECT oid::text databaseoid FROM pg_database WHERE datname = current_database()
	) AS q1) AS q2;

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

-- test if truncate on empty table works
TRUNCATE TABLE cstore_truncate_test;
SELECT * from cstore_truncate_test;

DROP FOREIGN TABLE cstore_truncate_test, cstore_truncate_test_second;
DROP TABLE cstore_truncate_test_regular;
