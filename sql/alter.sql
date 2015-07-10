--
-- Testing ALTER TABLE on cstore_fdw tables.
--

-- DROP COLUMN TESTS

CREATE FOREIGN TABLE test_alter_drop_column (a int, b int, c int) SERVER cstore_server;

SELECT count(*) FROM test_alter_drop_column;

-- insert few rows
INSERT INTO test_alter_drop_column (SELECT 1, 2, 3);
INSERT INTO test_alter_drop_column (SELECT 4, 5, 6);
INSERT INTO test_alter_drop_column (SELECT 7, 8, 9);

SELECT count(*) FROM test_alter_drop_column;

-- drop a column
ALTER FOREIGN TABLE test_alter_drop_column DROP COLUMN a;

-- test analyze
ANALYZE test_alter_drop_column;

-- verify select runs fine
SELECT * FROM test_alter_drop_column;

-- verify column is dropped and errors here
SELECT a FROM test_alter_drop_column;

-- should return all b's
SELECT b FROM test_alter_drop_column;

-- should fail
INSERT INTO test_alter_drop_column (SELECT 3, 5, 8);

-- should succeed
INSERT INTO test_alter_drop_column (SELECT 5, 8);

SELECT * from test_alter_drop_column;

DROP FOREIGN TABLE test_alter_drop_column;


-- ADD COLUMN TESTS

CREATE FOREIGN TABLE test_alter_add_column (a int) SERVER cstore_server;

SELECT count(*) FROM test_alter_add_column;

--insert some rows
INSERT INTO test_alter_add_column select * from generate_series(1,10);

-- verify they are inserted
SELECT * from test_alter_add_column;

ALTER FOREIGN TABLE test_alter_add_column ADD COLUMN b int;

-- this should display b column with no values
SELECT * from test_alter_add_column;

ALTER FOREIGN TABLE test_alter_add_column ADD COLUMN c int default 3;

-- this should display c column with default 3
SELECT * from test_alter_add_column;

INSERT INTO test_alter_add_column(a,b) (select a, a from generate_series(11, 20) a);

-- this should display c column with default 3
SELECT * from test_alter_add_column;

ALTER FOREIGN TABLE test_alter_add_column ADD COLUMN d text DEFAULT 'TEXT ME';

-- this should display d column with default text me
SELECT * from test_alter_add_column;

-- drop a column to make sure nothing is broken
ALTER FOREIGN TABLE test_alter_add_column DROP COLUMN a;

SELECT * from test_alter_add_column;

DROP FOREIGN TABLE test_alter_add_column;
