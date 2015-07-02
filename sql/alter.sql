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

-- verify select runs fine
SELECT * FROM test_alter_drop_column;

-- verify column is dropped and errors here
SELECT a FROM test_alter_drop_column;

-- should return all b's
SELECT b FROM test_alter_drop_column;

-- should fail
INSERT INTO test_alter_drop_column (SELECT 3, 5, 8);

-- should succeed
INSERT INTO test_alter_drop_column (SELECT 3, 5, 8);

SELECT * from test_alter_drop_column;

DROP FOREIGN TABLE test_alter_drop_column;
