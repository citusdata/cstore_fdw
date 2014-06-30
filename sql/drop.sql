--
-- Test the DROP FOREIGN TABLE command for cstore_fdw tables.
--

-- Check that files for the automatically managed table exist in the
-- cstore_fdw/{databaseoid} directory.
SELECT count(*) FROM (
	SELECT pg_ls_dir('cstore_fdw/' || databaseoid ) FROM (
	SELECT oid::text databaseoid FROM pg_database WHERE datname = current_database()
	) AS q1) AS q2;

-- DROP cstore_fdw tables
DROP FOREIGN TABLE contestant;
DROP FOREIGN TABLE contestant_compressed;

-- Check that the files have been deleted and the directory is empty after the
-- DROP table command.
SELECT count(*) FROM (
	SELECT pg_ls_dir('cstore_fdw/' || databaseoid ) FROM (
	SELECT oid::text databaseoid FROM pg_database WHERE datname = current_database()
	) AS q1) AS q2;
