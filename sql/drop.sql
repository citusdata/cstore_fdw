--
-- Tests the different DROP commands for cstore_fdw tables.
--
-- DROP FOREIGN TABLE
-- DROP SCHEMA
-- DROP EXTENSION
-- DROP DATABASE
--

SELECT current_database() current_db \gset

-- determine base directory where tables files are created
SELECT ('base/' || databaseoid)::text as databasedir
	FROM (
		SELECT oid::text databaseoid
			FROM pg_database
			WHERE datname = current_database()) pgdir
	\gset

-- record created cstore relation oids as text
SELECT array_agg(relation_name::regclass::oid::text) as relation_oids
	FROM (
		SELECT unnest(ARRAY[
			'contestant',
			'contestant_compressed']) relation_name) l
	\gset

-- DROP cstore_fdw tables
DROP FOREIGN TABLE contestant;
DROP FOREIGN TABLE contestant_compressed;

-- Create a cstore_fdw table under a schema and drop it.
CREATE SCHEMA test_schema;
CREATE FOREIGN TABLE test_schema.test_table(data int) SERVER cstore_server;
-- append this relation's oid to relation_oids
SELECT :'relation_oids'::text[] || relation_oid as relation_oids
	FROM (
		SELECT 'test_schema.test_table'::regclass::oid::text as relation_oid) q1
	\gset
DROP SCHEMA test_schema CASCADE;

-- Check that the files have been deleted from databasedir
-- note that postgres maintaines 1 file per relation even
-- after the relation is dropped. 0 sized file is removed
-- during checkpoint
SELECT count(*) FROM (
	SELECT * FROM pg_ls_dir(:'databasedir') as file_name
	WHERE (regexp_split_to_array(file_name, '_'))[1] IN 
		(SELECT * FROM unnest(:'relation_oids'::text[])) ) q1;

-- create a test db and switch to it
CREATE DATABASE db_to_drop;
\c db_to_drop

CREATE EXTENSION cstore_fdw;
CREATE SERVER cstore_server FOREIGN DATA WRAPPER cstore_fdw;
CREATE FOREIGN TABLE test_table(data int) SERVER cstore_server;

SELECT ('base/' || databaseoid)::text as databasedir
	FROM (
		SELECT oid::text databaseoid
			FROM pg_database
			WHERE datname = current_database()) pgdir
	\gset

SELECT array_agg(relation_name::regclass::oid::text) as relation_oids
	FROM (
		SELECT unnest(ARRAY['test_table']) relation_name) l
	\gset

-- should see 2 files, data and footer file for single table
SELECT count(*) FROM (
	SELECT * FROM pg_ls_dir(:'databasedir') as file_name
	WHERE (regexp_split_to_array(file_name, '_'))[1] IN 
		(SELECT * FROM unnest(:'relation_oids'::text[])) ) q1;

DROP EXTENSION cstore_fdw CASCADE;

-- should see a single file
SELECT count(*) FROM (
	SELECT * FROM pg_ls_dir(:'databasedir') as file_name
	WHERE (regexp_split_to_array(file_name, '_'))[1] IN 
		(SELECT * FROM unnest(:'relation_oids'::text[])) ) q1;
		
CHECKPOINT;

-- should see no files
SELECT count(*) FROM (
	SELECT * FROM pg_ls_dir(:'databasedir') as file_name
	WHERE (regexp_split_to_array(file_name, '_'))[1] IN 
		(SELECT * FROM unnest(:'relation_oids'::text[])) ) q1;

\c :current_db

DROP DATABASE db_to_drop;