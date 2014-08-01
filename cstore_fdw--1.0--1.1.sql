/* cstore_fdw/cstore_fdw--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION UPDATE
\echo Use "ALTER EXTENSION cstore_fdw UPDATE TO '1.1'" to load this file. \quit

CREATE FUNCTION cstore_ddl_event_end_trigger()
RETURNS event_trigger
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE EVENT TRIGGER cstore_ddl_event_end
ON ddl_command_end
EXECUTE PROCEDURE cstore_ddl_event_end_trigger();

CREATE FUNCTION cstore_table_size(relation regclass)
RETURNS bigint
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;
