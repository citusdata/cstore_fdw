/* cstore_fdw/cstore_fdw--1.5.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION cstore_fdw" to load this file. \quit

CREATE FUNCTION cstore_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION cstore_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER cstore_fdw
HANDLER cstore_fdw_handler
VALIDATOR cstore_fdw_validator;

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

CREATE OR REPLACE FUNCTION cstore_clean_table_resources(oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION cstore_drop_trigger()
	RETURNS event_trigger
	LANGUAGE plpgsql
	AS $csdt$
DECLARE dropped_object record;
BEGIN
	FOR dropped_object IN SELECT * FROM pg_event_trigger_dropped_objects() LOOP

		IF dropped_object.object_type <> 'foreign table' THEN
			CONTINUE;
		END IF;

		IF EXISTS(SELECT * FROM pg_catalog.pg_cstore_tables WHERE logicalrelid = dropped_object.objid) THEN
			PERFORM public.cstore_clean_table_resources(dropped_object.objid);
			PERFORM public.unregister_cstore_table(dropped_object.objid);
		END IF;
	END LOOP;
END;
$csdt$;

CREATE EVENT TRIGGER cstore_drop_event
    ON SQL_DROP
    EXECUTE PROCEDURE cstore_drop_trigger();

CREATE TABLE public.pg_cstore_tables (logicalrelid regclass PRIMARY KEY);
ALTER TABLE public.pg_cstore_tables SET SCHEMA pg_catalog;


CREATE OR REPLACE FUNCTION public.register_cstore_table(table_name oid)
	RETURNS boolean
	LANGUAGE plpgsql
	AS $rct$
BEGIN
    IF EXISTS(
		SELECT fdwname FROM pg_foreign_table ft, pg_foreign_server fs, pg_foreign_data_wrapper fdw
			WHERE 
				ft.ftrelid = table_name AND
				fs.oid = ft.ftserver AND
				fdw.oid = fs.srvfdw AND
				fdw.fdwname = 'cstore_fdw'
				) AND
		NOT EXISTS(SELECT * FROM pg_cstore_tables WHERE logicalrelid = table_name)
	THEN
		INSERT INTO pg_cstore_tables SELECT table_name;
		RETURN true;
	END IF;

	return false;
END;
$rct$;

CREATE OR REPLACE FUNCTION public.unregister_cstore_table(table_name oid)
	RETURNS void
	LANGUAGE plpgsql
	AS $uct$
BEGIN
	DELETE FROM pg_cstore_tables WHERE logicalrelid = table_name;
END;
$uct$;
