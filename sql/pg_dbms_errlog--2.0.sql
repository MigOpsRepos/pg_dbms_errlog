---------------------------------------------------------------------------------
-- pg_dbms_errlog extension for PostgreSQL
--	Emulate DBMS_ERRLOG Oracle module but in a simplistic way.
---------------------------------------------------------------------------------

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_dbms_errlog" to load this file. \quit

-- check the functions bodies as creation time, enabled by default
SET LOCAL check_function_bodies = on ;
-- make sure of client encofing
SET LOCAL client_encoding = 'UTF8';


-- Create the extension schema
CREATE SCHEMA IF NOT EXISTS @extschema@;
REVOKE ALL ON SCHEMA @extschema@ FROM PUBLIC;
GRANT USAGE ON SCHEMA @extschema@ TO PUBLIC;


-- Create registration table for error loging tables association
CREATE TABLE @extschema@.register_errlog_tables
(
	reldml oid, -- oid of the table where DML are done
	relerrlog oid -- oid of the table for error logging
);
CREATE UNIQUE INDEX ON @extschema@.register_errlog_tables(reldml);
CREATE UNIQUE INDEX ON @extschema@.register_errlog_tables(relerrlog);

-- Include tables into pg_dump
SELECT pg_catalog.pg_extension_config_dump('register_errlog_tables', '');

CREATE OR REPLACE PROCEDURE @extschema@.create_error_log (
    dml_table_name varchar(132), -- name of the DML table to base the error logging table on, can use fqdn.
    err_log_table_name varchar(132) DEFAULT NULL, -- name of the error logging table to create, default is the first 58 characters in the name of the DML table prefixed with 'ERR$_', can use fqdn.
    err_log_table_owner name DEFAULT NULL, -- name of the owner of the error logging table. Default current user.
    err_log_table_space name DEFAULT NULL --  tablespace the error logging table will be created in.
) AS
$$
DECLARE
    sql_create_table text;
    sql_register_table text;
    fqdn_pos int := 0;
    err_log_tbname name := $2;
BEGIN
    IF dml_table_name IS NULL THEN
        RAISE EXCEPTION 'You must specify a DML table name.';
    END IF;
    IF err_log_table_name IS NULL THEN
        fqdn_pos := position('.' IN dml_table_name) + 1;
        err_log_tbname := '"ERR$_'||substring(dml_table_name FROM fqdn_pos FOR 58)||'"';
    END IF;
    sql_create_table := 'CREATE TABLE '||err_log_tbname||' (
        PG_ERR_NUMBER$ text, -- PostgreSQL error number
        PG_ERR_MESG$ text,   -- PostgreSQL error message
        PG_ERR_OPTYP$ char(1), -- Type of operation: insert (I), update (U), delete (D)
        PG_ERR_TAG$ text, -- Label used to identify the DML batch
        PG_ERR_QUERY$ text, -- Query at origin
        PG_ERR_DETAIL$ text -- Detail of the query origin
    )';

    EXECUTE sql_create_table;
    IF err_log_table_owner IS NOT NULL THEN
        EXECUTE 'ALTER TABLE '||err_log_tbname||' OWNER TO '||err_log_table_owner;
    END IF;
    IF err_log_table_space IS NOT NULL THEN
        EXECUTE 'ALTER TABLE '||err_log_tbname||' SET TABLESPACE '||err_log_table_space||' NOWAIT';
    END IF;
    sql_register_table := 'INSERT INTO @extschema@.register_errlog_tables VALUES ('''||dml_table_name||'''::regclass::oid, '''||err_log_tbname||'''::regclass::oid)';
    EXECUTE sql_register_table;
END;
$$
LANGUAGE plpgsql SECURITY INVOKER;

CREATE OR REPLACE FUNCTION @extschema@.unregister_errlog_table()
  RETURNS event_trigger
  AS $$
DECLARE
    sql_unregister_table text;
    relinfo RECORD;
BEGIN
    IF tg_tag = 'DROP TABLE' OR tg_tag = 'DROP SCHEMA' THEN
        FOR relinfo IN SELECT * FROM pg_catalog.pg_event_trigger_dropped_objects() WHERE object_type IN ('table', 'view')
        LOOP
            sql_unregister_table := 'DELETE FROM @extschema@.register_errlog_tables WHERE reldml ='||relinfo.objid||' OR relerrlog = '||relinfo.objid;
            EXECUTE sql_unregister_table;
        END LOOP;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION @extschema@.publish_queue(
    wait_for_completion bool DEFAULT false
) RETURNS bool
LANGUAGE C COST 1000
AS '$libdir/pg_dbms_errlog', 'pg_dbms_errlog_publish_queue';
REVOKE ALL ON FUNCTION @extschema@.publish_queue FROM public;

CREATE EVENT TRIGGER ddl_drop_errlog_table ON sql_drop EXECUTE PROCEDURE @extschema@.unregister_errlog_table();
