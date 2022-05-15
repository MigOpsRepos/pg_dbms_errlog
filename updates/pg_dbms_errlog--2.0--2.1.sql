---------------------------------------------------------------------------------
-- pg_dbms_errlog extension for PostgreSQL
--	Emulate DBMS_ERRLOG Oracle module but in a simplistic way.
---------------------------------------------------------------------------------

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION pg_dbms_errlog UPDATE" to load this file. \quit

-- check the functions bodies as creation time, enabled by default
SET LOCAL check_function_bodies = on ;
-- make sure of client encofing
SET LOCAL client_encoding = 'UTF8';

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
    err_log_namespace text;
BEGIN
    IF dml_table_name IS NULL THEN
        RAISE EXCEPTION 'You must specify a DML table name.';
    END IF;

    -- Verify that the DML table exists and get the
    -- schema and name of the table from the catalog
    SELECT n.nspname, c.relname INTO STRICT err_log_namespace, dml_table_name FROM pg_catalog.pg_namespace n JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid) WHERE c.oid = dml_table_name::regclass::oid;
    -- Set the name of the error table if it is not provided
    IF err_log_table_name IS NULL THEN
        fqdn_pos := position('.' IN dml_table_name) + 1;
        err_log_tbname := 'ERR$_'||substring(dml_table_name FROM fqdn_pos FOR 58);
    ELSE
        -- Remove quoting from table name if any
        err_log_table_name := replace(err_log_table_name, '"', '');
        fqdn_pos := position('.' IN err_log_table_name) + 1;
        err_log_tbname := substring(err_log_table_name FROM fqdn_pos FOR 58);
	IF fqdn_pos > 1 THEN
            err_log_namespace := substring(err_log_table_name FROM 1 FOR fqdn_pos - 2);
        END IF;
    END IF;
    err_log_tbname := quote_ident(err_log_namespace) || '.' || quote_ident(err_log_tbname);

    -- Create the error log table
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
