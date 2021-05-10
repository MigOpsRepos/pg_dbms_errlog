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


ALTER EXTENSION pg_dbms_errlog DROP EVENT TRIGGER ddl_drop_errlog_table;
DROP EVENT TRIGGER ddl_drop_errlog_table;
ALTER EXTENSION pg_dbms_errlog DROP FUNCTION @extschema@.unregister_errlog_table();
DROP FUNCTION @extschema@.unregister_errlog_table();
