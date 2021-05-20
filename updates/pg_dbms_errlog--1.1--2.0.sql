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

CREATE OR REPLACE FUNCTION @extschema@.publish_queue(
    wait_for_completion bool DEFAULT false
) RETURNS bool
LANGUAGE C COST 1000
AS '$libdir/pg_dbms_errlog', 'pg_dbms_errlog_publish_queue';
REVOKE ALL ON FUNCTION @extschema@.publish_queue FROM public;

CREATE OR REPLACE FUNCTION @extschema@.queue_size(
    OUT num_errors integer
) RETURNS integer
LANGUAGE C COST 1000
AS '$libdir/pg_dbms_errlog', 'pg_dbms_errlog_queue_size';
GRANT ALL ON FUNCTION @extschema@.queue_size TO public;
