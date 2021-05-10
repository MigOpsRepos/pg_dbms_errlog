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

CREATE EVENT TRIGGER ddl_drop_errlog_table ON sql_drop EXECUTE PROCEDURE @extschema@.unregister_errlog_table();
