LOAD 'pg_dbms_errlog';
SET pg_dbms_errlog.synchronous = query;

-- Create the error log table for relation t1 in a dedicated schema
CREATE SCHEMA testerrlog;
CALL dbms_errlog.create_error_log('public."t1"', 'testerrlog."errTable"');
SELECT count(*) FROM dbms_errlog.register_errlog_tables ;

-- Set error log behavior for this DML batch
SET pg_dbms_errlog.query_tag TO 'daily_load';
SET pg_dbms_errlog.reject_limit TO 25;
SET pg_dbms_errlog.enabled TO true;

-- Start a transaction
BEGIN;
-- Insert successful
INSERT INTO t1 VALUES (3);
-- DELETE will fail
SAVEPOINT aze;
DELETE FROM t1 WHERE a = '10.6';
ROLLBACK TO aze;
SELECT * FROM testerrlog."errTable"
ORDER BY "pg_err_number$" COLLATE "C", "pg_err_mesg$" COLLATE "C";
-- Update will fail but at parser level, it must not be logged
SAVEPOINT aze;
UPDATE t1 SET a = '10.7'::varchar interval WHERE a = 1;
ROLLBACK TO aze;
-- Test prepared statement
PREPARE prep_delete (bigint) AS DELETE FROM t1 WHERE a IN ($1);
-- Delete will fail
SAVEPOINT aze;
EXECUTE prep_delete ('10.8');
ROLLBACK TO aze;
DEALLOCATE prep_delete;
PREPARE prep_update (bigint) AS UPDATE t1 SET a = $1 WHERE a = 2;
-- Update will fail
SAVEPOINT aze;
EXECUTE prep_update('10.9');
ROLLBACK TO aze;
DEALLOCATE prep_update;
ROLLBACK;

-- Looking at error logging table
\x
SELECT * FROM testerrlog."errTable"
ORDER BY "pg_err_number$" COLLATE "C", "pg_err_mesg$" COLLATE "C";
\x

-- Dropping one of the table
BEGIN;
DROP TABLE testerrlog."errTable";
SELECT count(*) FROM dbms_errlog.register_errlog_tables ;
ROLLBACK;
SELECT count(*) FROM dbms_errlog.register_errlog_tables ;
BEGIN;
DROP TABLE t1;
SELECT count(*) FROM dbms_errlog.register_errlog_tables ;
ROLLBACK;
SELECT count(*) FROM dbms_errlog.register_errlog_tables ;
DROP TABLE testerrlog."errTable";
