LOAD 'pg_dbms_errlog';
SET pg_dbms_errlog.synchronous = on;

-- Create the error log table for relation t1
CALL dbms_errlog.create_error_log('t1');
SELECT count(*) FROM dbms_errlog.register_errlog_tables ;

-- Set error log behavior for this DML batch
SET pg_dbms_errlog.query_tag TO 'daily_load';
SET pg_dbms_errlog.reject_limit TO 25;
SET pg_dbms_errlog.enabled TO true;

-- Start a transaction
BEGIN;
-- Insert will fail
SAVEPOINT aze;
INSERT INTO t1 VALUES ('10.4');
ROLLBACK TO aze;
SELECT * FROM "ERR$_t1"
ORDER BY "pg_err_number$" COLLATE "C", "pg_err_mesg$" COLLATE "C";
-- Insert successful
INSERT INTO t1 VALUES (1);
-- Insert will fail on duplicate key
SAVEPOINT aze;
INSERT INTO t1 VALUES (1);
ROLLBACK TO aze;
SELECT * FROM "ERR$_t1"
ORDER BY "pg_err_number$" COLLATE "C", "pg_err_mesg$" COLLATE "C";
PREPARE prep_insert AS INSERT INTO t1 VALUES ($1);
-- Insert successful
SAVEPOINT aze;
EXECUTE prep_insert(2);
ROLLBACK TO aze;
-- Insert will fail
SAVEPOINT aze;
EXECUTE prep_insert('10.5');
ROLLBACK TO aze;
DEALLOCATE prep_insert;
ROLLBACK;

-- Looking at error logging table
\x
SELECT * FROM "ERR$_t1"
ORDER BY "pg_err_number$" COLLATE "C", "pg_err_mesg$" COLLATE "C";
\x

-- Dropping one of the table
BEGIN;
DROP TABLE "ERR$_t1";
SELECT count(*) FROM dbms_errlog.register_errlog_tables ;
ROLLBACK;
SELECT count(*) FROM dbms_errlog.register_errlog_tables ;
BEGIN;
DROP TABLE t1;
SELECT count(*) FROM dbms_errlog.register_errlog_tables ;
ROLLBACK;
SELECT count(*) FROM dbms_errlog.register_errlog_tables ;
DROP TABLE "ERR$_t1";
