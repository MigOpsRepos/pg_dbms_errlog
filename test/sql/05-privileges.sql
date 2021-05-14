LOAD 'pg_dbms_errlog';
SET pg_dbms_errlog.synchronous = on;

CREATE TABLE t2 (
    id int NOT NULL
);
GRANT ALL ON t2 TO pel_u1;

-- Create the error log table for relation t2
CALL dbms_errlog.create_error_log('t2');
SELECT count(*) FROM dbms_errlog.register_errlog_tables ;

-- connect as basic user to test privileges
SET SESSION AUTHORIZATION 'pel_u1';

-- Set error log behavior for this DML batch
SET pg_dbms_errlog.query_tag TO 'daily_load';
SET pg_dbms_errlog.reject_limit TO 25;
SET pg_dbms_errlog.enabled TO true;

-- Start a transaction
BEGIN;
-- Insert will fail
SAVEPOINT aze;
INSERT INTO t2 VALUES (NULL);
ROLLBACK TO aze;
COMMIT;

-- Not allowed
SELECT * FROM "ERR$_t2"
ORDER BY "pg_err_number$" COLLATE "C", "pg_err_mesg$" COLLATE "C";

-- Back to origin connection
SET SESSION AUTHORIZATION DEFAULT;

-- Looking at error logging table
\x
SELECT * FROM "ERR$_t2"
ORDER BY "pg_err_number$" COLLATE "C", "pg_err_mesg$" COLLATE "C";
\x

-- cleanup
DROP TABLE t2;
DROP TABLE "ERR$_t2";
SELECT count(*) FROM dbms_errlog.register_errlog_tables ;
DROP ROLE pel_u1;
