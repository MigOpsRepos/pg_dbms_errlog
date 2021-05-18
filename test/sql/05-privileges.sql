------------------------------------------------------------------
-- Test privilege on errorlog table. A user need to be granted the
-- DML privilege to the table and to the error log table to be able
-- to use this feature. Insert to the registration table is done
-- internally by superuser, to allow a user to create an error logging
-- logging table he must be granted to execute the create_error_log()
-- function and have read/write access to the registration table
-- dbms_errlog.register_errlog_tables.
----------------------------------------------------------------

-- Set error log behavior for this DML batch
SET pg_dbms_errlog.synchronous = on;
SET pg_dbms_errlog.query_tag TO 'daily_load1';
SET pg_dbms_errlog.reject_limit TO 25;
SET pg_dbms_errlog.enabled TO true;

DROP ROLE IF EXISTS pel_u1;
CREATE ROLE pel_u1 LOGIN;

CREATE TABLE t2 (
    id int NOT NULL
);
GRANT ALL ON t2 TO pel_u1;

-- Create the error log table for relation t2
CALL dbms_errlog.create_error_log('t2');
-- Verify that it have been registered
SELECT count(*) FROM dbms_errlog.register_errlog_tables ;

-- Start a transaction
BEGIN;
SAVEPOINT aze;
-- Insert will fail with insuffisient privilege and be registered to ERR$_t2
INSERT INTO t2 VALUES (NULL);
ROLLBACK TO aze;
COMMIT;

-- Show content of the error log table with test user.
\x
SELECT * FROM "ERR$_t2"
ORDER BY "pg_err_number$" COLLATE "C", "pg_err_mesg$" COLLATE "C";
\x

-- Cleanup
DELETE FROM "ERR$_t2";

-- connect as basic user to test privileges
SET SESSION AUTHORIZATION 'pel_u1';

-- Set error log behavior for this DML batch
SET pg_dbms_errlog.synchronous = on;
SET pg_dbms_errlog.query_tag TO 'daily_load2';
SET pg_dbms_errlog.reject_limit TO 25;
SET pg_dbms_errlog.enabled TO true;

-- Start a transaction
BEGIN;
SAVEPOINT aze;
-- Insert will fail with insuffisient privilege
-- and nothing is registered on ERR$_t2, not granted
INSERT INTO t2 VALUES (NULL);
ROLLBACK TO aze;
COMMIT;

-- Not enough privilege
SELECT * FROM "ERR$_t2"
ORDER BY "pg_err_number$" COLLATE "C", "pg_err_mesg$" COLLATE "C";

-- Back to origin connection
SET SESSION AUTHORIZATION DEFAULT;

-- Allow user pel_u1 to write to ERR$_t2
GRANT ALL ON "ERR$_t2" TO pel_u1;

-- switch back to test privilege user
SET SESSION AUTHORIZATION 'pel_u1';

SET pg_dbms_errlog.synchronous = on;
SET pg_dbms_errlog.query_tag TO 'daily_load3';
SET pg_dbms_errlog.reject_limit TO 25;
SET pg_dbms_errlog.enabled TO true;

-- Show content of the error log table with test user. 0
\x
SELECT * FROM "ERR$_t2"
ORDER BY "pg_err_number$" COLLATE "C", "pg_err_mesg$" COLLATE "C";
\x

-- Start a transaction
BEGIN;
SAVEPOINT aze;
-- Insert will fail and the error will be registered this time
INSERT INTO t2 VALUES (NULL);
ROLLBACK TO aze;
COMMIT;

-- Show content of the error log table with test user. 1
\x
SELECT * FROM "ERR$_t2"
ORDER BY "pg_err_number$" COLLATE "C", "pg_err_mesg$" COLLATE "C";
\x

-- Dropping table is not allowed, it must not be unregistered
DROP TABLE t2;
SELECT count(*) FROM dbms_errlog.register_errlog_tables ; -- returns 1

-- Back to original connection
SET SESSION AUTHORIZATION DEFAULT;

-- cleanup
DROP TABLE t2;
DROP TABLE "ERR$_t2";
SELECT count(*) FROM dbms_errlog.register_errlog_tables ;

----
-- Try error logging creation by non superuser role
----
CREATE TABLE t2 (
    id int NOT NULL
);
GRANT ALL ON t2 TO pel_u1;
GRANT ALL ON dbms_errlog.register_errlog_tables TO pel_u1;

SET SESSION AUTHORIZATION 'pel_u1';

SET pg_dbms_errlog.synchronous = on;
SET pg_dbms_errlog.query_tag TO 'daily_load4';
SET pg_dbms_errlog.reject_limit TO 25;
SET pg_dbms_errlog.enabled TO true;

-- Create the error log table for relation t2 as non superuser role
CALL dbms_errlog.create_error_log('t2');
-- Verify that it have been registered
SELECT count(*) FROM dbms_errlog.register_errlog_tables ;

-- Start a transaction
BEGIN;
SAVEPOINT aze;
-- Insert will fail for NULL value and will be registered in ERR$_t2
INSERT INTO t2 VALUES (NULL);
ROLLBACK TO aze;
COMMIT;

-- Show content of the error log table with test user.
\x
SELECT * FROM "ERR$_t2"
ORDER BY "pg_err_number$" COLLATE "C", "pg_err_mesg$" COLLATE "C";
\x

-- cleanup
DROP TABLE t2; -- will fail
DROP TABLE "ERR$_t2"; -- will be dropped
SELECT count(*) FROM dbms_errlog.register_errlog_tables ;

SET SESSION AUTHORIZATION DEFAULT;

DROP TABLE t2;
SELECT count(*) FROM dbms_errlog.register_errlog_tables ;
REVOKE ALL ON dbms_errlog.register_errlog_tables FROM pel_u1;
DROP ROLE pel_u1;
