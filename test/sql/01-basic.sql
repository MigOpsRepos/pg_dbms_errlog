SET pg_dbms_errlog.synchronous = query;

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

TRUNCATE "ERR$_t1";

-- test queue discard on rollback
SET pg_dbms_errlog.synchronous = off;
BEGIN;
SAVEPOINT aze;
INSERT INTO t1 VALUES ('queue 1');
ROLLBACK TO aze;
-- commit should discard the queue
ROLLBACK;
SELECT dbms_errlog.publish_queue(true);
SELECT 0 AS exp, count(*) FROM "ERR$_t1";

BEGIN;
INSERT INTO t1 VALUES ('queue 2');
-- unsuccessful commit should discard the queue
COMMIT;
SELECT dbms_errlog.publish_queue(true);
SELECT 0 AS exp, count(*) FROM "ERR$_t1";

BEGIN;
SAVEPOINT aze;
INSERT INTO t1 VALUES ('queue 3');
ROLLBACK TO aze;
SET pg_dbms_errlog.synchronous = query;
-- commit should publish the queue even in query level sync
COMMIT;
SELECT 1 AS exp, count(*) FROM "ERR$_t1";

SET pg_dbms_errlog.synchronous = off;
BEGIN;
SAVEPOINT aze;
INSERT INTO t1 VALUES ('queue 4');
ROLLBACK TO aze;
-- should publish the queue and wait for the result
SELECT dbms_errlog.publish_queue(true);
SELECT 2 AS exp, count(*) FROM "ERR$_t1";
-- error should still be visible in the error table
ROLLBACK;
SELECT 2 AS exp, count(*) FROM "ERR$_t1";

TRUNCATE "ERR$_t1";

-- test queuing multiple batches
SET pg_dbms_errlog.synchronous = off;
SET pg_dbms_errlog.frequency TO '1h';

BEGIN;
SAVEPOINT aze;
INSERT INTO t1 VALUES ('queue 5');
ROLLBACK TO aze;
COMMIT;
SELECT 0 AS exp, count(*) FROM "ERR$_t1";

BEGIN;
SAVEPOINT aze;
INSERT INTO t1 VALUES ('queue 6');
ROLLBACK TO aze;
SET pg_dbms_errlog.synchronous = query;
COMMIT;
SELECT 2 AS exp, count(*) FROM "ERR$_t1";

TRUNCATE "ERR$_t1";

-- test waiting for already published batches
SET pg_dbms_errlog.synchronous = off;

BEGIN;
SAVEPOINT aze;
INSERT INTO t1 VALUES ('queue 7');
ROLLBACK TO aze;
COMMIT;

BEGIN;
SAVEPOINT aze;
INSERT INTO t1 VALUES ('queue 8');
ROLLBACK TO aze;
COMMIT;

SELECT 0 AS exp, count(*) FROM "ERR$_t1";
SELECT dbms_errlog.publish_queue(true);
SELECT 2 AS exp, count(*) FROM "ERR$_t1";

TRUNCATE "ERR$_t1";

-- test transaction level sync
SET pg_dbms_errlog.synchronous = transaction;
BEGIN;
SAVEPOINT aze;
INSERT INTO t1 VALUES ('queue 9');
ROLLBACK TO aze;
-- the error should not have been published or processed
SELECT 0 AS exp, count(*) FROM "ERR$_t1";
-- commit should publish the error and wait for the result
COMMIT;
SELECT 1 AS exp, count(*) FROM "ERR$_t1";

RESET pg_dbms_errlog.frequency;

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
