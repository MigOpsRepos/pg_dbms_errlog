SET pg_dbms_errlog.synchronous = query;

-- Create the error log table for relation t1 in a dedicated schema
CALL dbms_errlog.create_error_log('public."t1"', 'testerrlog."errTable"');
SELECT count(*) FROM dbms_errlog.register_errlog_tables ;

CREATE TABLE t2 (id varchar);

-- Create the trigger that will generate an error, must be logged
CREATE FUNCTION trig_fct() RETURNS TRIGGER AS
$$
BEGIN
	INSERT INTO t2 VALUES (NEW.id);
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER t1_insert BEFORE INSERT ON t1 FOR EACH ROW EXECUTE PROCEDURE trig_fct();

-- Set error log behavior for this DML batch
SET pg_dbms_errlog.query_tag TO 'daily_load';
SET pg_dbms_errlog.reject_limit TO 25;
SET pg_dbms_errlog.enabled TO true;

-- Start a transaction
BEGIN;
-- Insert will fail inside trigger
SAVEPOINT aze;
INSERT INTO t1 VALUES (7);
ROLLBACK TO aze;
DROP TRIGGER t1_insert ON t1;
-- Create a function that execute a failing insert, must not ne logged
CREATE FUNCTION insert_fct() RETURNS integer AS
$$
BEGIN
	INSERT INTO t1 VALUES (1234.45);
	RETURN 1;
END;
$$ LANGUAGE plpgsql;
SAVEPOINT aze;
SELECT insert_fct();
ROLLBACK TO aze;
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

DROP TRIGGER t1_insert ON t1;
DROP FUNCTION trig_fct;
DROP TABLE t2;
DROP TABLE t1;
