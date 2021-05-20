SET pg_dbms_errlog.synchronous = query;

CREATE TABLE measurement (
    city_id         int not null,
    logdate         date not null,
    peaktemp        int,
    unitsales       int
) PARTITION BY RANGE (logdate);

CREATE TABLE measurement_y2006m02 PARTITION OF measurement
    FOR VALUES FROM ('2006-02-01') TO ('2006-03-01');

CREATE TABLE measurement_y2006m03 PARTITION OF measurement
    FOR VALUES FROM ('2006-03-01') TO ('2006-04-01');

-- Create the error log table for relation t1 in a dedicated schema
CALL dbms_errlog.create_error_log('public.measurement');
SELECT count(*) FROM dbms_errlog.register_errlog_tables ;

-- Set error log behavior for this DML batch
SET pg_dbms_errlog.query_tag TO 'daily_load';
SET pg_dbms_errlog.reject_limit TO 25;
SET pg_dbms_errlog.enabled TO true;

-- Start a transaction
BEGIN;
-- Insert will fail inside trigger
SAVEPOINT aze;
INSERT INTO measurement VALUES (1, '2006-04-01', 0, 2);
ROLLBACK TO aze;
ROLLBACK;

-- Looking at error logging table
\x
SELECT * FROM "ERR$_measurement"
ORDER BY "pg_err_number$" COLLATE "C", "pg_err_mesg$" COLLATE "C";
\x

-- Dropping one of the table
BEGIN;
DROP TABLE "ERR$_measurement";
SELECT count(*) FROM dbms_errlog.register_errlog_tables ;
ROLLBACK;
SELECT count(*) FROM dbms_errlog.register_errlog_tables ;
BEGIN;
DROP TABLE measurement;
SELECT count(*) FROM dbms_errlog.register_errlog_tables ;
ROLLBACK;
SELECT count(*) FROM dbms_errlog.register_errlog_tables ;
DROP TABLE "ERR$_measurement";
DROP TABLE measurement;

