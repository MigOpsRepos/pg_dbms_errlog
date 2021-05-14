/*
 * Must be executed before all regression test.
 */

-- Create the pg_background PostgreSQL extension, pg_statement_rollback is
-- also required but it is imported using LOAD in the other test files.
CREATE EXTENSION pg_background;

-- Create the PostgreSQL extension
CREATE EXTENSION pg_dbms_errlog;

-- Create the test table
CREATE TABLE IF NOT EXISTS t1 (a bigint PRIMARY KEY);

-- Create a basic user to test privileges
DROP ROLE IF EXISTS pel_u1;
CREATE ROLE pel_u1 LOGIN;
