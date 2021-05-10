## pg_dbms_errlog - DML error logging

* [Description](#description)
* [Installation](#installation)
* [Configuration](#configuration)
* [Use of the extension](#use-of-the-extension)
* [Limitations](#limitation)
* [Which errors are logged](#which-errors-are-logged)?
* [Using a table with error logging](#using-a-table-with-error-logging)
* [Error logging table](#error-logging-table)
* [Batch script example](#batch-script-example)
* [Authors](#authors)
* [License](#license)

### [Description](#description)

The pg_dbms_errlog extension provides the infrastructure that enables you to
create an error logging table so that DML operations can continue after
encountering errors rather than abort and roll back. It requires the use of
the pg_statement_rollback extension or to fully manage the SAVEPOINT in the
DML script. Logging in the corresponding error table is done using extension
pg_background, it is required that the extension must have been created in the
database. Note that `max_worker_processes` must be high enough to support the
pg_dbms_errlog extension, a pg_background process can be forked in each session
using the pg_dbms_errlog extension.

* [Installation](#installation)

To install the pg_dbms_errlog extension you need at least a PostgreSQL
version 10. Untar the pg_dbms_errlog tarball anywhere you want then you'll
need to compile it with PGXS.  The `pg_config` tool must be in your path.

Depending on your installation, you may need to install some devel package.
Once `pg_config` is in your path, do

    make
    sudo make install

To run test execute the following command as superuser:

    make installcheck

### [Configuration](#configuration)

- *pg_dbms_errlog.enabled*

Enable/disable log of failing queries. Default disabled.

- *pg_dbms_errlog.query_tag*

Tag (a numeric or string literal in parentheses) that gets added to the error
log to help identify the statement that caused the errors. If the tag is
omitted, a NULL value is used.

- *pg_dbms_errlog.reject_limit*

Maximum number of errors that can be encountered before the DML statement
terminates and rolls back. A value of -1 mean unlimited. The default reject
limit is zero, which means that upon encountering the first error, the error
is logged and the statement rolls back. FIXME: not supported yet.

- *pg_dbms_errlog.synchronous*

Wait for pg_background error logging completion when an error happens.
Default enabled. Asynchronous logging is discouraged as you can loose some
error messages when the script ends unless you wait enough time at end of
the script to wait for all asynchronous insert to be effective.

- *pg_dbms_errlog.no_client_error*

Enable/disable client error logging. Enable by default, the error messages
logged will not be sent to the client but still looged on server side. This
correspond to the Oracle behavior.


### [Use of the extension](#use-of-the-extension)

Enabling error logging for a table is done through the call of procedure
`dbms_errlog.create_error_log()`. This procedure creates the error
logging table that need to use the DML error logging capability.


*dbms_errlog.create_error_log (dml_table_name varchar(132), err_log_table_name varchar(132) DEFAULT NULL, err_log_table_owner name DEFAULT NULL, err_log_table_space name DEFAULT NULL)*

- *dml_table_name varchar(128)*

Name of the DML table to base the error logging table on, can be fqdn.

- *err_log_table_name name*

Name of the error logging table to create. Can be NULL, default is to take
the first 58 characters of the DML table name and to prefixed it with string
`ERR$_`. The table is created in the current working schema it can be changed
using `search_path` or using fqdn table name.

- *err_log_table_owner name*

Name of the owner of the error logging table. Can be NULL, default to
current user.

- *err_log_table_space name*

Name of the tablespace where the error logging table will be created in. Can
be NULL to not use any specific tablespace.

This correspond to the [Oracle CREATE_ERROR_LOG Procedure](https://docs.oracle.com/database/121/ARPLS/d_errlog.htm#ARPLS66322) minus the use of
parameter `skip_unsupported` because we don't have such limitation with the
current design of the extension. The pg_dbms_errlog extension doesn't copy
the DML data into a dedicated column but it logs the whole query and error
details in two text columns.

- Example 1:
```
CREATE EXTENSION pg_dbms_errlog;
LOAD 'pg_dbms_errlog';
BEGIN;
CALL dbms_errlog.create_error_log('employees');
END;
```
will create the logging table;
```
gilles=# \d public."ERR$_employees" 
                  Table "public.ERR$_employees"
     Column     |     Type     | Collation | Nullable | Default 
----------------+--------------+-----------+----------+---------
 pg_err_number$ | text         |           |          | 
 pg_err_mesg$   | text         |           |          | 
 pg_err_optyp$  | character(1) |           |          | 
 pg_err_tag$    | text         |           |          | 
 pg_err_query$  | text         |           |          | 
 pg_err_detail$ | text         |           |          | 
```
- Example 2:
```
CREATE EXTENSION pg_dbms_errlog;
LOAD 'pg_dbms_errlog';
BEGIN;
CALL dbms_errlog.create_error_log('hr.employees','"ERRORS"."ERR$_EMPTABLE");
END;
```
will create the logging table;
```
gilles=# \d "ERRORS"."ERR$_EMPTABLE"
                    Table « "ERRORS"."ERR$_EMPTABLE" »
     Column     |     Type     | Collation | Nullable | Default 
----------------+--------------+-----------------+-----------+------------
 pg_err_number$ | text         |                 |           |
 pg_err_mesg$   | text         |                 |           |
 pg_err_optyp$  | character(1) |                 |           |
 pg_err_tag$    | text         |                 |           |
 pg_err_query$  | text         |                 |           |
 pg_err_detail$ | text         |                 |           |
```

### [Limitations](#limitation)

As explain above the pg_dbms_errlog extension copy the failing DML query into
a text column and error details in an other text column, that means that the
statement logged must have a length below 1GB.

Expect changes on this part in further version, having a dedicated log column
per column's data to give the exact same behavior as Oracle implementation.

The form `INSERT INTO <tablename> SELECT ...` will not have the same behavior
than in Oracle. It will not stored the succesfull insert and looged the rows
in error. This is not supported because it is a single transaction for PostgreSQL
and everything is rollbacked in case of error. But the erro


### [Which errors are logged](#which-errors-are-logged)?

For the pg_dbms_errlog extension all errors are logged except errors at parse
level, aka syntax error.

About Oracle RDMBS_ERRLOG module DML operations logged see
[Error Logging Restrictions and Caveats](https://docs.oracle.com/database/121/ADMIN/tables.htm#GUID-4E5F45D4-DA96-48AE-A3DD-7AC5C1C11076) for more details.


### [Using a table with error logging](#using-a-table-with-error-logging)

The following statements create a raises table in the sample schema `HR`,
create an error logging table using the pg_dbms_errlog extension, and
populate the raises table with data from the employees table. One of the
inserts violates the check constraint on raises, and that row can be seen
in associated error log table.

FIXME: If more than ten errors had occurred, then the statement would have
aborted, rolling back any insertions made.

```
CREATE EXTENSION pg_background;
CREATE EXTENSION pg_dbms_errlog;
LOAD 'pg_dbms_errlog';
LOAD 'pg_statement_rollback';

CREATE SCHEMA "HR";
CREATE TABLE "HR".raises ( emp_id integer, sal integer CHECK(sal > 8000) );

BEGIN;
CALL dbms_errlog.create_error_log('"HR".raises');
END;

SET pg_dbms_errlog.query_tag TO 'daily_load';
SET pg_dbms_errlog.reject_limit TO 10;
SET pg_dbms_errlog.enabled TO true;

BEGIN;
SET pg_statement_rollback.enabled TO on;
INSERT INTO "HR".raises VALUES (145, 15400); -- Success
INSERT INTO "HR".raises VALUES (161, 7700); -- Failure
-- Back to the automatic savepoint generated by the extension
-- pg_statement_rollback necessary to not break the transaction
ROLLBACK TO SAVEPOINT "PgSLRAutoSvpt";
INSERT INTO "HR".raises VALUES (175, 9680); -- Success
COMMIT;
```

This code have inserted 2 rows into the raise table and one log entry into
the error log table:
```
gilles=# SELECT * FROM "HR".raises;
 emp_id |  sal  
--------+-------
    145 | 15400
    175 |  9680
(2 rows)

gilles=# \x
Expanded display is on.
gilles=# SELECT * FROM "ERR$_raises";
-[ RECORD 1 ]--+------------------------------------------------------------------------------------------
pg_err_number$ | 23514
pg_err_mesg$   | new row for relation "raises" violates check constraint "raises_sal_check"
pg_err_optyp$  | I
pg_err_tag$    | daily_load
pg_err_query$  | INSERT INTO "HR".raises VALUES (161, 7700);
pg_err_detail$ | ERROR:  23514: new row for relation "raises" violates check constraint "raises_sal_check"+
               | DETAIL:  Failing row contains (161, 7700).                                               +
               | STATEMENT:  INSERT INTO "HR".raises VALUES (161, 7700);                                  +
               | 
```

The following settings are equivalent to a Oracle DML clause
`LOG ERRORS INTO errlog ('query tag') REJECT LIMIT 10`.

```
SET pg_dbms_errlog.enabled TO on;
SET pg_dbms_errlog.query_tag TO 'daily_log';
SET pg_dbms_errlog.reject_limit TO 10;
```

The destination error log table (here `errlog`) is found automatically by the
extension regarding the table where the DML is acting.

A more complex DML script to demonstrate the use based on the example above:
```
LOAD 'pg_dbms_errlog';
LOAD 'pg_statement_rollback';

SET pg_dbms_errlog.query_tag TO 'batch_load';
SET pg_dbms_errlog.reject_limit TO 0;
SET pg_dbms_errlog.enabled TO true;

BEGIN;
DELETE FROM "HR".raises;
DELETE FROM "ERR$_raises";
SET pg_statement_rollback.enabled TO on;
DO
$$
DECLARE
    emp RECORD;
BEGIN
    FOR emp IN SELECT employee_id, salary FROM employees WHERE commission_pct > .2
    LOOP
	BEGIN
		INSERT INTO "HR".raises VALUES (emp.employee_id, emp.salary);
	EXCEPTION WHEN OTHERS THEN
	    ROLLBACK TO "PgSLRAutoSvpt";
	END;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
COMMIT;

```

### [Error logging table](#error-logging-table)

The table created to log DML errors executed on a table has the following
structure:
```
CREATE TABLE ERR$_DML_TABLE (
	PG_ERR_NUMBER$ integer, -- PostgreSQL error number
	PG_ERR_MESG$ text,   -- PostgreSQL error message
	PG_ERR_OPTYP$ char(1), -- Type of operation: insert (I), update (U), delete (D)
	PG_ERR_TAG$ text, -- Label used to identify the DML batch
	PG_ERR_QUERY$ text -- Query at origin (insert,delete, update or the prepared DML statement)
	PG_ERR_DETAIL$ text -- Errors details
);
```

### [Batch script example](#batch-script-example)

Test file `test/batch_script_example.pl` is a Perl script to demonstrate the
use of the extension in a batch script. This script will try to insert 10 rows
in table `t1` where the half will generate an error and will be logged to the
corresponding error table `ERR$_t1`.

```
#!/usr/bin/perl

use DBI;

print "Creating the regression database.\n";
my $dbh = DBI->connect("dbi:Pg:dbname=template1", '', '', {AutoCommit => 1});
die "ERROR: can't connect to database template1\n" if (not defined $dbh);
$dbh->do("DROP DATABASE contrib_regression");
$dbh->do("CREATE DATABASE contrib_regression");
$dbh->do("ALTER DATABASE contrib_regression SET lc_messages = 'C'");
$dbh->disconnect;

print "Connect to the regression database.\n";
$dbh = DBI->connect("dbi:Pg:dbname=contrib_regression", '', '', {AutoCommit => 1, PrintError => 0});
die "ERROR: can't connect to database ontrib_regression\n" if (not defined $dbh);
print "---------------------------------------------\n";
print "Create the extension and initialize the test\n";
print "---------------------------------------------\n";
$dbh->do("CREATE EXTENSION pg_background");
$dbh->do("CREATE EXTENSION pg_dbms_errlog");
$dbh->do("LOAD 'pg_dbms_errlog'");
$dbh->do("SET pg_dbms_errlog.synchronous = on;");
$dbh->do("CREATE TABLE t1 (a bigint PRIMARY KEY, lbl text)");
$dbh->do("CALL dbms_errlog.create_error_log('t1');");
$dbh->do("SET pg_dbms_errlog.query_tag TO 'daily_load'");
$dbh->do("SET pg_dbms_errlog.reject_limit TO 25");
$dbh->do("SET pg_dbms_errlog.enabled TO true");
$dbh->do("BEGIN;");
print "---------------------------------------------\n";
print "Start DML work\n";
print "---------------------------------------------\n";
for (my $i = 0; $i <= 10; $i++)
{
	$dbh->do("SAVEPOINT aze");
	my $sth = $dbh->prepare("INSERT INTO t1 VALUES (?, ?)");
	if (not defined $sth) {
		#print STDERR "PREPARE ERROR: " . $dbh->errstr . "\n";
		next;
	}
	# Generate a duplicate key each two row inserted
	my $val = $i;
	$val = $i-1 if ($i % 2 != 0);
	unless ($sth->execute($val, 'insert '.$i)) {
		#print STDERR "EXECUTE ERROR: " . $dbh->errstr . "\n";
		$dbh->do("ROLLBACK TO aze");
	}
}

print "---------------------------------------------\n";
print "Look at inserted values in DML table\n";
print "---------------------------------------------\n";
my $sth = $dbh->prepare("SELECT * FROM t1");
$sth->execute();
while (my $row = $sth->fetch) {
	print "INSERTED ID: $row->[0]\n";
}
print "---------------------------------------------\n";
print "Look at failing insert in error logging table\n";
print "---------------------------------------------\n";
$sth = $dbh->prepare('SELECT * FROM "ERR$_t1"');
$sth->execute();
while (my $row = $sth->fetch) {
	print "ERROR: LOGGED: ", join(' | ', @$row), "\n";
}
$dbh->do("COMMIT;");

$dbh->disconnect;

exit 0;
```

### [Authors](#authors)

- Julien Rouhaud
- Gilles Darold


### [License](#license)

This extension is free software distributed under the PostgreSQL
License.

    Copyright (c) 2021 MigOps Inc.

