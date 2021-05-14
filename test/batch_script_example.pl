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
