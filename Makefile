EXTENSION  = pg_dbms_errlog
EXTVERSION = $(shell grep default_version $(EXTENSION).control | \
               sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")

PGFILEDESC = "pg_dbms_errlog - Emulate Oracle DBMS_ERRLOG for PostgreSQL"

PG_CONFIG = pg_config

PG_CPPFLAGS = -Wno-uninitialized
PG_LIBDIR := $(shell $(PG_CONFIG) --libdir)

DOCS = $(wildcard README*)
MODULE_big = pg_dbms_errlog

OBJS = pg_dbms_errlog.o pel_errqueue.o pel_worker.o

DATA = $(wildcard updates/*--*.sql) sql/$(EXTENSION)--$(EXTVERSION).sql

TESTS        =  00-init  \
		01-basic  \
		02-upt-del \
		03-trig-fct \
		04-partition

REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

