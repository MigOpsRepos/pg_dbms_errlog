/*-------------------------------------------------------------------------
 *
 * pg_dbms_errlog.c
 *	pg_dbms_errlog is a PostgreSQL extension that logs each failing DML query.
 *	It emulates the DBMS_ERRLOG Oracle module.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#if PG_VERSION_NUM >= 130000
#include "access/table.h"
#else
/* for imported functions */
#include "access/xact.h"
#include "mb/pg_wchar.h"
#endif
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "parser/analyze.h"
#include "parser/parser.h"
#include "storage/ipc.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/varlena.h"

#if PG_VERSION_NUM < 100000
#error Minimum version of PostgreSQL required is 10
#endif

#if PG_VERSION_NUM < 120000
#define table_openrv(r,l)      heap_openrv(r,l)
#define table_close(r,l)       heap_close(r,l)
#endif

/* Define ProcessUtility hook proto/parameters following the PostgreSQL version */
#if PG_VERSION_NUM >= 130000
#define PEL_PROCESSUTILITY_PROTO PlannedStmt *pstmt, const char *queryString, \
					ProcessUtilityContext context, ParamListInfo params, \
					QueryEnvironment *queryEnv, DestReceiver *dest, \
					QueryCompletion *qc
#define PEL_PROCESSUTILITY_ARGS pstmt, queryString, context, params, queryEnv, dest, qc
#else
#define PEL_PROCESSUTILITY_PROTO PlannedStmt *pstmt, const char *queryString, \
					ProcessUtilityContext context, ParamListInfo params, \
					QueryEnvironment *queryEnv, DestReceiver *dest, \
					char *completionTag
#define PEL_PROCESSUTILITY_ARGS pstmt, queryString, context, params, queryEnv, dest, completionTag
#endif

#define PEL_NAMESPACE_NAME "dbms_errlog"
#define PEL_REGISTRATION_TABLE "register_errlog_tables"
#define Anum_pel_relid 1
#define Anum_pel_errlogid 2

#define MAX_PREPARED_STMT_SIZE   1048576

#define PEL_ASYNC_QUERY "SELECT pg_background_launch($$INSERT INTO %s VALUES ('%s', %s, '%c', %s, %s, %s)$$);"
#define PEL_SYNC_QUERY "SELECT * FROM pg_background_result(pg_background_launch($$INSERT INTO %s VALUES ('%s', %s, '%c', %s, %s, %s)$$)) AS (result TEXT);"


PG_MODULE_MAGIC;

/* Saved hook values in case of unload */
static ProcessUtility_hook_type prev_ProcessUtility = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static emit_log_hook_type prev_emit_log_hook = NULL;
static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;


/* GUC variables */
bool pel_enabled = false;
bool pel_done = false;
int  reject_limit = 0;
char *query_tag = NULL;
bool pel_synchronous = false;
bool pel_no_client_error = true;

/* global variable used to store DML table name */
char *current_dml_table = NULL;
char current_dml_kind = '\0';
char *current_bind_parameters = NULL;

/* Current nesting depth of ExecutorRun calls */
static int	exec_nested_level = 0;

/* cache to store query of prepared stamement */
struct HTAB *PreparedCache = NULL;

/* Functions declaration */
void        _PG_init(void);
void        _PG_fini(void);
static void pel_ProcessUtility(PEL_PROCESSUTILITY_PROTO);
static void pel_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void pel_ExecutorRun(QueryDesc *queryDesc,
							ScanDirection direction,
							uint64 count, bool execute_once);
static void pel_ExecutorFinish(QueryDesc *queryDesc);
static void pel_ExecutorEnd(QueryDesc *queryDesc);
static void pel_log_error(ErrorData *edata);
char *lookupCachedPrepared(const char *preparedName);
void removeCachedPrepared(const char *localPreparedName);
void putCachedPrepared(const char *preparedName, const char *preparedStmt);
void pel_unregister_errlog_table(Oid relid);
char *get_relation_name(Oid relid);
void generate_error_message (ErrorData *edata, StringInfoData *buf);
void append_with_tabs(StringInfo buf, const char *str);
#if PG_VERSION_NUM < 130000
/* Copied from src/backend/nodes/params.c for older versions */
char *BuildParamLogString(ParamListInfo params, char **knownTextValues, int maxlen);
/* Copied from src/backend/utils/mb/stringinfo_mb.c */
void appendStringInfoStringQuoted(StringInfo str, const char *s, int maxlen);
/* Copied from src/backend/access/transam/xact.c */
bool IsAbortedTransactionBlockState(void);
#endif

typedef struct PreparedCacheKey
{
	char preparedName[NAMEDATALEN];
} PreparedCacheKey;

typedef struct PreparedCacheEntry
{
	PreparedCacheKey key;
	char preparedStmt[MAX_PREPARED_STMT_SIZE];
} PreparedCacheEntry;

void
putCachedPrepared(const char *preparedName, const char *preparedStmt)
{
	PreparedCacheKey key = { { 0 } };
	PreparedCacheEntry *entry;
	bool found;

	strncpy(key.preparedName, preparedName, sizeof(PreparedCacheKey));

	entry = hash_search(PreparedCache, &key, HASH_ENTER, &found);
	if (found)
	{
		elog(ERROR, "PREPAREDCACHE: Prepared statement '%s' already exist in cache with query: %s",
				preparedName, entry->preparedStmt);
	}
	else
	{
		elog(DEBUG1, "PREPAREDCACHE: Add prepared statement '%s' with query '%s'",
				preparedName, preparedStmt);
		strncpy(entry->preparedStmt, preparedStmt, sizeof(entry->preparedStmt));
	}
}

void
removeCachedPrepared(const char *preparedName)
{
	PreparedCacheKey key = { { 0 } }; // zero out the key, no trailing garbage
	PreparedCacheEntry *entry;
	bool found;

	strncpy(key.preparedName, preparedName, sizeof(PreparedCacheKey));

	entry = hash_search(PreparedCache, &key, HASH_REMOVE, &found);
	if (found)
		elog(DEBUG1, "PREPAREDCACHE: Prepared statement '%s' exist in cache with query '%s'",
				preparedName, entry->preparedStmt);
	else
		elog(DEBUG1, "PREPAREDCACHE: Prepared statement '%s' to remove not in cache",
				preparedName);
}

char *
lookupCachedPrepared(const char *preparedName)
{
	PreparedCacheKey key = { { 0 } };
	PreparedCacheEntry *entry;
	bool found;

	strncpy(key.preparedName, preparedName, sizeof(PreparedCacheKey));

	entry = hash_search(PreparedCache, &key, HASH_FIND, &found);

	if (!found)
		elog(ERROR, "Prepared statement '%s' is not found in cache", preparedName);

	elog(DEBUG1, "PREPAREDCACHE: prepared statement '%s' found in cache with query: %s",
			key.preparedName, entry->preparedStmt);
	return entry->preparedStmt;
}

/*
 * Module load callback
 */
void
_PG_init(void)
{
	/* Define custom GUC variables */
	DefineCustomBoolVariable( "pg_dbms_errlog.enabled",
				"Enable/disable log of failing queries.",
				NULL,
				&pel_enabled,
				false,
				PGC_USERSET, /* Any user can set it */
				0,
				NULL,
				NULL,
				NULL);

	DefineCustomIntVariable("pg_dbms_errlog.reject_limit",
				"Maximum number of errors that can be encountered before the DML"
			       	" statement terminates and rolls back. A value of -1 mean unlimited."
				" The default reject limit is zero, which means that upon encountering"
				" the first error, the error is logged and the statement rolls back.",
				NULL,
				&reject_limit,
				0,
				-1,
				INT_MAX,
				PGC_USERSET, /* Any user can set it */
				0,
				NULL,
				NULL,
				NULL);

	DefineCustomStringVariable( "pg_dbms_errlog.query_tag",
				"Tag (a numeric or string literal in parentheses) that gets added"
				" to the error log to help identify the statement that caused the"
				" errors. If the tag is omitted, a NULL value is used.",
				NULL,
				&query_tag,
				NULL,
				PGC_USERSET, /* Any user can set it */
				0,
				NULL,
				NULL,
				NULL );

	DefineCustomBoolVariable("pg_dbms_errlog.synchronous",
				"Wait for pg_background error logging completion when an error happens",
				NULL,
				&pel_synchronous,
				false,
				PGC_USERSET,
				0,
				NULL,
				NULL,
				NULL);

	DefineCustomBoolVariable("pg_dbms_errlog.no_client_error",
				"Enable/disable client error logging",
				NULL,
				&pel_no_client_error,
				true,
				PGC_USERSET,
				0,
				NULL,
				NULL,
				NULL);

	EmitWarningsOnPlaceholders("pg_dbms_errlog");

	/* Initialize cache */
	if (PreparedCache == NULL)
	{
		HASHCTL    ctl;
		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(PreparedCacheKey);
		ctl.entrysize = sizeof(PreparedCacheEntry);
		/* allocate PrepareHash in the cache context */
		ctl.hcxt = CacheMemoryContext;
		PreparedCache = hash_create("pg_dbms_errlog_prepares", 8, &ctl,
									 HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);
	}

	/* Install hooks */
	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = pel_ProcessUtility;
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = pel_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = pel_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = pel_ExecutorFinish;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = pel_ExecutorEnd;
	prev_emit_log_hook = emit_log_hook;
	emit_log_hook = pel_log_error;
	prev_post_parse_analyze_hook = post_parse_analyze_hook;

}

/*
 * Module unload callback
 */
void
_PG_fini(void)
{
	/* Uninstall hooks */
	ProcessUtility_hook = prev_ProcessUtility;
	ExecutorStart_hook = prev_ExecutorStart;
	ExecutorRun_hook = prev_ExecutorRun;
	ExecutorFinish_hook = prev_ExecutorFinish;
	ExecutorEnd_hook = prev_ExecutorEnd;
	emit_log_hook = prev_emit_log_hook;
	post_parse_analyze_hook = prev_post_parse_analyze_hook;
}

static void
pel_ProcessUtility(PEL_PROCESSUTILITY_PROTO)
{
	Node *parsetree = pstmt->utilityStmt;

	if (IsA(parsetree, PrepareStmt))
	{
		PrepareStmt *stmt = (PrepareStmt *) parsetree;
		putCachedPrepared(stmt->name, debug_query_string);
	}
	else if (IsA(parsetree, DeallocateStmt))
	{
		DeallocateStmt *stmt = (DeallocateStmt *) parsetree;
		removeCachedPrepared(stmt->name);
	}

	/* Excecute the utility command, we are not concerned */
	if (prev_ProcessUtility)
		prev_ProcessUtility(PEL_PROCESSUTILITY_ARGS);
	else
		standard_ProcessUtility(PEL_PROCESSUTILITY_ARGS);
}

void
pel_unregister_errlog_table(Oid relid)
{
	RangeVar     *rv;
	Relation      rel;
	ScanKeyData   key[1];
	SysScanDesc   scan;
	HeapTuple     tuple;
	bool          found = false;

	elog(DEBUG1, "Looking for registered error logging table with relid = %d", relid);

	/* Set and open the error log registration relation */
	rv = makeRangeVar(PEL_NAMESPACE_NAME, PEL_REGISTRATION_TABLE, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	/* Define scanning */
	ScanKeyInit(&key[0], Anum_pel_relid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));

	/* Start search of relation */
	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	/* Remove the tuples. */
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		elog(DEBUG1, "removing tuple with relid = %d", relid);
		simple_heap_delete(rel, &tuple->t_self);
		found = true;
	}
	/* Cleanup. */
	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	/*
	 * we have not found a registered relation as regular table,
	 * look for an error table now
	 */
	if (!found)
	{
		elog(DEBUG1, "Not found looking if relid %d is registered as an error logging table", relid);

		/* Set and open the pg_dbms_errlog registration relation */
		rv = makeRangeVar(PEL_NAMESPACE_NAME, PEL_REGISTRATION_TABLE, -1);
		rel = table_openrv(rv, RowExclusiveLock);

		/* Define scanning */
		ScanKeyInit(&key[0], Anum_pel_errlogid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));

		/* Start search of relation */
		scan = systable_beginscan(rel, 0, true, NULL, 1, key);
		/* Remove the tuples. */
		while (HeapTupleIsValid(tuple = systable_getnext(scan)))
		{
			elog(DEBUG1, "removing tuple with errlogid = %d", relid);
			simple_heap_delete(rel, &tuple->t_self);
			found = true;
		}
		/* Cleanup. */
		systable_endscan(scan);
		table_close(rel, RowExclusiveLock);
	}
}

static void
pel_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	if (pel_enabled && queryDesc->dest->mydest != DestSPI)
	{
		if (queryDesc->operation == CMD_INSERT)
			current_dml_kind = 'I';
		else if (queryDesc->operation == CMD_UPDATE)
			current_dml_kind = 'U';
		else if (queryDesc->operation == CMD_DELETE)
			current_dml_kind = 'D';
		else
			current_dml_kind = '?';
	}

	if (exec_nested_level == 0)
	{
		if (current_bind_parameters != NULL)
		{
			pfree(current_bind_parameters);
			current_bind_parameters = NULL;
		}
	}

	if (!pel_done)
	{
		if (queryDesc->params && queryDesc->params->numParams > 0)
			current_bind_parameters = BuildParamLogString(queryDesc->params, NULL, -1);
		else
			current_bind_parameters = NULL;
	}

	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
}

/*
 * ExecutorRun hook: all we need do is track nesting depth
 */
static void
pel_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count,
				 bool execute_once)
{
	exec_nested_level++;
	PG_TRY();
	{
		if (prev_ExecutorRun)
			prev_ExecutorRun(queryDesc, direction, count, execute_once);
		else
			standard_ExecutorRun(queryDesc, direction, count, execute_once);
	}
	PG_FINALLY();
	{
		exec_nested_level--;
	}
	PG_END_TRY();
}

/*
 * ExecutorFinish hook: all we need do is track nesting depth
 */
static void
pel_ExecutorFinish(QueryDesc *queryDesc)
{
	exec_nested_level++;
	PG_TRY();
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
	}
	PG_FINALLY();
	{
		exec_nested_level--;
	}
	PG_END_TRY();
}

/*
 * ExecutorEnd hook: done required cleanup
 */
static void
pel_ExecutorEnd(QueryDesc *queryDesc)
{
	if (exec_nested_level == 0)
	{
		if (current_bind_parameters != NULL)
		{
			pfree(current_bind_parameters);
			current_bind_parameters = NULL;
		}
	}

	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

/*
 * Log statement according to the user that launched the statement.
 */
static void
pel_log_error(ErrorData *edata)
{
	/* Only process errors */
	if (!edata || edata->elevel != ERROR)
		return;

	if (edata->sqlerrcode == ERRCODE_SUCCESSFUL_COMPLETION)
		return;

	if (!pel_done)
	{
		MemoryContext pelcontext, oldcontext;
		List *stmts;
		RawStmt *raw;
		Node    *stmt;
		const char *sql = debug_query_string;
		CmdType cmdType = CMD_UNKNOWN;
		char *operation = NULL;
		RangeVar *rv = NULL;
		Oid relid;

		pel_done = true; /* prevent recursive call */

		pelcontext = AllocSetContextCreate(CurTransactionContext,
								 "PEL temporary context",
								 ALLOCSET_DEFAULT_SIZES);
		oldcontext = MemoryContextSwitchTo(pelcontext);
#if PG_VERSION_NUM >= 140000
		stmts = raw_parser(sql, RAW_PARSE_DEFAULT);
#else
		stmts = raw_parser(sql);
#endif
		MemoryContextSwitchTo(oldcontext);
		stmts = list_copy_deep(stmts);
		MemoryContextDelete(pelcontext);

		if (list_length(stmts) != 1)
			elog(ERROR, "pel_log_error(): not supported");
		raw = (RawStmt *) linitial(stmts);
		stmt = raw->stmt;

		if (IsA(stmt, ExecuteStmt))
		{
			ExecuteStmt *s = (ExecuteStmt *) stmt;
			sql = lookupCachedPrepared(s->name);
#if PG_VERSION_NUM >= 140000
			stmts = raw_parser(sql, RAW_PARSE_DEFAULT);
#else
			stmts = raw_parser(sql);
#endif

			if (list_length(stmts) != 1)
				elog(ERROR, "not supported");
 
			raw = (RawStmt *) linitial(stmts);
			Assert(IsA(raw->stmt, PrepareStmt));
 
			stmt = ((PrepareStmt *) raw->stmt)->query;
		}
 
		if(IsA(stmt, InsertStmt))
		{
			rv = ((InsertStmt *) stmt)->relation;
			cmdType = CMD_INSERT;
			operation = "INSERT";
			current_dml_kind = 'I';
		}
		else if (IsA(stmt, UpdateStmt))
		{
			rv = ((UpdateStmt *) stmt)->relation;
			cmdType = CMD_UPDATE;
			operation = "UPDATE";
			current_dml_kind = 'U';
		}
		else if (IsA(stmt, DeleteStmt))
		{
			rv = ((DeleteStmt *) stmt)->relation;
			cmdType = CMD_DELETE;
			operation = "DELETE";
			current_dml_kind = 'D';
		}

		if (cmdType == CMD_UNKNOWN)
		{
			/* Unhandled DML, bail out */
			return;
		}
		else
		{
			Assert(rv != NULL);
			Assert(operation != NULL);
		}

		relid = RangeVarGetRelid(rv, AccessShareLock, true);

		if (!OidIsValid(relid))
		{
			if (rv->schemaname)
				elog(WARNING, "could not find an oid for relation \"%s\".\"%s\"",
						rv->schemaname, rv->relname);
			else
				elog(WARNING, "could not find an oid for relation \"%s\"",
						rv->relname);
			return;
		}

		elog(DEBUG1, "pel_log_error(): OPERATION: %s, KIND: %c, RELID: %u", operation, current_dml_kind, relid);

		/* Get the associated error logging table if any */
		if (OidIsValid(relid))
		{
			StringInfoData relstmt;
			StringInfoData msg;
			int rc = 0;
			char *logtable;
			int  finished = 0;
			bool isnull;

			initStringInfo(&relstmt);

			rc = SPI_connect();
			if (rc != SPI_OK_CONNECT)
				ereport(ERROR, (errmsg("Can not connect to SPI manager to retrieve error log table for \"%s\", rc=%d. ", rv->relname, rc)));
			appendStringInfo(&relstmt, "SELECT concat(quote_ident(n.nspname), '.', quote_ident(c.relname))::text FROM %s.register_errlog_tables e JOIN pg_catalog.pg_class c ON (c.oid = e.relerrlog) JOIN pg_namespace n ON (c.relnamespace=n.oid) WHERE e.reldml = %u", PEL_NAMESPACE_NAME, relid);
			rc = SPI_exec(relstmt.data, 0);
			if (rc != SPI_OK_SELECT || SPI_processed != 1)
				ereport(ERROR, (errmsg("SPI execution failure (rc=%d) on query: %s",
										rc, relstmt.data)));
			logtable = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0],
										SPI_tuptable->tupdesc,
										1,
										&isnull));
			if (isnull)
				ereport(ERROR, (errmsg("can not get error logging table for table %s", relstmt.data)));

			/* generate the full error message to log */
			initStringInfo(&msg);
			generate_error_message(edata, &msg);
			if (current_bind_parameters && current_bind_parameters[0] != '\0')
			{
				appendStringInfo(&msg, "PARAMETERS: %s", current_bind_parameters);

				pfree(current_bind_parameters);
				current_bind_parameters = NULL;
			}

			/*
			 * Append the error to the log table
			 */
			initStringInfo(&relstmt);
			appendStringInfo(&relstmt,
									(pel_synchronous ? PEL_SYNC_QUERY : PEL_ASYNC_QUERY),
									logtable,
									unpack_sql_state(edata->sqlerrcode),
									quote_literal_cstr(edata->message),
									current_dml_kind,
									(query_tag) ? quote_literal_cstr(query_tag) : "NULL",
									quote_literal_cstr(sql),
									quote_literal_cstr(msg.data)
									);
			rc = SPI_execute(relstmt.data, true, 0);
			if (rc != SPI_OK_SELECT || SPI_processed != 1)
				ereport(ERROR, (errmsg("SPI execution failure (rc=%d) on query: %s",
										rc, relstmt.data)));

			finished = SPI_finish();
			if (finished != SPI_OK_FINISH)
				ereport(ERROR, (errmsg("could not disconnect from SPI manager")));

			elog(DEBUG1, "pel_log_error(): ERRCODE: %s;KIND: %c,TAG: %s;MESSAGE: %s;QUERY: %s;TABLE: %s; INFO: %s",
								unpack_sql_state(edata->sqlerrcode),
								current_dml_kind,
								(query_tag) ? query_tag : "null",
								(edata->message) ? edata->message : "null",
								quote_literal_cstr(sql),
								rv->relname,
								msg.data
								);
		}
		if (pel_no_client_error)
			edata->output_to_client = false;
	}
	pel_done = false;
}

/*
 *      append_with_tabs
 *
 *      Append the string to the StringInfo buffer, inserting a tab after any
 *      newline.
 */
void
append_with_tabs(StringInfo buf, const char *str)
{
	char            ch;

	while ((ch = *str++) != '\0')
	{
		appendStringInfoCharMacro(buf, ch);
		if (ch == '\n')
			appendStringInfoCharMacro(buf, '\t');
	}
}

void
generate_error_message (ErrorData *edata, StringInfoData *buf)
{
	appendStringInfo(buf, "%s:  ", "ERROR");
	appendStringInfo(buf, "%s: ", unpack_sql_state(edata->sqlerrcode));

	if (edata->message)
		append_with_tabs(buf, edata->message);
	else
		append_with_tabs(buf, _("missing error text"));

	if (edata->cursorpos > 0)
		appendStringInfo(buf, _(" at character %d"),
				edata->cursorpos);
	else if (edata->internalpos > 0)
		appendStringInfo(buf, _(" at character %d"),
				edata->internalpos);

	appendStringInfoChar(buf, '\n');

	if (edata->detail_log)
	{
		appendStringInfoString(buf, _("DETAIL:  "));
		append_with_tabs(buf, edata->detail_log);
		appendStringInfoChar(buf, '\n');
	}
	else if (edata->detail)
	{
		appendStringInfoString(buf, _("DETAIL:  "));
		append_with_tabs(buf, edata->detail);
		appendStringInfoChar(buf, '\n');
	}
	if (edata->hint)
	{
		appendStringInfoString(buf, _("HINT:  "));
		append_with_tabs(buf, edata->hint);
		appendStringInfoChar(buf, '\n');
	}
	if (edata->internalquery)
	{
		appendStringInfoString(buf, _("QUERY:  "));
		append_with_tabs(buf, edata->internalquery);
		appendStringInfoChar(buf, '\n');
	}
	if (edata->context)
	{
		appendStringInfoString(buf, _("CONTEXT:  "));
		append_with_tabs(buf, edata->context);
		appendStringInfoChar(buf, '\n');
	}
	if (debug_query_string != NULL)
	{
		appendStringInfoString(buf, _("STATEMENT:  "));
		append_with_tabs(buf, debug_query_string);
		appendStringInfoChar(buf, '\n');
	}
}

#if PG_VERSION_NUM < 130000
/*
 * BuildParamLogString
 *              Return a string that represents the parameter list, for logging.
 *
 * If caller already knows textual representations for some parameters, it can
 * pass an array of exactly params->numParams values as knownTextValues, which
 * can contain NULLs for any unknown individual values.  NULL can be given if
 * no parameters are known.
 *
 * If maxlen is >= 0, that's the maximum number of bytes of any one
 * parameter value to be printed; an ellipsis is added if the string is
 * longer.  (Added quotes are not considered in this calculation.)
 */
char *
BuildParamLogString(ParamListInfo params, char **knownTextValues, int maxlen)
{
	MemoryContext tmpCxt,
				oldCxt;
	StringInfoData buf;

	/*
	 * NB: think not of returning params->paramValuesStr!  It may have been
	 * generated with a different maxlen, and so be unsuitable.  Besides that,
	 * this is the function used to create that string.
	 */

	/*
	 * No work if the param fetch hook is in use.  Also, it's not possible to
	 * do this in an aborted transaction.  (It might be possible to improve on
	 * this last point when some knownTextValues exist, but it seems tricky.)
	 */
	if (params->paramFetch != NULL ||
		IsAbortedTransactionBlockState())
		return NULL;

	/* Initialize the output stringinfo, in caller's memory context */
	initStringInfo(&buf);

	/* Use a temporary context to call output functions, just in case */
	tmpCxt = AllocSetContextCreate(CurrentMemoryContext,
								   "BuildParamLogString",
								   ALLOCSET_DEFAULT_SIZES);
	oldCxt = MemoryContextSwitchTo(tmpCxt);

	for (int paramno = 0; paramno < params->numParams; paramno++)
	{
		ParamExternData *param = &params->params[paramno];

		appendStringInfo(&buf,
						 "%s$%d = ",
						 paramno > 0 ? ", " : "",
						 paramno + 1);

		if (param->isnull || !OidIsValid(param->ptype))
			appendStringInfoString(&buf, "NULL");
		else
		{
			if (knownTextValues != NULL && knownTextValues[paramno] != NULL)
				appendStringInfoStringQuoted(&buf, knownTextValues[paramno],
											 maxlen);
			else
			{
				Oid                     typoutput;
				bool            typisvarlena;
				char       *pstring;

				getTypeOutputInfo(param->ptype, &typoutput, &typisvarlena);
				pstring = OidOutputFunctionCall(typoutput, param->value);
				appendStringInfoStringQuoted(&buf, pstring, maxlen);
			}
		}
	}

	MemoryContextSwitchTo(oldCxt);
	MemoryContextDelete(tmpCxt);

	return buf.data;
}

/*
 * appendStringInfoStringQuoted
 *
 * Append up to maxlen bytes from s to str, or the whole input string if
 * maxlen < 0, adding single quotes around it and doubling all single quotes.
 * Add an ellipsis if the copy is incomplete.
 */
void
appendStringInfoStringQuoted(StringInfo str, const char *s, int maxlen)
{
	char       *copy = NULL;
	const char *chunk_search_start,
			   *chunk_copy_start,
			   *chunk_end;
	int                     slen;
	bool            ellipsis;

	Assert(str != NULL);


	slen = strlen(s);
	if (maxlen >= 0 && maxlen < slen)
	{
		int                     finallen = pg_mbcliplen(s, slen, maxlen);

		copy = pnstrdup(s, finallen);
		chunk_search_start = copy;
		chunk_copy_start = copy;

		ellipsis = true;
	}
	else
	{
		chunk_search_start = s;
		chunk_copy_start = s;

		ellipsis = false;
	}

	appendStringInfoCharMacro(str, '\'');

	while ((chunk_end = strchr(chunk_search_start, '\'')) != NULL)
	{
		/* copy including the found delimiting ' */
		appendBinaryStringInfoNT(str,
								 chunk_copy_start,
								 chunk_end - chunk_copy_start + 1);

		/* in order to double it, include this ' into the next chunk as well */
		chunk_copy_start = chunk_end;
		chunk_search_start = chunk_end + 1;
	}

	/* copy the last chunk and terminate */
	if (ellipsis)
		appendStringInfo(str, "%s...'", chunk_copy_start);
	else
		appendStringInfo(str, "%s'", chunk_copy_start);

	if (copy)
		pfree(copy);
}
#endif
