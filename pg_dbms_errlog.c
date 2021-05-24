/*-------------------------------------------------------------------------
 *
 * pg_dbms_errlog.c:
 * 	pg_dbms_errlog is a PostgreSQL extension that logs each failing DML
 * 	query. It emulates the DBMS_ERRLOG Oracle module.
 *
 * This program is open source, licensed under the PostgreSQL license.
 * For license terms, see the LICENSE file.
 *
 * Copyright (C) 2021: MigOps Inc
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#if PG_VERSION_NUM >= 130000
#include "access/table.h"
#else
/* for imported functions */
#include "mb/pg_wchar.h"
#endif
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_namespace.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "parser/analyze.h"
#include "parser/parser.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#if PG_VERSION_NUM < 110000
#include "utils/memutils.h"
#endif
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/varlena.h"
#if PG_VERSION_NUM >= 140000
#include "utils/wait_event.h"
#else
#include "pgstat.h"
#endif

#include "include/pg_dbms_errlog.h"
#include "include/pel_errqueue.h"

#if PG_VERSION_NUM < 100000
#error Minimum version of PostgreSQL required is 10
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

#define PEL_REGISTRATION_TABLE "register_errlog_tables"
#define Anum_pel_relid 1
#define Anum_pel_errlogid 2

#define MAX_PREPARED_STMT_SIZE   1048576


PG_MODULE_MAGIC;

#define PEL_TRANCHE_NAME		"pg_dbms_errlog"

/* Saved hook values in case of unload */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static ProcessUtility_hook_type prev_ProcessUtility = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static emit_log_hook_type prev_emit_log_hook = NULL;
static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;

/* Links to shared memory state */
pelSharedState *pel = NULL;
dsa_area *pel_area = NULL;

/* GUC variables */

typedef enum
{
	PEL_SYNC_OFF,				/* never wait for queued errors processing */
	PEL_SYNC_QUERY,				/* wait for every error */
	PEL_SYNC_XACT				/* wait at the end of the xact (implicit or not) */
}			pelSyncLevel;

static const struct config_enum_entry pel_sync_options[] =
{
	{"off", PEL_SYNC_OFF, false},
	{"query", PEL_SYNC_QUERY, false},
	{"transaction", PEL_SYNC_XACT, false},
	{NULL, 0, false}
};

#define PEL_SYNC_ON_QUERY()	(pel_synchronous == PEL_SYNC_QUERY || \
		(pel_synchronous == PEL_SYNC_XACT && !IsTransactionBlock()))
/*
 * We also have to wait for completion if level is PEL_SYNC_QUERY and we're in
 * a transaction block, as it could have be raised from PEL_SYNC_OFF just
 * before a COMMIT, which should force a sync
 */
#define PEL_SYNC_ON_XACT()	(pel_synchronous == PEL_SYNC_XACT || \
		(pel_synchronous == PEL_SYNC_QUERY && IsTransactionBlock()))
bool pel_debug = false;
bool pel_done = false;
bool pel_enabled = false;
int pel_frequency = 60;
int pel_max_workers = 1;
int  pel_reject_limit = -1;
char *query_tag = NULL;
int pel_synchronous = PEL_SYNC_XACT;
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

extern PGDLLEXPORT Datum pg_dbms_errlog_publish_queue(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_dbms_errlog_publish_queue);
extern PGDLLEXPORT Datum pg_dbms_errlog_queue_size(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_dbms_errlog_queue_size);

static void pel_shmem_startup(void);
static void pel_ProcessUtility(PEL_PROCESSUTILITY_PROTO);
static void pel_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void pel_ExecutorRun(QueryDesc *queryDesc,
							ScanDirection direction,
							uint64 count, bool execute_once);
static void pel_ExecutorFinish(QueryDesc *queryDesc);
static void pel_ExecutorEnd(QueryDesc *queryDesc);
static void pel_log_error(ErrorData *edata);
static Size pel_memsize(void);
char *lookupCachedPrepared(const char *preparedName);
void removeCachedPrepared(const char *localPreparedName);
static void pel_setupCachedPreparedHash(void);
void putCachedPrepared(const char *preparedName, const char *preparedStmt);
char *get_relation_name(Oid relid);
void generate_error_message (ErrorData *edata, StringInfoData *buf);
void append_with_tabs(StringInfo buf, const char *str);
#if PG_VERSION_NUM < 130000
/* Copied from src/backend/nodes/params.c for older versions */
char *BuildParamLogString(ParamListInfo params, char **knownTextValues, int maxlen);
/* Copied from src/backend/utils/mb/stringinfo_mb.c */
void appendStringInfoStringQuoted(StringInfo str, const char *s, int maxlen);
/* Copied from src/backend/access/transam/xact.c */
/* Adapted from src/backend/nodes/list.c */
static List *list_copy_deep(const List *oldlist);
#endif
#if PG_VERSION_NUM < 110000
static void appendBinaryStringInfoNT(StringInfo str, const char *data, int datalen);
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

static void
pel_setupCachedPreparedHash(void)
{
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
}

void
putCachedPrepared(const char *preparedName, const char *preparedStmt)
{
	PreparedCacheKey key = { { 0 } };
	PreparedCacheEntry *entry;
	bool found;

	pel_setupCachedPreparedHash();

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

	pel_setupCachedPreparedHash();

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

	pel_setupCachedPreparedHash();

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
	BackgroundWorker worker;

	if (!process_shared_preload_libraries_in_progress)
	{
		elog(ERROR, "This module can only be loaded via shared_preload_libraries");
		return;
	}

	/* Define custom GUC variables */
	DefineCustomBoolVariable( "pg_dbms_errlog.debug",
				"Enable/disable debug traces.",
				NULL,
				&pel_debug,
				false,
				PGC_USERSET, /* Any user can set it */
				0,
				NULL,
				NULL,
				NULL);

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

	DefineCustomIntVariable("pg_dbms_errlog.frequency",
				"Defines the frequency for checking for data to process",
				NULL,
				&pel_frequency,
				60,
				10,
				3600,
				PGC_SUSET,
				GUC_UNIT_S,
				NULL,
				NULL,
				NULL);

	DefineCustomIntVariable("pg_dbms_errlog.max_workers",
				"Defines the maximum number of bgworker to launch to process data",
				NULL,
				&pel_max_workers,
				1,
				1,
				max_worker_processes,
				PGC_POSTMASTER,
				0,
				NULL,
				NULL,
				NULL);

	DefineCustomIntVariable("pg_dbms_errlog.reject_limit",
				"Maximum number of errors that can be encountered before the DML"
				" statement terminates and rolls back. A value of -1 mean unlimited,"
				" this is the default. When reject limit is zero no error is logged"
				" and the statement rolls back.",
				NULL,
				&pel_reject_limit,
				-1,
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

	DefineCustomEnumVariable("pg_dbms_errlog.synchronous",
				"Wait for error queue completion when an error happens",
				NULL,
				&pel_synchronous,
				PEL_SYNC_XACT,
				pel_sync_options,
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

	/*
	 * Request additional shared resources.  (These are no-ops if we're not in
	 * the postmaster process.)  We'll allocate or attach to the shared
	 * resources in pel_shmem_startup().
	 */
	RequestAddinShmemSpace(pel_memsize());
	RequestNamedLWLockTranche(PEL_TRANCHE_NAME, 1);


	/* Install hooks */
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pel_shmem_startup;
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

	memset(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time =     BgWorkerStart_RecoveryFinished;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "pg_dbms_errlog");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "pel_worker_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_dbms_errlog main worker");
	worker.bgw_restart_time = 0;
	worker.bgw_main_arg = (Datum) 0;
	worker.bgw_notify_pid = 0;
	RegisterBackgroundWorker(&worker);
}

/*
 * Module unload callback
 */
void
_PG_fini(void)
{
	/* Uninstall hooks */
	shmem_startup_hook = prev_shmem_startup_hook;
	ProcessUtility_hook = prev_ProcessUtility;
	ExecutorStart_hook = prev_ExecutorStart;
	ExecutorRun_hook = prev_ExecutorRun;
	ExecutorFinish_hook = prev_ExecutorFinish;
	ExecutorEnd_hook = prev_ExecutorEnd;
	emit_log_hook = prev_emit_log_hook;
	post_parse_analyze_hook = prev_post_parse_analyze_hook;
}

PGDLLEXPORT Datum
pg_dbms_errlog_publish_queue(PG_FUNCTION_ARGS)
{
	bool sync;
	int pos;

	if (PG_ARGISNULL(0))
		sync = false;
	else
		sync = PG_GETARG_BOOL(0);

	pos = pel_publish_queue(sync);

	PG_RETURN_BOOL(pos != PEL_PUBLISH_ERROR);
}

PGDLLEXPORT Datum
pg_dbms_errlog_queue_size(PG_FUNCTION_ARGS)
{
	int num = pel_queue_size();

	if (num == -1)
		PG_RETURN_NULL();
	else
		PG_RETURN_INT32(num);
}

static void
pel_shmem_startup(void)
{
	bool found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	/* reset in case this is a restart within the postmaster */
	pel = NULL;

	/* Create or attach to the shared memory state */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	pel = ShmemInitStruct("pg_dbms_errlog",
			pel_memsize(),
			&found);

	if (!found)
	{
		int     trancheid;

		/* First time through ... */
		pel->bgw_saved_cur = 0;
		pg_atomic_init_u32(&pel->bgw_procno, INVALID_PGPROCNO);
		StaticAssertStmt(sizeof(INVALID_PGPROCNO <= sizeof(pel->bgw_procno)),
				"INVALID_PGPROCNO is bigger for uint32");
		pel->lock = &(GetNamedLWLockTranche(PEL_TRANCHE_NAME))->lock;
		pel->pel_dsa_handle = DSM_HANDLE_INVALID;
		pel->pqueue = InvalidDsaPointer;
		pel->max_errs = 0;
		pel->cur_err = 0;
		pel->bgw_err = 0;

		/* try to guess our trancheid */
		for (trancheid = LWTRANCHE_FIRST_USER_DEFINED; ; trancheid++)
		{
			if (strcmp(GetLWLockIdentifier(PG_WAIT_LWLOCK, trancheid),
						PEL_TRANCHE_NAME) == 0)
			{
				/* Found it! */
				break;
			}
			if ((trancheid - LWTRANCHE_FIRST_USER_DEFINED) > 50)
			{
				/* No point trying so hard, just give up. */
				trancheid = LWTRANCHE_FIRST_USER_DEFINED;
				break;
			}
		}
		Assert(trancheid >= LWTRANCHE_FIRST_USER_DEFINED);
		pel->LWTRANCHE_PEL = trancheid;
	}

	LWLockRelease(AddinShmemInitLock);
}

static void
pel_ProcessUtility(PEL_PROCESSUTILITY_PROTO)
{
	Node *parsetree = pstmt->utilityStmt;

	pel_done = false;

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

	/* Publish or discard queue on COMMIT/ROLLBACK */
	if (IsA(parsetree, TransactionStmt))
	{
		TransactionStmt *stmt = (TransactionStmt *) parsetree;

		/* Check if the commit really performed a commit */
		if (stmt->kind == TRANS_STMT_COMMIT)
		{
			bool is_commit = false;

#if PG_VERSION_NUM >= 130000
			if (!qc)
			{
				/* no way to tell, assume commit did happen */
				is_commit = true;
			}
			else if (qc->commandTag != CMDTAG_ROLLBACK)
				is_commit = true;
#else
			if (strcmp(completionTag, "ROLLBACK") != 0)
				is_commit = true;
#endif

			if (is_commit)
			{
				if (pel_publish_queue(PEL_SYNC_ON_XACT()) == PEL_PUBLISH_ERROR)
					elog(WARNING, "could not publish the queue");
			}
			else
				pel_discard_queue();
		}
		else if (stmt->kind == TRANS_STMT_ROLLBACK)
			pel_discard_queue();
	}
}

static void
pel_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	if (pel_enabled)
	{
		if (queryDesc->dest->mydest != DestSPI)
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
			pel_done = false;
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
		exec_nested_level--;
	}
	PG_CATCH();
	{
		exec_nested_level--;
		PG_RE_THROW();
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
		exec_nested_level--;
	}
	PG_CATCH();
	{
		exec_nested_level--;
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * ExecutorEnd hook: done required cleanup
 */
static void
pel_ExecutorEnd(QueryDesc *queryDesc)
{
	if (pel_enabled && exec_nested_level == 0)
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
	if (!pel_enabled
		|| !edata || edata->elevel != ERROR /* Only process errors */
		|| !debug_query_string /* Ignore errors raised from non-backend processes */
		|| edata->sqlerrcode == ERRCODE_SUCCESSFUL_COMPLETION
		)
	{
		goto prev_hook;
	}

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
		{
			elog(WARNING, "pel_log_error(): not supported");
			goto prev_hook;
		}
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
			{
				elog(WARNING, "not supported");
				goto prev_hook;
			}
 
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
			goto prev_hook;
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
			goto prev_hook;
		}

		elog(DEBUG1, "pel_log_error(): OPERATION: %s, KIND: %c, RELID: %u", operation, current_dml_kind, relid);

		/* Get the associated error logging table if any */
		if (OidIsValid(relid))
		{
			StringInfoData relstmt;
			StringInfoData msg;
			int rc = 0;
			Oid  logtable;
			bool isnull, ok;
			bool need_priv_escalation = !superuser(); /* we might be a SU */
			Oid  save_userid;
			int  save_sec_context;
			Relation      rel;
			AclResult     aclresult;

			initStringInfo(&relstmt);

			/* Inserting error to log table must be created as SU */
                        if (need_priv_escalation)
			{
				/* Get current user's Oid and security context */
				GetUserIdAndSecContext(&save_userid, &save_sec_context);
				/* Become superuser */
				SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, save_sec_context
									| SECURITY_LOCAL_USERID_CHANGE
									| SECURITY_RESTRICTED_OPERATION);
			}

			rc = SPI_connect();
			if (rc != SPI_OK_CONNECT)
			{
				elog(WARNING, "Can not connect to SPI manager to retrieve"
							  " error log table for \"%s\", rc=%d. ",
					rv->relname, rc);
				goto prev_hook;
			}

			appendStringInfo(&relstmt, "SELECT e.relerrlog"
					" FROM %s.%s e"
					" WHERE e.reldml = %u",
					quote_identifier(PEL_NAMESPACE_NAME),
					quote_identifier(PEL_REGISTRATION_TABLE),
					relid);

			rc = SPI_exec(relstmt.data, 0);
			if (rc != SPI_OK_SELECT || SPI_processed != 1)
			{
				elog(WARNING, "SPI execution failure (rc=%d) on query: %s",
					 rc, relstmt.data);
				goto prev_hook;
			}

			logtable = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[0],
										SPI_tuptable->tupdesc,
										1,
										&isnull));
			if (isnull)
			{
				elog(WARNING, "can not get error logging table for table %s",
					 relstmt.data);
				goto prev_hook;
			}

			rc = SPI_finish();
			if (rc != SPI_OK_FINISH)
			{
				elog(WARNING, "could not disconnect from SPI manager");
				goto prev_hook;
			}

			/* Restore user's privileges */
			if (need_priv_escalation)
				SetUserIdAndSecContext(save_userid, save_sec_context);

			/*
			 * Try to open the error log relation to catch priviledge issues
			 * as the bg_worker will have the full priviledge on the table.
			 */
			rel = table_open(logtable, AccessShareLock);
			aclresult = pg_class_aclcheck(RelationGetRelid(rel), GetUserId(),
											ACL_INSERT);
			if (aclresult != ACLCHECK_OK)
				aclcheck_error(aclresult, get_relkind_objtype(rel->rd_rel->relkind),
							RelationGetRelationName(rel));
			table_close(rel, AccessShareLock);

			/* generate the full error message to log */
			initStringInfo(&msg);
			generate_error_message(edata, &msg);
			if (current_bind_parameters && current_bind_parameters[0] != '\0')
			{
				appendStringInfo(&msg, "PARAMETERS: %s",
								 current_bind_parameters);
			}

			/* Queue the error information. */
			ok = pel_queue_error(logtable,
					edata->sqlerrcode,
					edata->message,
					current_dml_kind,
					query_tag,
					sql,
					msg.data,
					PEL_SYNC_ON_QUERY());
			if (!ok)
				goto prev_hook;

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

prev_hook:
	if (current_bind_parameters != NULL)
	{
		pfree(current_bind_parameters);
		current_bind_parameters = NULL;
	}

	/* Continue chain to previous hook */
	if (prev_emit_log_hook)
		(*prev_emit_log_hook) (edata);
}

static Size
pel_memsize(void)
{
	Size size;

	size = CACHELINEALIGN(sizeof(pelSharedState));

	return size;
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

#ifdef USE_ASSERT_CHECKING
/*
 * Check that the specified List is valid (so far as we can tell).
 */
static void
check_list_invariants(const List *list)
{
	if (list == NIL)
		return;

	Assert(list->length > 0);
	Assert(list->head != NULL);
	Assert(list->tail != NULL);

	Assert(list->type == T_List ||
		   list->type == T_IntList ||
		   list->type == T_OidList);

	if (list->length == 1)
		Assert(list->head == list->tail);
	if (list->length == 2)
		Assert(list->head->next == list->tail);
	Assert(list->tail->next == NULL);
}
#else
#define check_list_invariants(l)
#endif							/* USE_ASSERT_CHECKING */

/*
 * Return a deep copy of the specified list.
 *
 * The list elements are copied via copyObject(), so that this function's
 * idea of a "deep" copy is considerably deeper than what list_free_deep()
 * means by the same word.
 */
static List *
list_copy_deep(const List *oldlist)
{
	List	   *newlist = NIL;
	ListCell   *lc;

	if (oldlist == NIL)
		return NIL;

	if (oldlist->type != T_List)
	{
		/* Should not be reached in our code */
		elog(ERROR, "list type unsupported");
	}

	/* This is only sensible for pointer Lists */
	Assert(IsA(oldlist, List));

	foreach(lc, oldlist)
		newlist = lappend(newlist, copyObjectImpl(lfirst(lc)));

	check_list_invariants(newlist);
	return newlist;
}
#endif

#if PG_VERSION_NUM < 110000
/*
 * appendBinaryStringInfoNT
 *
 * Append arbitrary binary data to a StringInfo, allocating more space
 * if necessary. Does not ensure a trailing null-byte exists.
 */
static void
appendBinaryStringInfoNT(StringInfo str, const char *data, int datalen)
{
	Assert(str != NULL);

	/* Make more room if needed */
	enlargeStringInfo(str, datalen);

	/* OK, append the data */
	memcpy(str->data + str->len, data, datalen);
	str->len += datalen;
}
#endif
