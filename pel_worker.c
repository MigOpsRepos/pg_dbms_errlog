/*-------------------------------------------------------------------------
 *
 * pel_worker.h: Implementation of bgworker for error queue processing.
 *
 * This program is open source, licensed under the PostgreSQL license.
 * For license terms, see the LICENSE file.
 *
 * Copyright (C) 2021: ???
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#if PG_VERSION_NUM >= 100000

#include "access/xact.h"
#include "commands/dbcommands.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "miscadmin.h"
#if PG_VERSION_NUM < 140000
#include "pgstat.h"
#else
#include "utils/wait_event.h"
#endif
#include "postmaster/bgworker.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#if PG_VERSION_NUM >= 140000
#include "utils/backend_status.h"
#endif
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/ps_status.h"
#include "utils/snapmgr.h"

#include "include/pg_dbms_errlog.h"
#include "include/pel_worker.h"
#include "include/pel_errqueue.h"

#if PG_VERSION_NUM < 130000
#define PS_DISPLAY(m)	set_ps_display(m, false)
#else
#define PS_DISPLAY(m)	set_ps_display(m)
#endif

#define PEL_WORKER_MAX_DB	10

#define PEL_WORKER_INSERT "INSERT INTO %s.%s VALUES ('%s', %s, '%c', %s, %s, %s)"

typedef struct pelWorkerQueueItems
{
	int		nb_db;
	pid_t  *pid;
	Oid	   *dbs;
	int    *starting_pos;
	int	   *stopping_pos;
	BackgroundWorkerHandle **handles;
} pelWorkerQueueItems;

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;

static void pel_sighup(SIGNAL_ARGS);
static void pel_process_sighup(void);

static void pel_get_queue_items(pelWorkerQueueItems *items);

static void
pel_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;

	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

static void
pel_process_sighup(void)
{
	if (got_sighup)
	{
		got_sighup = false;

		ProcessConfigFile(PGC_SIGHUP);
	}
}

/*
 * Fillup the given pelWorkerQueueItems with work to be processed.
 *
 * We want to avoid spawning too many dynamic bgworkers, so we try to batch
 * queue items from the same database together.  However, we can't rely on the
 * launch of a dynamic bgworkers to always be successful, so we only batch them
 * if they're contiguous, otherwise we would risk to skip processing some queue
 * entries or process them multiple times.  We don't currently try to batch the
 * same database in multiple batch to avoid launching multiple dynamic
 * bgworkers on the same database, but this could be allowed in a later version
 * as the bgworker only performs INSERTs on tables without PK.
 *
 * We also save the current position in the global queue, so dynamic bgworker
 * will know where to stop even if other backend queue more errors.
 */
static void
pel_get_queue_items(pelWorkerQueueItems *items)
{
	Oid lastdbid = InvalidOid;
	int pos;

	items->nb_db = 0;

	Assert(!LWLockHeldByMe(pel->lock));
	Assert(items && items->pid && items->dbs && items->starting_pos &&
		   items->stopping_pos);

	LWLockAcquire(pel->lock, LW_SHARED);

	/* Remember the position of the current error */
	pel->bgw_saved_cur = pel->cur_err;

	if (pel->bgw_err == pel->cur_err)
	{
		LWLockRelease(pel->lock);
		return;
	}

	pos = pel->bgw_err;
	while (true)
	{
		Oid dbid;
		int i, arrid;

		pos = PEL_POS_NEXT(pos);

		dbid = pel_get_queue_item_dbid(pos);
		Assert(OidIsValid(dbid));
		if (!OidIsValid(lastdbid))
			lastdbid = dbid;

		arrid = -1;
		for(i = 0; i < items->nb_db; i++)
		{
			if (items->dbs[i] == dbid)
			{
				arrid = i;
				break;
			}
		}

		/* No more room for another worker */
		if (arrid == -1 && items->nb_db >= PEL_WORKER_MAX_DB)
			break;

		/*
		 * Found an already saved db that's non contiguous with the other one,
		 * we have to stop here.
		 */
		if (arrid >= 0 && dbid != lastdbid)
			break;

		if (arrid == -1)
		{
			/* New db (and worker), save the first position for it */
			items->pid[items->nb_db] = 0;
			items->dbs[items->nb_db] = dbid;
			items->starting_pos[items->nb_db] = pos;
			items->stopping_pos[items->nb_db] = pos;
			items->nb_db++;
		}
		else
		{
			/* Remember the last position seen for that db */
			items->stopping_pos[arrid] = pos;
		}

		/* Reached current queue item, no more work to do. */
		if (pos == pel->cur_err)
			break;
	}
	LWLockRelease(pel->lock);
}

void
pel_worker_main(Datum main_arg)
{
	pelWorkerQueueItems items;

	pqsignal(SIGHUP, pel_sighup);
	/* We get notified of dynamic bgworker termination through SIGUSR1 */
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	BackgroundWorkerUnblockSignals();

	elog(LOG, "pg_dbms_errlog main worker started");
	PS_DISPLAY("init");


	items.pid = (pid_t *) palloc(sizeof(pid_t) * pel_max_workers);
	items.dbs = (Oid *) palloc(sizeof(Oid) * pel_max_workers);
	items.starting_pos = (int *) palloc(sizeof(int) * pel_max_workers);
	items.stopping_pos = (int *) palloc(sizeof(int) * pel_max_workers);
	items.handles = (BackgroundWorkerHandle **)
		palloc(sizeof(BackgroundWorkerHandle *) * pel_max_workers);

	elog(PEL_DEBUG, "pel_worker_main(): set bgworker procno: %d",
		 MyProc->pgprocno);
	pg_atomic_write_u32(&pel->bgw_procno, MyProc->pgprocno);
	pel_init_dsm(true);

	while (true)
	{
		int i, last, rc;

		pel_process_sighup();

		PS_DISPLAY("checking for queue items to process");
		elog(PEL_DEBUG, "pel_worker_main() checking for queue items to process");
		pel->bgw_saved_cur = -1;
		pel_get_queue_items(&items);
		Assert(pel->bgw_saved_cur != -1);

		if (items.nb_db > 0)
		{
			PS_DISPLAY("launching dynamic bgworkers");
			elog(PEL_DEBUG, "pel_worker_main() will launch %d workers",
				 items.nb_db);
		}
		else
			elog(PEL_DEBUG, "pel_worker_main() no worker will be launched");

		last = -1;
		for (i = 0; i < items.nb_db; i++)
		{
			BackgroundWorker worker;

			worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
							   BGWORKER_BACKEND_DATABASE_CONNECTION;
			worker.bgw_start_time = BgWorkerStart_ConsistentState;
			worker.bgw_restart_time = BGW_NEVER_RESTART;
			sprintf(worker.bgw_library_name, "pg_dbms_errlog");
			sprintf(worker.bgw_function_name, "pel_dynworker_main");
			snprintf(worker.bgw_name, BGW_MAXLEN, "pel dyn worker for db %u",
					items.dbs[i]);
#if PG_VERSION_NUM >= 110000
			sprintf(worker.bgw_type, "pg_dbms_errlog");
#endif
			worker.bgw_main_arg = Int32GetDatum(items.starting_pos[i]);
			worker.bgw_notify_pid = MyProcPid;

			if (!RegisterDynamicBackgroundWorker(&worker, &items.handles[i]))
			{
				elog(WARNING, "could not register dynamic bgworker for db %u",
						items.starting_pos[i]);
				break;
			}
			else
				elog(PEL_DEBUG, "pel_worker_main() registered a bgworker for"
						" db %u", items.starting_pos[i]);
			last = i;
		}

		PS_DISPLAY("waiting for dynamic bgworker to finish");
		elog(PEL_DEBUG, "pel_worker_main() waiting for dynamic bgworker to finish");
		/* And now wait for the completion of all the workers */
		for (i = 0; i <= last; i++)
		{
			BgwHandleStatus status;

			status = WaitForBackgroundWorkerShutdown(items.handles[i]);
			switch (status)
			{
				case BGWH_STOPPED:
					/* done, we can wait for the next one */
					elog(PEL_DEBUG, "pel_worker_main() dynamic bgworker %d/%d"
							" terminated", i + 1, last + 1);
					break;
				case BGWH_POSTMASTER_DIED:
					/* postmaster died, simply exit */
					elog(PEL_DEBUG, "pel_worker_main(): set bgworker procno: %d",
						 INVALID_PGPROCNO);
					pg_atomic_write_u32(&pel->bgw_procno, INVALID_PGPROCNO);
					exit(1);
					break;
				default:
					/* should not happen */
					elog(PEL_DEBUG, "pel_worker_main(): set bgworker procno: %d",
						 INVALID_PGPROCNO);
					pg_atomic_write_u32(&pel->bgw_procno, INVALID_PGPROCNO);
					elog(WARNING, "unexpected worker status %d", status);
					exit(1);
					break;
			}
			LWLockAcquire(pel->lock, LW_EXCLUSIVE);
			pel->bgw_err = items.stopping_pos[i];
			LWLockRelease(pel->lock);
			pfree(items.handles[i]);
		}

		/*
		 * If we just processed some queued entries, check if some more work
		 * has been queue since we last woke up and process it immediately,
		 * otherwise we might miss some request from backends and let them wait
		 * up to pel_frequency, as their SetLatch could be reset by the dynamic
		 * bgworker infrastructure.
		 */
		if (last >= 0)
			continue;

		PS_DISPLAY("sleeping");
		elog(PEL_DEBUG, "pel_worker_main(): sleeping (procno: %d)",
			 MyProc->pgprocno);
		rc = WaitLatch(&MyProc->procLatch,
				WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
				pel_frequency * 1000,
				PG_WAIT_EXTENSION);
		ResetLatch(&MyProc->procLatch);
		elog(PEL_DEBUG, "pel_worker_main() woke up, rc: %d", rc);
	}
}

void
pel_dynworker_main(Datum main_arg)
{
	int				pos = DatumGetInt32(main_arg);
	pelWorkerEntry *entry;
	pelWorkerError *error;
	StringInfoData	sql;
	int i, limit;;

	pqsignal(SIGHUP, pel_sighup);
	BackgroundWorkerUnblockSignals();

	PS_DISPLAY("init");

	/*
	 * No need for lock as the main bgworker won't update it's saved position
	 * while dynamic bgworker are running.
	 */
	limit = pel->bgw_saved_cur;

	entry = pel_get_worker_entry(pos);

	elog(LOG, "pg_dbms_errlog dynamic worker for db %u started", entry->dbid);

	/* XXX should we use a non privileged user here? */
	BackgroundWorkerInitializeConnectionByOid(entry->dbid, InvalidOid
#if PG_VERSION_NUM >= 110000
			,0
#endif
			);

	set_config_option("pg_dbms_errlog.enabled", "off",
			PGC_SUSET, PGC_S_SESSION, GUC_ACTION_SET, true, 0, false);

	while(true)
	{
		Oid new_dbid;
		int rc;

		rc = SPI_connect();
		if (rc != SPI_OK_CONNECT)
			ereport(WARNING, (errmsg("Can not connect to SPI manager.")));

		StartTransactionCommand();
		SetCurrentStatementStartTimestamp();
		PushActiveSnapshot(GetTransactionSnapshot());

		for(i = 0; i < entry->num_errors; i++)
		{
			char *relnamespace, *relname;

			error = &entry->errors[i];

			relnamespace = get_namespace_name(get_rel_namespace(error->err_relid));
			Assert(relnamespace);

			relname = get_rel_name(error->err_relid);
			Assert(relname);

			initStringInfo(&sql);
			appendStringInfo(&sql, PEL_WORKER_INSERT,
					quote_identifier(relnamespace),
					quote_identifier(relname),
					unpack_sql_state(error->sqlstate),
					quote_literal_cstr(error->errmessage),
					error->cmdtype,
					error->query_tag ? quote_literal_cstr(error->query_tag) : "NULL",
					quote_literal_cstr(error->sql),
					error->detail ? quote_literal_cstr(error->detail) : "NULL"
					);
			pgstat_report_activity(STATE_RUNNING, sql.data);
			SPI_execute(sql.data, false, 0);
                        if (rc != SPI_OK_INSERT)
                        if (SPI_processed != 1)
                                ereport(WARNING,
						(errmsg("SPI execution failure (rc=%d) on query: %s",
											rc, sql.data)));
			elog(PEL_DEBUG, "pel_dynworker_main(): inserted %ld rows", SPI_processed);
			pgstat_report_activity(STATE_IDLE, NULL);

			pfree(sql.data);
		}

		rc = SPI_finish();
		if (rc != SPI_OK_FINISH)
			elog(WARNING, "could not disconnect from SPI manager");
		PopActiveSnapshot();
		CommitTransactionCommand();

		/*
		 * Cleanup data structure that was set in pel_get_worker_entry() and
		 * notify caller if it was required
		 */
		pel_worker_entry_complete();

		/* Move to the next item in the batch if any */
		pos = PEL_POS_NEXT(pos);
		elog(PEL_DEBUG, "pel_dynworker_main(): new pos after processing %d", pos);

		if (pos > limit)
			break;

		LWLockAcquire(pel->lock, LW_SHARED);
		new_dbid = pel_get_queue_item_dbid(pos);
		LWLockRelease(pel->lock);

		if (new_dbid != entry->dbid)
			break;

		entry = pel_get_worker_entry(pos);
	}

	pgstat_report_activity(STATE_IDLE, NULL);
	elog(LOG, "pg_dbms_errlog dynamic worker for db %u finished", entry->dbid);
}
#endif			/* pg10 */
