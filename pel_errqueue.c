/*-------------------------------------------------------------------------
 *
 * pel_errrqueue.c:
 * 	Implementation of error logging queue in dynshm.
 *
 * This program is open source, licensed under the PostgreSQL license.
 * For license terms, see the LICENSE file.
 *
 * Copyright (C) 2021: MigOps Inc
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#if PG_VERSION_NUM >= 100000

#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/dsa.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#if PG_VERSION_NUM >= 140000
#include "utils/wait_event.h"
#else
#include "pgstat.h"
#endif
#include "include/pg_dbms_errlog.h"
#include "include/pel_errqueue.h"

#define PEL_QUEUE_SIZE			1000 /* XXX alow queue size to grow? */
#define PEL_ENTRY_INIT_SIZE		50

typedef struct pelError
{
	Oid			err_relid;
	int			sqlstate;
	dsa_pointer perrmessage;
	char		cmdtype;
	dsa_pointer pquery_tag;
	dsa_pointer psql;
	dsa_pointer pdetail;
} pelError;

typedef struct pelQueueEntry
{
	int			num_entries;
	int			max_entries;
	dsa_pointer	perrors; /* array of pelError */
} pelQueueEntry;

typedef struct pelGlobalEntry
{
	Oid			dbid;
	int			pgprocno;
	dsa_handle handle;
	dsa_pointer pentry; /* store a pelQueueEntry */
	bool		notify;
	bool		processed;
} pelGlobalEntry;

typedef struct pelGlobalQueue
{
	pelGlobalEntry *entries; /* used as token ring */
} pelGlobalQueue;

/*
 * Struct containing reference to the current local entry, used in a global
 * variable
 */
typedef struct pelLocalData
{
	dsa_area *area;
	dsa_pointer pentry;
	pelQueueEntry *entry;
	pelError *errors;
	int pos;		/* offset of the entry in queue.entries, -1 if no local
					   entry */
	int last_pos;	/* offset of the last queued and not processed entry, -1 if
					   none */
} pelLocalData;

/* Local variables */
static pelGlobalQueue queue = { 0 };

static pelLocalData local_data = { NULL, InvalidDsaPointer, NULL, NULL, -1, -1 };

/* Functions declaration */
static void pel_attach_dsa(bool globalOnly);
static void pel_setup_local_area(dsm_handle handle);
static void pel_attach_entry(dsa_pointer pentry);
static bool pel_prepare_entry(void);
static void pel_cleanup_local(void);

/* Create or attach to dsa and allocate the global queue if needed.
 *
 * can throw an error if the global queue can't be allocated
 */
static void
pel_attach_dsa(bool globalOnly)
{
	MemoryContext	oldcontext;

	Assert(!LWLockHeldByMe(pel->lock));

	/* Nothing to do if we're already attached to the dsa. */
	if (pel_area != NULL && (globalOnly || local_data.area != NULL))
		return;

	oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	LWLockAcquire(pel->lock, LW_EXCLUSIVE);
	if (pel->pel_dsa_handle == DSM_HANDLE_INVALID)
	{
		pel_area = dsa_create(pel->LWTRANCHE_PEL);
		dsa_pin(pel_area);
		dsa_pin_mapping(pel_area);
		pel->pel_dsa_handle = dsa_get_handle(pel_area);
	}
	else if (pel_area == NULL)
	{
		pel_area = dsa_attach(pel->pel_dsa_handle);
		dsa_pin_mapping(pel_area);
	}

	MemoryContextSwitchTo(oldcontext);

	if (pel->pqueue == InvalidDsaPointer)
	{
		Assert(queue.entries == NULL);
		pel->pqueue = dsa_allocate_extended(pel_area,
				sizeof(pelGlobalEntry) * PEL_QUEUE_SIZE, DSA_ALLOC_ZERO);
		pel->max_errs = PEL_QUEUE_SIZE;
		queue.entries = dsa_get_address(pel_area, pel->pqueue);
	}
	else if (queue.entries == NULL)
	{
		Assert(pel->pqueue != InvalidDsaPointer);

		queue.entries = dsa_get_address(pel_area, pel->pqueue);
	}

	Assert(pel->pqueue != InvalidDsaPointer && queue.entries != NULL);

	LWLockRelease(pel->lock);

	if (globalOnly)
		return;

	if (local_data.area == NULL)
		pel_setup_local_area(InvalidDsaPointer);
}

/*
 * Only create or attach to the local area, the rest of the data will be setup
 * in pel_prepare_entry() or pel_attach_entry().
 */
static void
pel_setup_local_area(dsm_handle handle)
{
	MemoryContext	oldcontext;

	Assert(local_data.area == NULL);
	Assert(local_data.entry == NULL);
	Assert(local_data.pentry == InvalidDsaPointer);
	Assert(local_data.errors == NULL);

	if (handle == DSM_HANDLE_INVALID)
	{
		/* This area will only be pinned when published */
		/* FIXME: use a dedicated context under TopTransactionContext */
		oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		local_data.area = dsa_create(pel->LWTRANCHE_PEL);
		MemoryContextSwitchTo(oldcontext);
	}
	else
	{
		oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		local_data.area = dsa_attach(handle);
		MemoryContextSwitchTo(oldcontext);
	}
	dsa_pin_mapping(local_data.area);
}

static void
pel_attach_entry(dsa_pointer pentry)
{
	Assert(local_data.area != NULL);
	Assert(local_data.pentry == InvalidDsaPointer);
	Assert(local_data.entry == NULL);
	Assert(local_data.errors == NULL);

	local_data.pentry = pentry;
	local_data.entry = dsa_get_address(local_data.area, local_data.pentry);
	local_data.errors = dsa_get_address(local_data.area,
										local_data.entry->perrors);
}

/* Create a pelQueueEntry or make room for a new error */
static bool
pel_prepare_entry(void)
{
	Assert(!LWLockHeldByMe(pel->lock));
	pel_attach_dsa(false);

	if (local_data.pentry == InvalidDsaPointer)
	{
		if (pel_reject_limit == 0)
		{
			elog(WARNING, "pg_dbms_errlog.reject_limit is set to 0, no error"
						  " is handled");
			return false;
		}

		local_data.pentry = dsa_allocate_extended(local_data.area, sizeof(pelQueueEntry),
				DSA_ALLOC_ZERO | DSA_ALLOC_NO_OOM);

		if (!local_data.pentry)
		{
			pel_cleanup_local();
			return false;
		}

		local_data.pos = -1;
	}

	local_data.entry = dsa_get_address(local_data.area, local_data.pentry);

	if (local_data.entry->max_entries >= pel_reject_limit )
	{
		elog(WARNING, "pg_dbms_errlog.reject_limit is reached, no further"
					  " error is handled");
		pel_cleanup_local();
		return false;
	}

	/* init or grow the entry if needed */
	if (local_data.entry->max_entries == 0 ||
			local_data.entry->num_entries >= local_data.entry->max_entries)
	{
		bool	do_init = (local_data.entry->max_entries == 0);

		Assert(
			(local_data.entry->num_entries == 0 &&
			 local_data.entry->perrors == InvalidDsaPointer
			) ||
			(local_data.entry->num_entries != 0 &&
			 local_data.entry->perrors != InvalidDsaPointer)
		);

		/* FIXME implement the memory size limit */
		if (local_data.entry->max_entries == 0)
			local_data.entry->max_entries = PEL_ENTRY_INIT_SIZE;
		else
			local_data.entry->max_entries *= 2;

		if (do_init)
		{
			Assert(local_data.errors == NULL);
			Assert(local_data.entry->perrors == InvalidDsaPointer);

			local_data.entry->perrors = dsa_allocate_extended(local_data.area,
					sizeof(pelError) * local_data.entry->max_entries,
					DSA_ALLOC_NO_OOM);

			if (local_data.entry->perrors == InvalidDsaPointer)
			{
				pel_cleanup_local();
				return false;
			}

			local_data.errors = dsa_get_address(local_data.area,
												local_data.entry->perrors);
		}
		else
		{
			dsa_pointer new_perrors = InvalidDsaPointer;
			pelError *new_errors;

			Assert(local_data.entry != NULL);

			new_perrors = dsa_allocate_extended(local_data.area,
					sizeof(pelError) * local_data.entry->max_entries, DSA_ALLOC_NO_OOM);

			if (new_perrors == InvalidDsaPointer)
			{
				pel_cleanup_local();
				return false;
			}

			new_errors = dsa_get_address(local_data.area, new_perrors);

			memcpy(new_errors, local_data.entry, sizeof(pelError) * local_data.entry->num_entries);

			local_data.entry = NULL;
			dsa_free(local_data.area, local_data.entry->perrors);
			local_data.entry->perrors = new_perrors;
			local_data.errors = new_errors;
		}
	}

	Assert(local_data.entry != NULL);
	Assert(local_data.errors != NULL);
	return true;
}

/*
 * Cleanup local variables and detach from the local area.
 */
static void
pel_cleanup_local()
{
	Assert(local_data.area != NULL);

	local_data.pentry = InvalidDsaPointer;
	local_data.entry = NULL;
	local_data.errors = NULL;
	local_data.pos = -1;

	/*
	 * dsa_detach will free the local memory allocated in dsa_create() if the
	 * area isn't pinned.
	 */
	dsa_detach(local_data.area);

	local_data.area = NULL;
}

/* Discard errors cached in dyn shm not yet published, if any */
void pel_discard_queue(void)
{
	pel_attach_dsa(false);

	pel_cleanup_local();
}

/* Publish the pelError in the global queue.
 *
 * If sync is true, it wakes up the bgworker and wait for it to process all
 * queued entries until the one we just published.
 */
int
pel_publish_queue(bool sync)
{
	int pos;

	pel_attach_dsa(false);
	Assert(queue.entries != NULL);

	/* No work queued */
	if(local_data.pentry == InvalidDsaPointer)
	{
		pel_cleanup_local();

		/*
		 * If there wasn't any previously queued work or if it's async mode,
		 * return immediately.
		 */
		if (local_data.last_pos < 0 || !sync)
		{
			elog(PEL_DEBUG, "pel_publish_queue(): no queued work or async mode (%d)", sync);
			return PEL_PUBLISH_EMPTY;
		}

		/* Restore the last known queue entry position */
		pos = local_data.last_pos;
		elog(PEL_DEBUG, "pel_publish_queue(): restore the last known queue entry position %d", pos);
		LWLockAcquire(pel->lock, LW_EXCLUSIVE);
		/*
		 * Check if the entry in that position is still ours.  If yes, ask to
		 * be notify, otherwise we know it was already processed so we can
		 * inform caller.
		 */
		if (queue.entries[pos].pgprocno == MyProc->pgprocno)
			queue.entries[pos].notify = true;
		else
			local_data.last_pos = -1;
		LWLockRelease(pel->lock);

		if (local_data.last_pos == -1)
			return PEL_PUBLISH_EMPTY;

		goto wait_for_bgworker;
	}

	Assert(!LWLockHeldByMe(pel->lock));
	LWLockAcquire(pel->lock, LW_EXCLUSIVE);

	/* bail out if nothing is queued */
	if (local_data.entry->num_entries == 0)
	{
		LWLockRelease(pel->lock);
		pel_cleanup_local();
		goto error;
	}

	/* check if there's enough room to store the entry */
	if (pel->cur_err != pel->bgw_err)
	{
		if (PEL_POS_NEXT(pel->cur_err) == pel->bgw_err)
		{
			/* no more room, too bad */
			LWLockRelease(pel->lock);
			pel_cleanup_local();
			goto error;
		}
	}

	pos = PEL_POS_NEXT(pel->cur_err);

	local_data.last_pos = pos;

	queue.entries[pos].dbid = MyDatabaseId;
	queue.entries[pos].pgprocno = MyProc->pgprocno;
	queue.entries[pos].handle = dsa_get_handle(local_data.area);
	queue.entries[pos].pentry = local_data.pentry;
	queue.entries[pos].notify = sync;
	queue.entries[pos].processed = false;

	/* We transfer the area to the queue, so make it long lived */
	dsa_pin(local_data.area);

	/*
	 * XXX is an error happens here we'll leak the memory forever. Should we
	 * first make the entry visible and handle a possibly freed area?
	 */

	/* And make it visible in the global queue */
	pel->cur_err = pos;

	LWLockRelease(pel->lock);

	/* And reset local variables */
	pel_cleanup_local();

wait_for_bgworker:
	elog(PEL_DEBUG, "pel_publish_queue(): wait_for_bgworker, sync: %d", sync);
	Assert(!LWLockHeldByMe(pel->lock));

	if (sync == true)
	{
		uint32	bgw_procno = pg_atomic_read_u32(&pel->bgw_procno);
		int		sleep_time = 1;
		bool	processed;

		while (bgw_procno == INVALID_PGPROCNO)
		{
			/*
			 * If we're in the middle of an error interruption, interrupts will
			 * be held, so we'll have to detect a cancel or terminate pending
			 * and act accordingly.
			 */
			if (InterruptHoldoffCount > 0)
			{
				if (QueryCancelPending || ProcDiePending)
					goto error;
			}
			else
				CHECK_FOR_INTERRUPTS();

			elog(PEL_DEBUG, "pel_publish_queue(): WaitLatch for %d", sleep_time);
			WaitLatch(&MyProc->procLatch,
					WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					sleep_time,
					PG_WAIT_EXTENSION);
			ResetLatch(&MyProc->procLatch);

			sleep_time = Max(sleep_time * 2, 100);

			bgw_procno = pg_atomic_read_u32(&pel->bgw_procno);
		}

		/* Wake up the bgworker to force queue processing */
		elog(PEL_DEBUG, "pel_publish_queue(): set bgworker latch (procno: %d)",
			 bgw_procno);
		SetLatch(&ProcGlobal->allProcs[bgw_procno].procLatch);

		/*
		 * And sleep until the launched dynamic bgworker wakes us.
		 */
		elog(PEL_DEBUG, "pel_publish_queue(): sleep until the launched dynamic"
				" bgworker wakes us, procno: %d", MyProc->pgprocno);
		processed = false;
		while (!processed)
		{
			int rc;

			rc = WaitLatch(&MyProc->procLatch,
					WL_TIMEOUT | WL_LATCH_SET | WL_POSTMASTER_DEATH,
					1000,
					PG_WAIT_EXTENSION);
			ResetLatch(&MyProc->procLatch);
			elog(PEL_DEBUG, "pel_publish_queue() woke up, rc: %d"
					" (procno: %d)", rc, MyProc->pgprocno);

			/*
			 * If we're in the middle of an error interruption, interrupts will
			 * be held, so we'll have to detect a cancel or terminate pending
			 * and act accordingly.
			 */
			if (InterruptHoldoffCount > 0)
			{
				if (QueryCancelPending || ProcDiePending)
					goto error;
			}
			else
				CHECK_FOR_INTERRUPTS();

			if (rc == WL_POSTMASTER_DEATH)
				goto error;

			/* bgworker restarted */
			if (pg_atomic_read_u32(&pel->bgw_procno) != bgw_procno)
			{
				bgw_procno = pg_atomic_read_u32(&pel->bgw_procno);
				elog(PEL_DEBUG, "pel_publish_queue(): bgworker restarted"
						" (procno: %d)", bgw_procno);
				continue;
			}

			LWLockAcquire(pel->lock, LW_SHARED);
			/*
			 * Check if the item is processed.  Note that while unlikely it's
			 * possible the system consumed the whole queue entries and the
			 * entry at the given position now belongs to another backend.
			 */
			elog(PEL_DEBUG, "pel_publish_queue(): Checking if item at pos %d is processed %d (queue.pgprocno: %d / MyProc->pgprocno: %d.", pos, queue.entries[pos].processed, queue.entries[pos].pgprocno, MyProc->pgprocno);
			processed = (queue.entries[pos].processed ||
					queue.entries[pos].pgprocno != MyProc->pgprocno);
			LWLockRelease(pel->lock);

			/* There won't be any need to check for that entry anymore */
			if (processed)
				local_data.last_pos = -1;

			elog(PEL_DEBUG, "pel_publish_queue(): queue entry %d processed last_pos = %d", pos, local_data.last_pos);
		}
	}

	return pos;

error:
	elog(PEL_DEBUG, "pel_publish_queue(): error, returning invalid pos %d", PEL_PUBLISH_ERROR);
	local_data.last_pos = -1;
	return PEL_PUBLISH_ERROR;
}

int pel_queue_size(void)
{
	if(local_data.pentry == InvalidDsaPointer)
		return -1;

	return local_data.entry->num_entries;
}

Oid
pel_get_queue_item_dbid(int pos)
{
	Assert(LWLockHeldByMe(pel->lock));
	Assert(pos < PEL_QUEUE_SIZE);

	Assert(pel->pqueue != InvalidDsaPointer && queue.entries != NULL);

	return queue.entries[pos].dbid;
}

/*
 * Clean the pelLocalData corresponding to a previous call of
 * pel_get_worker_entry().
 */
void
pel_worker_entry_complete(void)
{
	int pgprocno;

	Assert(!LWLockHeldByMe(pel->lock));
	Assert(local_data.pos >= 0);

	elog(PEL_DEBUG, "pel_worker_entry_complete(): Mark the entry as processed");
	LWLockAcquire(pel->lock, LW_EXCLUSIVE);

	/* Mark the entry as processed */
	queue.entries[local_data.pos].processed = true;
	if (queue.entries[local_data.pos].notify)
		pgprocno = queue.entries[local_data.pos].pgprocno;
	else
		pgprocno = INVALID_PGPROCNO;
	LWLockRelease(pel->lock);

	elog(PEL_DEBUG, "pel_worker_entry_complete(): notify caller %d if it was required after releasing pel->lock", pgprocno);
	/* Notify caller if it was required after releasing pel->lock */
	if (pgprocno != INVALID_PGPROCNO)
		SetLatch(&ProcGlobal->allProcs[pgprocno].procLatch);

	/* Finally clean all local data structures */
	pel_cleanup_local();
}

/*
 * Return a pelWorkerEntry for the given position.
 *
 * This is done setting up pelLocalData with the entry at the given position.
 *
 * This should be called by dynamic background worker only, and
 * pel_clear_worker_entry() should be called once work is finished with the
 * given entry.
 */
pelWorkerEntry *
pel_get_worker_entry(int pos)
{
	pelWorkerEntry *result = (pelWorkerEntry *) palloc(sizeof(pelWorkerEntry));
	pelGlobalEntry global;
	int num_entries, i;

	Assert(!LWLockHeldByMe(pel->lock));
	Assert(pos < PEL_QUEUE_SIZE);

	/* We'll reattach on a specific local_data.area */
	pel_attach_dsa(true);
	Assert(pel->pqueue != InvalidDsaPointer && queue.entries != NULL);

	LWLockAcquire(pel->lock, LW_SHARED);

	global = queue.entries[pos];
	Assert(OidIsValid(global.dbid));

	Assert(global.handle != DSM_HANDLE_INVALID);
	Assert(global.pentry != InvalidDsaPointer);

	pel_setup_local_area(global.handle);
	pel_attach_entry(global.pentry);

	/* Remember the position for pel_worker_entry_complete() */
	local_data.pos = pos;

	Assert(local_data.entry->num_entries > 0);
	num_entries = local_data.entry->num_entries;

	local_data.errors = dsa_get_address(local_data.area, local_data.entry->perrors);

	/*
	 * None of the underlying dynshm should be changed until caller process the
	 * data and the bgworker release it.
	 */
	LWLockRelease(pel->lock);

	result->dbid = global.dbid;
	result->pgprocno = global.pgprocno;
	result->num_errors = num_entries;
	result->errors = (pelWorkerError *) palloc(sizeof(pelWorkerError) * num_entries);
	for (i = 0; i < num_entries; i++)
	{
		pelError *error = &local_data.errors[i];
		char *msg = dsa_get_address(local_data.area, error->perrmessage);
		char *sql = dsa_get_address(local_data.area, error->psql);
		char *query_tag;
		char *detail;
		int lenmsg;
		int lensql;
		int lenquery_tag;
		int lendetail;

		if (error->pquery_tag != InvalidDsaPointer)
		{
			query_tag = dsa_get_address(local_data.area, error->pquery_tag);
			lenquery_tag = strlen(query_tag);
		}
		else
			lenquery_tag = 0;

		if (error->pdetail != InvalidDsaPointer)
		{
			detail = dsa_get_address(local_data.area, error->pdetail);
			lendetail = strlen(detail);
		}
		else
			lendetail = 0;

		Assert(msg != NULL && sql != NULL);
		lenmsg = strlen(msg);
		lensql = strlen(sql);

		Assert(lenmsg > 0 && lensql > 0);

		result->errors[i].err_relid = error->err_relid;
		result->errors[i].sqlstate = error->sqlstate;

		result->errors[i].errmessage = (char *) palloc(lenmsg + 1);
		strncpy(result->errors[i].errmessage, msg, lenmsg);
		result->errors[i].errmessage[lenmsg] = '\0';

		result->errors[i].cmdtype = error->cmdtype;

		if (lenquery_tag > 0)
		{
			result->errors[i].query_tag = (char *) palloc(lenquery_tag + 1);
			strncpy(result->errors[i].query_tag, query_tag, lenquery_tag);
			result->errors[i].query_tag[lenquery_tag] = '\0';
		}
		else
			result->errors[i].query_tag = NULL;

		result->errors[i].sql = (char *) palloc(lensql + 1);
		strncpy(result->errors[i].sql, sql, lensql);
		result->errors[i].sql[lensql] = '\0';

		if (lendetail > 0)
		{
			result->errors[i].detail = (char *) palloc(lendetail + 1);
			strncpy(result->errors[i].detail, detail, lendetail);
			result->errors[i].detail[lendetail] = '\0';
		}
		else
			result->errors[i].detail = NULL;
	}

	return result;
}

void
pel_init_dsm(bool isMainBgworker)
{
	pel_attach_dsa(isMainBgworker);
}

bool
pel_queue_error(Oid err_relid, int sqlstate, char *errmessage, char cmdtype,
		const char *query_tag, const char *sql, const char *detail, bool sync)
{
	pelError *error;
	char *tmp;
	Size len;

	Assert(errmessage != NULL);
	Assert(sql != NULL);

	if (!pel_prepare_entry())
		return false;

	Assert(local_data.area != NULL);

	Assert(local_data.entry->num_entries < local_data.entry->max_entries);

	error = &local_data.errors[local_data.entry->num_entries];

	error->err_relid = err_relid;
	error->sqlstate = sqlstate;

	len = strlen(errmessage);
	error->perrmessage = dsa_allocate_extended(local_data.area, len + 1,
			DSA_ALLOC_NO_OOM);
	if (error->perrmessage == InvalidDsaPointer)
	{
		elog(WARNING, "pel_queue_error(): can not extend queue for %ld bytes, errmsg: %s",
									len+1, errmessage);
		pel_cleanup_local();
		return false;
	}
	tmp = dsa_get_address(local_data.area, error->perrmessage);
	memcpy(tmp, errmessage, len);
	tmp[len] = '\0';

	error->cmdtype = cmdtype;

	if (query_tag)
	{
		len = strlen(query_tag);
		error->pquery_tag = dsa_allocate_extended(local_data.area, len + 1,
				DSA_ALLOC_NO_OOM);
		if (error->pquery_tag == InvalidDsaPointer)
		{
			elog(WARNING, "pel_queue_error(): can not extend queue for %ld bytes, query_tag: %s",
									len+1, errmessage);
			pel_cleanup_local();
			return false;
		}
		tmp = dsa_get_address(local_data.area, error->pquery_tag);
		memcpy(tmp, query_tag, len);
		tmp[len] = '\0';
	}
	else
		error->pquery_tag = InvalidDsaPointer;

	len = strlen(sql);
	error->psql = dsa_allocate_extended(local_data.area, len + 1,
			DSA_ALLOC_NO_OOM);
	if (error->psql == InvalidDsaPointer)
	{
		elog(WARNING, "pel_queue_error(): can not extend queue for %ld bytes, query: %s",
									len+1, errmessage);
		pel_cleanup_local();
		return false;
	}
	tmp = dsa_get_address(local_data.area, error->psql);
	memcpy(tmp, sql, len);
	tmp[len] = '\0';

	if (detail)
	{
		len = strlen(detail);
		error->pdetail = dsa_allocate_extended(local_data.area, len + 1,
				DSA_ALLOC_NO_OOM);
		if (error->pdetail == InvalidDsaPointer)
		{
			elog(WARNING, "pel_queue_error(): can not extend queue for %ld bytes, detail: %s",
									len+1, errmessage);
			pel_cleanup_local();
			return false;
		}
		tmp = dsa_get_address(local_data.area, error->pdetail);
		memcpy(tmp, detail, len);
		tmp[len] = '\0';
	}
	else
		error->pdetail = InvalidDsaPointer;

	/* no error, make the error visible */
	local_data.entry->num_entries++;

	if (sync)
	{
		int pos = pel_publish_queue(true);
		elog(PEL_DEBUG, "pel_queue_error(): queue position returned by pel_publish_queue(): %d", pos);

		if (pos == PEL_PUBLISH_ERROR)
		{
			Assert(local_data.area == NULL);
			return false;
		}
	}

	return true;
}

#endif
