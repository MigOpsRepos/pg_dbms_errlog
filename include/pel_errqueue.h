/*-------------------------------------------------------------------------
 *
 * pel_errrqueue.h:
 * 	Implementation of error logging queue in dynshm.
 *
 * This program is open source, licensed under the PostgreSQL license.
 * For license terms, see the LICENSE file.
 *
 * Copyright (C) 2021: MigOps Inc
 *
 *-------------------------------------------------------------------------
 */

#ifndef _PEL_ERRORQUEUE_H
#define _PEL_ERRORQUEUE_H

#include "postgres.h"

#include "nodes/nodes.h"

#define PEL_PUBLISH_ERROR	-1
#define PEL_PUBLISH_EMPTY	-2

typedef struct pelWorkerError
{
	Oid			err_relid;
	int			sqlstate;
	char	   *errmessage;
	char		cmdtype;
	char	   *query_tag;
	char	   *sql;
	char	   *detail;
} pelWorkerError;

typedef struct pelWorkerEntry
{
	Oid			dbid;
	int			pgprocno;
	int			num_errors;
	pelWorkerError *errors;
} pelWorkerEntry;

Oid pel_get_queue_item_dbid(int pos);
pelWorkerEntry *pel_get_worker_entry(int pos);
void pel_worker_entry_complete(void);
void pel_init_dsm(bool isMainBgworker);
bool pel_queue_error(Oid err_relid, int sqlstate, char *errmessage,
		char cmdtype, const char *query_tag, const char *sql,
		const char *detail, bool sync);
void pel_discard_queue(void);
int pel_publish_queue(bool async);
int pel_queue_size(void);

#endif
