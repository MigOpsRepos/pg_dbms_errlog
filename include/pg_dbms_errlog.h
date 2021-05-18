/*-------------------------------------------------------------------------
 *
 * pg_errlog.c
 *	pg_errlog is a PostgreSQL extension that logs each failing DML query.
 *	It emulates the DBMS_ERRLOG Oracle module.
 *-------------------------------------------------------------------------
 */

#ifndef _PEL_H
#define _PEL_H

#include "postgres.h"

#include "storage/lwlock.h"
#include "utils/dsa.h"

#if PG_VERSION_NUM < 120000
#define table_open(r,l)		heap_open(r,l)
#define table_openrv(r,l)	heap_openrv(r,l)
#define table_close(r,l)	heap_close(r,l)
#endif

#define PEL_NAMESPACE_NAME "dbms_errlog"

#if PG_VERSION_NUM >= 100000

#define PEL_POS_PREV(pos)	( ((pos) == 0) ? pel->max_errs : (pos) - 1)
#define PEL_POS_NEXT(pos)	( ((pos) >= pel->max_errs) ? 0 : (pos) + 1)

#define PEL_DEBUG		(pel_debug ? WARNING : DEBUG1)

/* Global shared state */
typedef struct pelSharedState {
	int			bgw_saved_cur; /* upper limit for dynbgworker, only written in
								  the main bgworker.*/
	pg_atomic_uint32	bgw_procno;
	int			max_errs;
	LWLock	   *lock; /* protects the following fields only */
	int			LWTRANCHE_PEL;
	dsa_handle	pel_dsa_handle;
	dsa_pointer	pqueue; /* pointer to pelGlobalQueue */
	int			cur_err;
	int			bgw_err;
} pelSharedState;

/* Links to shared memory state */
extern pelSharedState *pel;
extern dsa_area *pel_area;
#endif

/* GUC variables */
extern bool pel_debug;
extern int pel_frequency;
extern int pel_max_workers;

#endif
