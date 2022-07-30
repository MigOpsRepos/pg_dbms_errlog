/*-------------------------------------------------------------------------
 *
 * pel_worker.h:
 * 	Implementation of bgworker for error queue processing.
 *
 * This program is open source, licensed under the PostgreSQL license.
 * For license terms, see the LICENSE file.
 *
 * Copyright (C) 2021: MigOps Inc
 *
 *-------------------------------------------------------------------------
 */

#ifndef _PEL_WORKER_H
#define _PEL_WORKER_H

#include "postgres.h"

#if PG_VERSION_NUM >= 100000
PGDLLEXPORT void pel_worker_main(Datum main_arg) pg_attribute_noreturn();
PGDLLEXPORT void pel_dynworker_main(Datum main_arg);
#endif			/* pg10 */
#endif			/* _PEL_WORKER_H */
