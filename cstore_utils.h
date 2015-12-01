/*-------------------------------------------------------------------------
 *
 * cstore_utils.h
 *
 * Type and function declarations for CStore utility functions.
 *
 * Copyright (c) 2015, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef CSTORE_UTILS_H
#define CSTORE_UTILS_H

#if PG_VERSION_NUM >= 90500
#include "common/pg_lzcompress.h"
#else
#include "utils/pg_lzcompress.h"
#endif

#if PG_VERSION_NUM >= 90500
typedef struct PGLZ_Header
{
        int32           vl_len_;                /* varlena header (do not touch directly!) */
        int32           rawsize;
} PGLZ_Header;


/* ----------
 * PGLZ_RAW_SIZE -
 *
 *              Macro to determine the uncompressed data size contained
 *              in the entry.
 * ----------
 */
#define PGLZ_RAW_SIZE(_lzdata)                  ((_lzdata)->rawsize)
#define PGLZ_SET_RAW_SIZE(_lzdata, l)           (_lzdata->rawsize = l)
#endif

bool cstore_pglz_compress(const char *source, int32 slen, PGLZ_Header *dest, const PGLZ_Strategy *strategy);
void cstore_pglz_decompress(const PGLZ_Header *source, char *dest);
#endif   /* CSTORE_UTILS_H */
