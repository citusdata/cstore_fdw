/*-------------------------------------------------------------------------
 *
 * cstore_utils.c
 *
 * This file contains utility functions definitions for cstore
 *
 * Copyright (c) 2015, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"
#include "cstore_fdw.h"
#include "cstore_utils.h"


/*
 * This is a wrapper function on PostgreSQL's pglz_compress
 * function, because PostgreSQL 9.5 changes the function signature.
 */
bool
cstore_pglz_compress(const char *source, int32 slen, PGLZ_Header *dest,
                          const PGLZ_Strategy *strategy)
{
#if PG_VERSION_NUM >= 90500
	StringInfoData *info = (StringInfoData*)dest;
	int	compressedLength = 0;
	char *bp = (char*)((unsigned char *) info) + sizeof(PGLZ_Header);

	compressedLength = pglz_compress(source, slen, bp, strategy);
	if (compressedLength <= 0)
		return false;

	dest->rawsize = slen;
	SET_VARSIZE_COMPRESSED(dest, compressedLength + sizeof(PGLZ_Header));
#else
	return pglz_compress(source, slen, dest, strategy);
#endif
	return true;
}


/*
 * This is a wrapper function on PostgreSQL's pglz_decompress
 * function, because PostgreSQL 9.5 changes the function prototype.
 */
void
cstore_pglz_decompress(const PGLZ_Header *source, char *dest)
{
#if PG_VERSION_NUM >= 90500
	char *sp = (char*)((const unsigned char *) source) + sizeof(PGLZ_Header);
	pglz_decompress (sp, VARSIZE(source), dest, PGLZ_RAW_SIZE(source));
#else
	pglz_decompress(source, dest);
#endif
}
