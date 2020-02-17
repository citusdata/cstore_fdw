/*-------------------------------------------------------------------------
 *
 * cstore_compression.c
 *
 * This file contains compression/decompression functions definitions
 * used in cstore_fdw.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cstore_fdw.h"

#if PG_VERSION_NUM >= 90500
#include "common/pg_lzcompress.h"
#else
#include "utils/pg_lzcompress.h"
#endif

#include "snappy-c.h"
#include "zlib.h"


#if PG_VERSION_NUM >= 90500
/*
 *	The information at the start of the compressed data. This decription is taken
 *	from pg_lzcompress in pre-9.5 version of PostgreSQL.
 */
typedef struct CStoreCompressHeader
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int32		rawsize;
} CStoreCompressHeader;

/*
 * Utilities for manipulation of header information for compressed data
 */

#define CSTORE_COMPRESS_HDRSZ		((int32) sizeof(CStoreCompressHeader))
#define CSTORE_COMPRESS_RAWSIZE(ptr) (((CStoreCompressHeader *) (ptr))->rawsize)
#define CSTORE_COMPRESS_RAWDATA(ptr) (((char *) (ptr)) + CSTORE_COMPRESS_HDRSZ)
#define CSTORE_COMPRESS_SET_RAWSIZE(ptr, len) (((CStoreCompressHeader *) (ptr))->rawsize = (len))

#else

#define CSTORE_COMPRESS_HDRSZ		(0)
#define CSTORE_COMPRESS_RAWSIZE(ptr) (PGLZ_RAW_SIZE((PGLZ_Header *) buffer->data))
#define CSTORE_COMPRESS_RAWDATA(ptr) (((PGLZ_Header *) (ptr)))
#define CSTORE_COMPRESS_SET_RAWSIZE(ptr, len) (((PGLZ_Header *) (ptr))->rawsize = (len))

#endif



/*
 * CompressBuffer compresses the given buffer with the given compression type
 * outputBuffer enlarged to contain compressed data. The function returns true
 * if compression is done, returns false if compression is not done.
 * outputBuffer is valid only if the function returns true.
 */
bool
CompressBuffer(StringInfo inputBuffer, StringInfo outputBuffer,
			   CompressionType compressionType)
{
	uint64 maximumLength = 0;
	bool compressionResult = false;
//#if PG_VERSION_NUM >= 90500
	int32 compressedByteCount = 0;
//#endif

	if (compressionType == COMPRESSION_PG_LZ)
	{
		maximumLength = PGLZ_MAX_OUTPUT(inputBuffer->len) + CSTORE_COMPRESS_HDRSZ;
	        resetStringInfo(outputBuffer);
	        enlargeStringInfo(outputBuffer, maximumLength);

#if PG_VERSION_NUM >= 90500
		compressedByteCount = pglz_compress((const char *) inputBuffer->data,
										inputBuffer->len,
										CSTORE_COMPRESS_RAWDATA(outputBuffer->data),
										PGLZ_strategy_always);
		if (compressedByteCount >= 0)
		{
			CSTORE_COMPRESS_SET_RAWSIZE(outputBuffer->data, inputBuffer->len);
			SET_VARSIZE_COMPRESSED(outputBuffer->data,
								   compressedByteCount + CSTORE_COMPRESS_HDRSZ);
			compressionResult = true;
		}
#else

		compressionResult = pglz_compress(inputBuffer->data, inputBuffer->len,
										  CSTORE_COMPRESS_RAWDATA(outputBuffer->data),
										  PGLZ_strategy_always);
#endif
	} else if (compressionType == COMPRESSION_SNAPPY)
	{
		maximumLength = snappy_max_compressed_length(inputBuffer->len);
	        resetStringInfo(outputBuffer);
	        enlargeStringInfo(outputBuffer, maximumLength);

		if (snappy_compress(inputBuffer->data, inputBuffer->len, (char *)CSTORE_COMPRESS_RAWDATA(outputBuffer->data), (size_t *)&compressedByteCount) == SNAPPY_OK)
		{

                	if (compressedByteCount >= 0)
	                {
	                        CSTORE_COMPRESS_SET_RAWSIZE(outputBuffer->data, inputBuffer->len);
	                        SET_VARSIZE_COMPRESSED(outputBuffer->data,
                                                                   compressedByteCount + CSTORE_COMPRESS_HDRSZ);
	                        compressionResult = true;
	                }
		} else
		{
			compressionResult = false;
		}



	} else if (compressionType == COMPRESSION_DEFLATE)
	{
		z_stream strm;

		/* allocate deflate state */
		strm.zalloc = Z_NULL;
		strm.zfree = Z_NULL;
		strm.opaque = Z_NULL;

		if (deflateInit(&strm, Z_DEFAULT_COMPRESSION) != Z_OK)
		{
                        ereport(ERROR, (errmsg("deflate cannot compress the buffer"),
                                                        errdetail("unable to initialize deflate state")));
		}
	
		/* get upper bound for compressed data and allocate buffer */	
		maximumLength = deflateBound(&strm, inputBuffer->len);
	        resetStringInfo(outputBuffer);
	        enlargeStringInfo(outputBuffer, maximumLength);

		/* set the streaming context */
		strm.avail_in = inputBuffer->len;
		strm.next_in = (Bytef *)inputBuffer->data;
		strm.avail_out = maximumLength;
		strm.next_out = (Bytef *)CSTORE_COMPRESS_RAWDATA(outputBuffer->data);

		/* do a single pass deflate compression */
		if (deflate(&strm, Z_FINISH) == Z_STREAM_END) 
		{
			if (maximumLength - strm.avail_out >= 0)
                        {
                                CSTORE_COMPRESS_SET_RAWSIZE(outputBuffer->data, inputBuffer->len);
                                SET_VARSIZE_COMPRESSED(outputBuffer->data,
                                                                   maximumLength - strm.avail_out + CSTORE_COMPRESS_HDRSZ);
                                compressionResult = true;
                        }

		} else
		{
                        compressionResult = false;
                }

		/* closes the deflate stream */
		(void)deflateEnd(&strm);
	}

	if (compressionResult)
	{
		outputBuffer->len = VARSIZE(outputBuffer->data);
	}

	return compressionResult;
}


/*
 * DecompressBuffer decompresses the given buffer with the given compression
 * type. This function returns the buffer as-is when no compression is applied.
 */
StringInfo
DecompressBuffer(StringInfo buffer, CompressionType compressionType)
{
	StringInfo decompressedBuffer = NULL;
	int32 decompressedByteCount = -1;
	char *decompressedData = NULL;
        uint32 decompressedDataSize = CSTORE_COMPRESS_RAWSIZE(buffer->data);
	uint32 compressedDataSize = VARSIZE(buffer->data) - CSTORE_COMPRESS_HDRSZ;

	Assert(compressionType == COMPRESSION_NONE || compressionType == COMPRESSION_PG_LZ ||
		compressionType == COMPRESSION_SNAPPY || compressionType == COMPRESSION_DEFLATE);

	if (compressionType == COMPRESSION_NONE)
	{
		/* in case of no compression, return buffer */
		decompressedBuffer = buffer;
	} else if (compressionType == COMPRESSION_PG_LZ)
	{
		if (compressedDataSize + CSTORE_COMPRESS_HDRSZ != buffer->len)
		{
			ereport(ERROR, (errmsg("pglz cannot decompress the buffer"),
							errdetail("expected %u bytes, but received %u bytes",
									  compressedDataSize, buffer->len)));
		}

		decompressedData = palloc0(decompressedDataSize);

#if PG_VERSION_NUM >= 90500

#if PG_VERSION_NUM >= 120000
		decompressedByteCount = pglz_decompress(CSTORE_COMPRESS_RAWDATA(buffer->data),
												compressedDataSize, decompressedData,
												decompressedDataSize, true);
#else
		decompressedByteCount = pglz_decompress(CSTORE_COMPRESS_RAWDATA(buffer->data),
												compressedDataSize, decompressedData,
												decompressedDataSize);
#endif

		if (decompressedByteCount < 0)
		{
			ereport(ERROR, (errmsg("pglz cannot decompress the buffer"),
							errdetail("compressed data is corrupted")));
		}
#else
		pglz_decompress((PGLZ_Header *) buffer->data, decompressedData);
#endif

		decompressedBuffer = palloc0(sizeof(StringInfoData));
		decompressedBuffer->data = decompressedData;
		decompressedBuffer->len = decompressedDataSize;
		decompressedBuffer->maxlen = decompressedDataSize;
	} else if (compressionType == COMPRESSION_SNAPPY) 
	{
		decompressedData = palloc0(decompressedDataSize);

		if (snappy_uncompress(CSTORE_COMPRESS_RAWDATA(buffer->data), compressedDataSize, decompressedData, (size_t *)&decompressedByteCount) != SNAPPY_OK)
		{
			ereport(ERROR, (errmsg("snappy cannot decompress the buffer"),
							errdetail("compressed data is corrupted")));
		}
		if (decompressedByteCount < 0)
		{
			ereport(ERROR, (errmsg("snappy cannot decompress the buffer"),
							errdetail("count less than zero")));
		}

		decompressedBuffer = palloc0(sizeof(StringInfoData));
		decompressedBuffer->data = decompressedData;
		decompressedBuffer->len = decompressedDataSize;
		decompressedBuffer->maxlen = decompressedDataSize;

	} else if (compressionType == COMPRESSION_DEFLATE) 
	{
		z_stream strm;

		/* allocate deflate state */
		strm.zalloc = Z_NULL;
		strm.zfree = Z_NULL;
		strm.opaque = Z_NULL;

		if (inflateInit(&strm) != Z_OK)
		{
                        ereport(ERROR, (errmsg("inflate cannot decompress the buffer"),
                                                        errdetail("unable to initialize inflate state")));
		}
	
                decompressedData = palloc0(decompressedDataSize);

		/* set the streaming context */
		strm.avail_in = compressedDataSize;
		strm.next_in = (Bytef *)CSTORE_COMPRESS_RAWDATA(buffer->data);
		strm.avail_out = decompressedDataSize;
		strm.next_out = (Bytef *)decompressedData;

		/* do a single pass inflate decompression */
		if (inflate(&strm, Z_FINISH) != Z_STREAM_END) 
		{
                        ereport(ERROR, (errmsg("inflate cannot decompress the buffer"),
                                                        errdetail("data is corrupted")));
		}

		decompressedBuffer = palloc0(sizeof(StringInfoData));
		decompressedBuffer->data = decompressedData;
		decompressedBuffer->len = decompressedDataSize;
		decompressedBuffer->maxlen = decompressedDataSize;

		/* closes the inflate stream */
		(void)inflateEnd(&strm);
	} 

	return decompressedBuffer;
}
