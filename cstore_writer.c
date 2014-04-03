/*-------------------------------------------------------------------------
 *
 * cstore_writer.c
 *
 * This file contains function definitions for writing cstore files. This
 * includes the logic for writing file level metadata, writing row stripes,
 * and calculating block skip nodes.
 *
 * Copyright (c) 2014, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"
#include "cstore_fdw.h"
#include "cstore_metadata_serialization.h"

#include <sys/stat.h>
#include "access/nbtree.h"
#include "catalog/pg_collation.h"
#include "commands/defrem.h"
#include "optimizer/var.h"
#include "port.h"
#include "storage/fd.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/pg_lzcompress.h"
#include "utils/rel.h"


static void CStoreWriteFooter(StringInfo footerFileName, TableFooter *tableFooter);
static StripeData * CreateEmptyStripeData(uint32 stripeMaxRowCount, uint32 blockRowCount,
										  uint32 columnCount);
static StripeSkipList * CreateEmptyStripeSkipList(uint32 stripeMaxRowCount,
												  uint32 blockRowCount,
												  uint32 columnCount);
static StripeMetadata FlushStripe(TableWriteState *writeState);
static StringInfo ** CreateExistsBufferArray(ColumnData **columnDataArray,
											 StripeSkipList *stripeSkipList);
static StringInfo ** CreateValueBufferArray(ColumnData **columnDataArray,
											StripeSkipList *stripeSkipList,
											TupleDesc tupleDescriptor);
static StringInfo * CreateSkipListBufferArray(StripeSkipList *stripeSkipList,
											  TupleDesc tupleDescriptor);
static StripeFooter * CreateStripeFooter(StripeSkipList *stripeSkipList,
										 StringInfo *skipListBufferArray);
static StringInfo SerializeBoolArray(bool *boolArray, uint32 boolArrayLength);
static StringInfo SerializeDatumArray(Datum *datumArray, bool *existsArray,
									  uint32 datumCount, bool datumTypeByValue,
									  int datumTypeLength, char datumTypeAlign);
static void UpdateBlockSkipNodeMinMax(ColumnBlockSkipNode *blockSkipNode,
									  Datum columnValue, bool columnTypeByValue,
									  int columnTypeLength, Oid columnCollation,
									  FmgrInfo *comparisonFunction);
static Datum DatumCopy(Datum datum, bool datumTypeByValue, int datumTypeLength);
static void AppendStripeMetadata(TableFooter *tableFooter,
								 StripeMetadata stripeMetadata);
static void WriteToFile(FILE *file, void *data, uint32 dataLength);
static void SyncAndCloseFile(FILE *file);


/*
 * CStoreBeginWrite initializes a cstore data load operation and returns a table
 * handle. This handle should be used for adding the row values and finishing the
 * data load operation. If the cstore footer file already exists, we read the
 * footer and then seek to right after the last stripe  where the new stripes
 * will be added.
 */
TableWriteState *
CStoreBeginWrite(const char *filename, CompressionType compressionType,
				 uint64 stripeMaxRowCount, uint32 blockRowCount,
				 TupleDesc tupleDescriptor)
{
	TableWriteState *writeState = NULL;
	FILE *tableFile = NULL;
	StringInfo tableFooterFilename = NULL;
	TableFooter *tableFooter = NULL;
	FmgrInfo **comparisonFunctionArray = NULL;
	MemoryContext stripeWriteContext = NULL;
	uint64 currentFileOffset = 0;
	uint32 columnCount = 0;
	uint32 columnIndex = 0;
	struct stat statBuffer;
	int statResult = 0;

	tableFooterFilename = makeStringInfo();
	appendStringInfo(tableFooterFilename, "%s%s", filename, CSTORE_FOOTER_FILE_SUFFIX);

	statResult = stat(tableFooterFilename->data, &statBuffer);
	if (statResult < 0)
	{
		tableFile = AllocateFile(filename, "w");
		if (tableFile == NULL)
		{
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not open file \"%s\" for writing: %m",
								   filename)));
		}

		tableFooter = palloc0(sizeof(TableFooter));
		tableFooter->blockRowCount = blockRowCount;
		tableFooter->stripeMetadataList = NIL;
	}
	else
	{
		tableFile = AllocateFile(filename, "r+");
		if (tableFile == NULL)
		{
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not open file \"%s\" for writing: %m",
								   filename)));
		}

		tableFooter = CStoreReadFooter(tableFooterFilename);
	}

	/*
	 * If stripeMetadataList is not empty, jump to the position right after
	 * the last position.
	 */
	if (tableFooter->stripeMetadataList != NIL)
	{
		StripeMetadata *lastStripe = NULL;
		uint64 lastStripeSize = 0;
		int fseekResult = 0;

		lastStripe = llast(tableFooter->stripeMetadataList);
		lastStripeSize += lastStripe->skipListLength;
		lastStripeSize += lastStripe->dataLength;
		lastStripeSize += lastStripe->footerLength;

		currentFileOffset = lastStripe->fileOffset + lastStripeSize;

		errno = 0;
		fseekResult = fseeko(tableFile, currentFileOffset, SEEK_SET);
		if (fseekResult != 0)
		{
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not seek in file \"%s\": %m", filename)));
		}
	}

	/* get comparison function pointers for each of the columns */
	columnCount = tupleDescriptor->natts;
	comparisonFunctionArray = palloc0(columnCount * sizeof(FmgrInfo *));
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		Oid typeId = tupleDescriptor->attrs[columnIndex]->atttypid;
		FmgrInfo *comparisonFunction = GetFunctionInfoOrNull(typeId, BTREE_AM_OID,
															 BTORDER_PROC);
		comparisonFunctionArray[columnIndex] = comparisonFunction;
	}

	/*
	 * We allocate all stripe specific data in the stripeWriteContext, and
	 * reset this memory context once we have flushed the stripe to the file.
	 * This is to avoid memory leaks.
	 */
	stripeWriteContext = AllocSetContextCreate(CurrentMemoryContext,
											   "Stripe Write Memory Context",
											   ALLOCSET_DEFAULT_MINSIZE,
											   ALLOCSET_DEFAULT_INITSIZE,
											   ALLOCSET_DEFAULT_MAXSIZE);

	writeState = palloc0(sizeof(TableWriteState));
	writeState->tableFile = tableFile;
	writeState->tableFooterFilename = tableFooterFilename;
	writeState->tableFooter = tableFooter;
	writeState->compressionType = compressionType;
	writeState->stripeMaxRowCount = stripeMaxRowCount;
	writeState->tupleDescriptor = tupleDescriptor;
	writeState->currentFileOffset = currentFileOffset;
	writeState->comparisonFunctionArray = comparisonFunctionArray;
	writeState->stripeData = NULL;
	writeState->stripeSkipList = NULL;
	writeState->stripeWriteContext = stripeWriteContext;

	return writeState;
}


/*
 * CStoreWriteRow adds a row to the cstore file. If the stripe is not initialized,
 * we create structures to hold stripe data and skip list. Then, we add data for
 * each of the columns and update corresponding skip nodes. Then, if row count
 * exceeds stripeMaxRowCount, we flush the stripe, and add its metadata to the
 * table footer.
 */
void
CStoreWriteRow(TableWriteState *writeState, Datum *columnValues, bool *columnNulls)
{
	uint32 columnIndex = 0;
	uint32 blockIndex = 0;
	uint32 blockRowIndex = 0;
	StripeData *stripeData = writeState->stripeData;
	StripeSkipList *stripeSkipList = writeState->stripeSkipList;
	uint32 columnCount = writeState->tupleDescriptor->natts;
	TableFooter *tableFooter = writeState->tableFooter;
	const uint32 blockRowCount = tableFooter->blockRowCount;

	MemoryContext oldContext = MemoryContextSwitchTo(writeState->stripeWriteContext);

	if (stripeData == NULL)
	{
		stripeData = CreateEmptyStripeData(writeState->stripeMaxRowCount,
										   blockRowCount, columnCount);
		stripeSkipList = CreateEmptyStripeSkipList(writeState->stripeMaxRowCount,
												   blockRowCount, columnCount);
		writeState->stripeData = stripeData;
		writeState->stripeSkipList = stripeSkipList;
	}

	blockIndex = stripeData->rowCount / blockRowCount;
	blockRowIndex = stripeData->rowCount % blockRowCount;

	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		ColumnData *columnData = stripeData->columnDataArray[columnIndex];
		ColumnBlockData *blockData = columnData->blockDataArray[blockIndex];
		ColumnBlockSkipNode **blockSkipNodeArray = stripeSkipList->blockSkipNodeArray;
		ColumnBlockSkipNode *blockSkipNode =
			&blockSkipNodeArray[columnIndex][blockIndex];

		if (columnNulls[columnIndex])
		{
			blockData->existsArray[blockRowIndex] = false;
		}
		else
		{
			FmgrInfo *comparisonFunction =
				writeState->comparisonFunctionArray[columnIndex];
			Form_pg_attribute attributeForm =
				writeState->tupleDescriptor->attrs[columnIndex];
			bool columnTypeByValue = attributeForm->attbyval;
			int columnTypeLength = attributeForm->attlen;
			Oid columnCollation = attributeForm->attcollation;

			blockData->existsArray[blockRowIndex] = true;
			blockData->valueArray[blockRowIndex] = DatumCopy(columnValues[columnIndex],
															 columnTypeByValue,
															 columnTypeLength);

			UpdateBlockSkipNodeMinMax(blockSkipNode, columnValues[columnIndex],
									  columnTypeByValue, columnTypeLength,
									  columnCollation, comparisonFunction);
		}

		blockSkipNode->rowCount++;
	}

	stripeSkipList->blockCount = blockIndex + 1;
	stripeData->rowCount++;
	if (stripeData->rowCount >= writeState->stripeMaxRowCount)
	{
		StripeMetadata stripeMetadata = FlushStripe(writeState);
		MemoryContextReset(writeState->stripeWriteContext);

		/* set stripe data and skip list to NULL so they are recreated next time */
		writeState->stripeData = NULL;
		writeState->stripeSkipList = NULL;

		/*
		 * Append stripeMetadata in old context so next MemoryContextReset
		 * doesn't free it.
		 */
		MemoryContextSwitchTo(oldContext);
		AppendStripeMetadata(tableFooter, stripeMetadata);
	}
	else
	{
		MemoryContextSwitchTo(oldContext);
	}
}


/*
 * CStoreEndWrite finishes a cstore data load operation. If we have an unflushed
 * stripe, we flush it. Then, we sync and close the cstore data file. Last, we
 * flush the footer to a temporary file, and atomically rename this temporary
 * file to the original footer file.
 */
void
CStoreEndWrite(TableWriteState *writeState)
{
	StringInfo tableFooterFilename = NULL;
	StringInfo tempTableFooterFileName = NULL;
	int renameResult = 0;

	StripeData *stripeData = writeState->stripeData;
	if (stripeData != NULL)
	{
		MemoryContext oldContext = MemoryContextSwitchTo(writeState->stripeWriteContext);

		StripeMetadata stripeMetadata = FlushStripe(writeState);
		MemoryContextReset(writeState->stripeWriteContext);

		MemoryContextSwitchTo(oldContext);
		AppendStripeMetadata(writeState->tableFooter, stripeMetadata);
	}

	SyncAndCloseFile(writeState->tableFile);

	tableFooterFilename = writeState->tableFooterFilename;
	tempTableFooterFileName = makeStringInfo();
	appendStringInfo(tempTableFooterFileName, "%s%s", tableFooterFilename->data,
					 CSTORE_TEMP_FILE_SUFFIX);

	CStoreWriteFooter(tempTableFooterFileName, writeState->tableFooter);

	renameResult = rename(tempTableFooterFileName->data, tableFooterFilename->data);
	if (renameResult != 0)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not rename file \"%s\" to \"%s\": %m",
							   tempTableFooterFileName->data,
							   tableFooterFilename->data)));
	}

	pfree(tempTableFooterFileName->data);
	pfree(tempTableFooterFileName);

	MemoryContextDelete(writeState->stripeWriteContext);
	list_free_deep(writeState->tableFooter->stripeMetadataList);
	pfree(writeState->tableFooter);
	pfree(writeState->tableFooterFilename->data);
	pfree(writeState->tableFooterFilename);
	pfree(writeState->comparisonFunctionArray);
	pfree(writeState);
}


/*
 * CStoreWriteFooter writes the given footer to given file. First, the function
 * serializes and writes the footer to the file. Then, the function serializes
 * and writes the postscript. Then, the function writes the postscript size as
 * the last byte of the file. Last, the function syncs and closes the footer file.
 */
static void
CStoreWriteFooter(StringInfo tableFooterFilename, TableFooter *tableFooter)
{
	FILE *tableFooterFile = NULL;
	StringInfo tableFooterBuffer = NULL;
	StringInfo postscriptBuffer = NULL;
	uint8 postscriptSize = 0;

	tableFooterFile = AllocateFile(tableFooterFilename->data, PG_BINARY_W);
	if (tableFooterFile == NULL)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not open file \"%s\" for writing: %m",
							   tableFooterFilename->data)));
	}

	/* write the footer */
	tableFooterBuffer = SerializeTableFooter(tableFooter);
	WriteToFile(tableFooterFile, tableFooterBuffer->data, tableFooterBuffer->len);

	/* write the postscript */
	postscriptBuffer = SerializePostScript(tableFooterBuffer->len);
	WriteToFile(tableFooterFile, postscriptBuffer->data, postscriptBuffer->len);

	/* write the 1-byte postscript size */
	Assert(postscriptBuffer->len < CSTORE_POSTSCRIPT_SIZE_MAX);
	postscriptSize = postscriptBuffer->len;
	WriteToFile(tableFooterFile, &postscriptSize, CSTORE_POSTSCRIPT_SIZE_LENGTH);

	SyncAndCloseFile(tableFooterFile);

	pfree(tableFooterBuffer->data);
	pfree(tableFooterBuffer);
	pfree(postscriptBuffer->data);
	pfree(postscriptBuffer);
}


/*
 * CreateEmptyStripeData allocates an empty StripeData structure with the given
 * column count. This structure has enough capacity to hold stripeMaxRowCount rows.
 */
static StripeData *
CreateEmptyStripeData(uint32 stripeMaxRowCount, uint32 blockRowCount,
					  uint32 columnCount)
{
	StripeData *stripeData = NULL;
	uint32 columnIndex = 0;
	uint32 maxBlockCount = (stripeMaxRowCount / blockRowCount) + 1;

	ColumnData **columnDataArray = palloc0(columnCount * sizeof(ColumnData *));
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		uint32 blockIndex = 0;
		ColumnBlockData **blockDataArray = NULL;

		blockDataArray = palloc0(maxBlockCount * sizeof(ColumnBlockData *));
		for (blockIndex = 0; blockIndex < maxBlockCount; blockIndex++)
		{
			bool *existsArray = palloc0(blockRowCount * sizeof(bool));
			Datum *valueArray = palloc0(blockRowCount * sizeof(Datum));

			blockDataArray[blockIndex] = palloc0(sizeof(ColumnBlockData));
			blockDataArray[blockIndex]->existsArray = existsArray;
			blockDataArray[blockIndex]->valueArray = valueArray;
		}

		columnDataArray[columnIndex] = palloc0(sizeof(ColumnData));
		columnDataArray[columnIndex]->blockDataArray = blockDataArray;
	}

	stripeData = palloc0(sizeof(StripeData));
	stripeData->columnDataArray = columnDataArray;
	stripeData->columnCount = columnCount;
	stripeData->rowCount = 0;

	return stripeData;
}


/*
 * CreateEmptyStripeSkipList allocates an empty StripeSkipList structure with
 * the given column count. This structure has enough blocks to hold statistics
 * for stripeMaxRowCount rows.
 */
static StripeSkipList *
CreateEmptyStripeSkipList(uint32 stripeMaxRowCount, uint32 blockRowCount,
						  uint32 columnCount)
{
	StripeSkipList *stripeSkipList = NULL;
	uint32 columnIndex = 0;
	uint32 maxBlockCount = (stripeMaxRowCount / blockRowCount) + 1;

	ColumnBlockSkipNode **blockSkipNodeArray =
		palloc0(columnCount * sizeof(ColumnBlockSkipNode *));
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		blockSkipNodeArray[columnIndex] =
			palloc0(maxBlockCount * sizeof(ColumnBlockSkipNode));
	}

	stripeSkipList = palloc0(sizeof(StripeSkipList));
	stripeSkipList->columnCount = columnCount;
	stripeSkipList->blockCount = 0;
	stripeSkipList->blockSkipNodeArray = blockSkipNodeArray;

	return stripeSkipList;
}


/*
 * FlushStripe compresses the data in the current stripe, flushes the compressed
 * data into the file, and returns the stripe metadata. To do this, the function
 * first creates the data buffers, and then updates position and length statistics
 * in stripe's skip list. Then, the function creates the skip list and footer
 * buffers. Finally, the function flushes the skip list, data, and footer buffers
 * to the file.
 */
static StripeMetadata
FlushStripe(TableWriteState *writeState)
{
	StripeMetadata stripeMetadata = {0, 0, 0, 0};
	uint64 skipListLength = 0;
	uint64 dataLength = 0;
	StringInfo **existsBufferArray = NULL;
	StringInfo **valueBufferArray = NULL;
	CompressionType **valueCompressionTypeArray = NULL;
	StringInfo *skipListBufferArray = NULL;
	StripeFooter *stripeFooter = NULL;
	StringInfo stripeFooterBuffer = NULL;
	uint32 columnIndex = 0;
	uint32 blockIndex = 0;

	FILE *tableFile = writeState->tableFile;
	StripeData *stripeData = writeState->stripeData;
	StripeSkipList *stripeSkipList = writeState->stripeSkipList;
	CompressionType compressionType = writeState->compressionType;
	TupleDesc tupleDescriptor = writeState->tupleDescriptor;
	uint32 columnCount = tupleDescriptor->natts;
	uint32 blockCount = stripeSkipList->blockCount;

	/* create "exists" and "value" buffers */
	existsBufferArray = CreateExistsBufferArray(stripeData->columnDataArray,
												stripeSkipList);
	valueBufferArray = CreateValueBufferArray(stripeData->columnDataArray,
											  stripeSkipList, tupleDescriptor);

	valueCompressionTypeArray = palloc0(columnCount * sizeof(CompressionType *));
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		CompressionType *blockCompressionTypeArray =
			palloc0(blockCount * sizeof(CompressionType));
		valueCompressionTypeArray[columnIndex] = blockCompressionTypeArray;

		for (blockIndex = 0; blockIndex < blockCount; blockIndex++)
		{
			StringInfo valueBuffer = NULL;
			uint64 maximumLength = 0;
			PGLZ_Header *compressedData = NULL;
			bool compressable = false;

			if (compressionType == COMPRESSION_NONE)
			{
				blockCompressionTypeArray[blockIndex] = COMPRESSION_NONE;
				continue;
			}

			/* the only other supported compression type is pg_lz for now */
			Assert(compressionType == COMPRESSION_PG_LZ);

			valueBuffer = valueBufferArray[columnIndex][blockIndex];
			maximumLength = PGLZ_MAX_OUTPUT(valueBuffer->len);
			compressedData = palloc0(maximumLength);
			compressable = pglz_compress((const char *) valueBuffer->data,
										 valueBuffer->len, compressedData,
										 PGLZ_strategy_always);
			if (compressable)
			{
				pfree(valueBuffer->data);

				valueBuffer->data = (char *) compressedData;
				valueBuffer->len = VARSIZE(compressedData);
				valueBuffer->maxlen = maximumLength;

				blockCompressionTypeArray[blockIndex] = COMPRESSION_PG_LZ;
			}
			else
			{
				pfree(compressedData);
				blockCompressionTypeArray[blockIndex] = COMPRESSION_NONE;
			}
		}
	}

	/* update buffer sizes and positions in stripe skip list */
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		ColumnBlockSkipNode **columnSkipNodeArray = stripeSkipList->blockSkipNodeArray;
		ColumnBlockSkipNode *blockSkipNodeArray = columnSkipNodeArray[columnIndex];
		uint32 blockCount = stripeSkipList->blockCount;
		uint32 blockIndex = 0;
		uint64 currentExistsBlockOffset = 0;
		uint64 currentValueBlockOffset = 0;

		for (blockIndex = 0; blockIndex < blockCount; blockIndex++)
		{
			uint64 existsBufferSize = existsBufferArray[columnIndex][blockIndex]->len;
			uint64 valueBufferSize = valueBufferArray[columnIndex][blockIndex]->len;
			CompressionType valueCompressionType =
				valueCompressionTypeArray[columnIndex][blockIndex];
			ColumnBlockSkipNode *blockSkipNode = &blockSkipNodeArray[blockIndex];

			blockSkipNode->existsBlockOffset = currentExistsBlockOffset;
			blockSkipNode->existsLength = existsBufferSize;
			blockSkipNode->valueBlockOffset = currentValueBlockOffset;
			blockSkipNode->valueLength = valueBufferSize;
			blockSkipNode->valueCompressionType = valueCompressionType;

			currentExistsBlockOffset += existsBufferSize;
			currentValueBlockOffset += valueBufferSize;
		}
	}

	/* create skip list and footer buffers */
	skipListBufferArray = CreateSkipListBufferArray(stripeSkipList, tupleDescriptor);
	stripeFooter = CreateStripeFooter(stripeSkipList, skipListBufferArray);
	stripeFooterBuffer = SerializeStripeFooter(stripeFooter);

	/*
	 * Each stripe has three sections:
	 * (1) Skip list, which contains statistics for each column block, and can
	 * be used to skip reading row blocks that are refuted by WHERE clause list,
	 * (2) Data section, in which we store data for each column continuously.
	 * We store data for each for each column in blocks. For each block, we
	 * store two buffers: "exists" buffer, and "value" buffer. "exists" buffer
	 * tells which values are not NULL. "value" buffer contains values for
	 * present values. For each column, we first store all "exists" buffers,
	 * and then all "value" buffers.
	 * (3) Stripe footer, which contains the skip list buffer size, exists buffer
	 * size, and value buffer size for each of the columns.
	 *
	 * We start by flushing the skip list buffers.
	 */
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		StringInfo skipListBuffer = skipListBufferArray[columnIndex];
		WriteToFile(tableFile, skipListBuffer->data, skipListBuffer->len);
	}

	/* then, we flush the data buffers */
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		uint32 blockIndex = 0;
		for (blockIndex = 0; blockIndex < stripeSkipList->blockCount; blockIndex++)
		{
			StringInfo existsBuffer = existsBufferArray[columnIndex][blockIndex];
			WriteToFile(tableFile, existsBuffer->data, existsBuffer->len);
		}

		for (blockIndex = 0; blockIndex < stripeSkipList->blockCount; blockIndex++)
		{
			StringInfo valueBuffer = valueBufferArray[columnIndex][blockIndex];
			WriteToFile(tableFile, valueBuffer->data, valueBuffer->len);
		}
	}

	/* finally, we flush the footer buffer */
	WriteToFile(tableFile, stripeFooterBuffer->data, stripeFooterBuffer->len);

	/* set stripe metadata */
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		skipListLength += stripeFooter->skipListSizeArray[columnIndex];
		dataLength += stripeFooter->existsSizeArray[columnIndex];
		dataLength += stripeFooter->valueSizeArray[columnIndex];
	}

	stripeMetadata.fileOffset = writeState->currentFileOffset;
	stripeMetadata.skipListLength = skipListLength;
	stripeMetadata.dataLength = dataLength;
	stripeMetadata.footerLength = stripeFooterBuffer->len;

	/* advance current file offset */
	writeState->currentFileOffset += skipListLength;
	writeState->currentFileOffset += dataLength;
	writeState->currentFileOffset += stripeFooterBuffer->len;

	return stripeMetadata;
}


/*
 * CreateExistsBufferArray serializes  the "exists" arrays of stripe data for
 * each columnIndex and blockIndex combination and returns the result as a two
 * dimensional array.
 */
static StringInfo **
CreateExistsBufferArray(ColumnData **columnDataArray,
						StripeSkipList *stripeSkipList)
{
	StringInfo **existsBufferArray = NULL;
	uint32 columnIndex = 0;
	uint32 columnCount = stripeSkipList->columnCount;
	uint32 blockCount = stripeSkipList->blockCount;
	ColumnBlockSkipNode **blockSkipNodeArray = stripeSkipList->blockSkipNodeArray;

	existsBufferArray = palloc0(columnCount * sizeof(StringInfo *));
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		uint32 blockIndex = 0;

		existsBufferArray[columnIndex] = palloc0(blockCount * sizeof(StringInfo));
		for (blockIndex = 0; blockIndex < blockCount; blockIndex++)
		{
			ColumnData *columnData = columnDataArray[columnIndex];
			ColumnBlockData *blockData = columnData->blockDataArray[blockIndex];
			ColumnBlockSkipNode *blockSkipNode =
				&blockSkipNodeArray[columnIndex][blockIndex];

			StringInfo existsBuffer = SerializeBoolArray(blockData->existsArray,
														 blockSkipNode->rowCount);

			existsBufferArray[columnIndex][blockIndex] = existsBuffer;
		}
	}

	return existsBufferArray;
}


/*
 * CreateValueBufferArray serializes the "values" arrays of stripe data for
 * each columnIndex and blockIndex combination and returns the result as a
 * two dimensional array.
 */
static StringInfo **
CreateValueBufferArray(ColumnData **columnDataArray,
					   StripeSkipList *stripeSkipList,
					   TupleDesc tupleDescriptor)
{
	StringInfo **valueBufferArray = NULL;
	uint32 columnIndex = 0;
	uint32 columnCount = stripeSkipList->columnCount;
	uint32 blockCount = stripeSkipList->blockCount;
	ColumnBlockSkipNode **blockSkipNodeArray = stripeSkipList->blockSkipNodeArray;

	valueBufferArray = palloc0(columnCount * sizeof(StringInfo *));
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		Form_pg_attribute attributeForm = tupleDescriptor->attrs[columnIndex];
		uint32 blockIndex = 0;

		valueBufferArray[columnIndex] = palloc0(blockCount * sizeof(StringInfo));
		for (blockIndex = 0; blockIndex < blockCount; blockIndex++)
		{
			ColumnData *columnData = columnDataArray[columnIndex];
			ColumnBlockData *blockData = columnData->blockDataArray[blockIndex];
			ColumnBlockSkipNode *blockSkipNode =
				&blockSkipNodeArray[columnIndex][blockIndex];

			StringInfo valueBuffer = SerializeDatumArray(blockData->valueArray,
														 blockData->existsArray,
														 blockSkipNode->rowCount,
														 attributeForm->attbyval,
														 attributeForm->attlen,
														 attributeForm->attalign);

			valueBufferArray[columnIndex][blockIndex] = valueBuffer;
		}
	}

	return valueBufferArray;
}


/*
 * CreateSkipListBufferArray serializes the skip list for each column of the
 * given stripe and returns the result as an array.
 */
static StringInfo *
CreateSkipListBufferArray(StripeSkipList *stripeSkipList, TupleDesc tupleDescriptor)
{
	StringInfo *skipListBufferArray = NULL;
	uint32 columnIndex = 0;
	uint32 columnCount = stripeSkipList->columnCount;

	skipListBufferArray = palloc0(columnCount * sizeof(StringInfo));
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		StringInfo skipListBuffer = NULL;
		ColumnBlockSkipNode *blockSkipNodeArray =
			stripeSkipList->blockSkipNodeArray[columnIndex];
		Form_pg_attribute attributeForm = tupleDescriptor->attrs[columnIndex];

		skipListBuffer = SerializeColumnSkipList(blockSkipNodeArray,
												 stripeSkipList->blockCount,
												 attributeForm->attbyval,
												 attributeForm->attlen);

		skipListBufferArray[columnIndex] = skipListBuffer;
	}

	return skipListBufferArray;
}


/* Creates and returns the footer for given stripe. */
static StripeFooter *
CreateStripeFooter(StripeSkipList *stripeSkipList, StringInfo *skipListBufferArray)
{
	StripeFooter *stripeFooter = NULL;
	uint32 columnIndex = 0;
	uint32 columnCount = stripeSkipList->columnCount;
	uint64 *skipListSizeArray = palloc0(columnCount * sizeof(uint64));
	uint64 *existsSizeArray = palloc0(columnCount * sizeof(uint64));
	uint64 *valueSizeArray = palloc0(columnCount * sizeof(uint64));

	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		ColumnBlockSkipNode *blockSkipNodeArray =
			stripeSkipList->blockSkipNodeArray[columnIndex];
		uint32 blockIndex = 0;

		for (blockIndex = 0; blockIndex < stripeSkipList->blockCount; blockIndex++)
		{
			existsSizeArray[columnIndex] += blockSkipNodeArray[blockIndex].existsLength;
			valueSizeArray[columnIndex] += blockSkipNodeArray[blockIndex].valueLength;
		}
		skipListSizeArray[columnIndex] = skipListBufferArray[columnIndex]->len;
	}

	stripeFooter = palloc0(sizeof(StripeFooter));
	stripeFooter->columnCount = columnCount;
	stripeFooter->skipListSizeArray = skipListSizeArray;
	stripeFooter->existsSizeArray = existsSizeArray;
	stripeFooter->valueSizeArray = valueSizeArray;

	return stripeFooter;
}


/*
 * SerializeBoolArray serializes the given boolean array and returns the result
 * as a StringInfo. This function packs every 8 boolean values into one byte.
 */
static StringInfo
SerializeBoolArray(bool *boolArray, uint32 boolArrayLength)
{
	StringInfo boolArrayBuffer = NULL;
	uint32 boolArrayIndex = 0;
	uint32 byteCount = (boolArrayLength + 7) / 8;

	boolArrayBuffer = makeStringInfo();
	enlargeStringInfo(boolArrayBuffer, byteCount);
	boolArrayBuffer->len = byteCount;
	memset(boolArrayBuffer->data, 0, byteCount);

	for (boolArrayIndex = 0; boolArrayIndex < boolArrayLength; boolArrayIndex++)
	{
		if (boolArray[boolArrayIndex])
		{
			uint32 byteIndex = boolArrayIndex / 8;
			uint32 bitIndex = boolArrayIndex % 8;
			boolArrayBuffer->data[byteIndex] |= (1 << bitIndex);
		}
	}

	return boolArrayBuffer;
}


/*
 * SerializeDatumArray serializes the non-null elements of the given datum array
 * into a string info buffer, and then returns this buffer.
 */
static StringInfo
SerializeDatumArray(Datum *datumArray, bool *existsArray, uint32 datumCount,
					bool datumTypeByValue, int datumTypeLength, char datumTypeAlign)
{
	StringInfo datumBuffer = makeStringInfo();
	uint32 datumIndex = 0;

	for (datumIndex = 0; datumIndex < datumCount; datumIndex++)
	{
		Datum datum = datumArray[datumIndex];
		uint32 datumLength = 0;
		uint32 datumLengthAligned = 0;
		char *currentDatumDataPointer = NULL;

		if (!existsArray[datumIndex])
		{
			continue;
		}

		datumLength = att_addlength_datum(0, datumTypeLength, datum);
		datumLengthAligned = att_align_nominal(datumLength, datumTypeAlign);

		enlargeStringInfo(datumBuffer, datumBuffer->len + datumLengthAligned);
		currentDatumDataPointer = datumBuffer->data + datumBuffer->len;
		memset(currentDatumDataPointer, 0, datumLengthAligned);

		if (datumTypeLength > 0)
		{
			if (datumTypeByValue)
			{
				store_att_byval(currentDatumDataPointer, datum, datumTypeLength);
			}
			else
			{
				memcpy(currentDatumDataPointer, DatumGetPointer(datum),
					   datumTypeLength);
			}
		}
		else
		{
			Assert(!datumTypeByValue);
			memcpy(currentDatumDataPointer, DatumGetPointer(datum), datumLength);
		}

		datumBuffer->len += datumLengthAligned;
	}

	return datumBuffer;
}


/*
 * UpdateBlockSkipNodeMinMax takes the given column value, and checks if this
 * value falls outside the range of minimum/maximum values of the given column
 * block skip node. If it does, the function updates the column block skip node
 * accordingly.
 */
static void
UpdateBlockSkipNodeMinMax(ColumnBlockSkipNode *blockSkipNode, Datum columnValue,
						  bool columnTypeByValue, int columnTypeLength,
						  Oid columnCollation, FmgrInfo *comparisonFunction)
{
	bool hasMinMax = blockSkipNode->hasMinMax;
	Datum previousMinimum = blockSkipNode->minimumValue;
	Datum previousMaximum = blockSkipNode->maximumValue;
	Datum currentMinimum = 0;
	Datum currentMaximum = 0;

	/* if type doesn't have a comparison function, skip min/max values */
	if (comparisonFunction == NULL)
	{
		return;
	}

	if (!hasMinMax)
	{
		currentMinimum = DatumCopy(columnValue, columnTypeByValue, columnTypeLength);
		currentMaximum = DatumCopy(columnValue, columnTypeByValue, columnTypeLength);
	}
	else
	{
		Datum minimumComparisonDatum = FunctionCall2Coll(comparisonFunction,
														 columnCollation, columnValue,
														 previousMinimum);
		Datum maximumComparisonDatum = FunctionCall2Coll(comparisonFunction,
														 columnCollation, columnValue,
														 previousMaximum);
		int minimumComparison = DatumGetInt32(minimumComparisonDatum);
		int maximumComparison = DatumGetInt32(maximumComparisonDatum);

		if (minimumComparison < 0)
		{
			currentMinimum = DatumCopy(columnValue, columnTypeByValue, columnTypeLength);
		}
		else
		{
			currentMinimum = previousMinimum;
		}

		if (maximumComparison > 0)
		{
			currentMaximum = DatumCopy(columnValue, columnTypeByValue, columnTypeLength);
		}
		else
		{
			currentMaximum = previousMaximum;
		}
	}

	blockSkipNode->hasMinMax = true;
	blockSkipNode->minimumValue = currentMinimum;
	blockSkipNode->maximumValue = currentMaximum;
}


/* Creates a copy of the given datum. */
static Datum
DatumCopy(Datum datum, bool datumTypeByValue, int datumTypeLength)
{
	Datum datumCopy = 0;

	if (datumTypeByValue)
	{
		datumCopy = datum;
	}
	else
	{
		uint32 datumLength = att_addlength_datum(0, datumTypeLength, datum);
		char *datumData = palloc0(datumLength);
		memcpy(datumData, DatumGetPointer(datum), datumLength);

		datumCopy = PointerGetDatum(datumData);
	}

	return datumCopy;
}


/*
 * AppendStripeMetadata adds a copy of given stripeMetadata to the given
 * table footer's stripeMetadataList.
 */
static void
AppendStripeMetadata(TableFooter *tableFooter, StripeMetadata stripeMetadata)
{
	StripeMetadata *stripeMetadataCopy = palloc0(sizeof(StripeMetadata));
	memcpy(stripeMetadataCopy, &stripeMetadata, sizeof(StripeMetadata));

	tableFooter->stripeMetadataList = lappend(tableFooter->stripeMetadataList,
											  stripeMetadataCopy);
}


/* Writes the given data to the given file pointer and checks for errors. */
static void
WriteToFile(FILE *file, void *data, uint32 dataLength)
{
	int writeResult = 0;
	int errorResult = 0;

	if (dataLength == 0)
	{
		return;
	}

	errno = 0;
	writeResult = fwrite(data, dataLength, 1, file);
	if (writeResult != 1)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not write file: %m")));
	}

	errorResult = ferror(file);
	if (errorResult != 0)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("error in file: %m")));
	}
}


/* Flushes, syncs, and closes the given file pointer and checks for errors. */
static void
SyncAndCloseFile(FILE *file)
{
	int flushResult = 0;
	int syncResult = 0;
	int errorResult = 0;
	int freeResult = 0;

	errno = 0;
	flushResult = fflush(file);
	if (flushResult != 0)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not flush file: %m")));
	}

	syncResult = pg_fsync(fileno(file));
	if (syncResult != 0)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not sync file: %m")));
	}

	errorResult = ferror(file);
	if (errorResult != 0)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("error in file: %m")));
	}

	freeResult = FreeFile(file);
	if (freeResult != 0)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not close file: %m")));
	}
}
