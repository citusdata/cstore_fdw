/*-------------------------------------------------------------------------
 *
 * cstore_reader.c
 *
 * This file contains function definitions for reading cstore files. This
 * includes the logic for reading file level metadata, reading row stripes,
 * and skipping unrelated row blocks and columns.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"
#include "cstore_fdw.h"
#include "cstore_metadata_serialization.h"

#include "access/nbtree.h"
#include "access/skey.h"
#include "commands/defrem.h"
#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/predtest.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "port.h"
#include "storage/fd.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"


/* static function declarations */
static StringInfo ReadFooterData(Relation relation);
static StripeBuffers * LoadFilteredStripeBuffers(Relation relation,
												 StripeMetadata *stripeMetadata,
												 TupleDesc tupleDescriptor,
												 List *projectedColumnList,
												 List *whereClauseList);
static void ReadStripeNextRow(StripeBuffers *stripeBuffers, List *projectedColumnList,
							  uint64 blockIndex, uint64 blockRowIndex,
							  ColumnBlockData **blockDataArray,
							  Datum *columnValues, bool *columnNulls);
static ColumnBuffers * LoadColumnBuffers(Relation relation,
										 ColumnBlockSkipNode *blockSkipNodeArray,
										 uint32 blockCount, uint64 existsFileOffset,
										 uint64 valueFileOffset,
										 Form_pg_attribute attributeForm);
static StripeFooter * LoadStripeFooter(Relation relation, StripeMetadata *stripeMetadata,
									   uint32 columnCount);
static StripeSkipList * LoadStripeSkipList(Relation relation,
										   StripeMetadata *stripeMetadata,
										   StripeFooter *stripeFooter,
										   uint32 columnCount,
										   bool *projectedColumnMask,
										   Form_pg_attribute attributeFormArray);
static bool * SelectedBlockMask(StripeSkipList *stripeSkipList,
								List *projectedColumnList, List *whereClauseList);
static List * BuildRestrictInfoList(List *whereClauseList);
static Node * BuildBaseConstraint(Var *variable);
static OpExpr * MakeOpExpression(Var *variable, int16 strategyNumber);
static Oid GetOperatorByType(Oid typeId, Oid accessMethodId, int16 strategyNumber);
static void UpdateConstraint(Node *baseConstraint, Datum minValue, Datum maxValue);
static StripeSkipList * SelectedBlockSkipList(StripeSkipList *stripeSkipList,
		 	 	 	 	 	 	 	 	 	  bool *projectedColumnMask,
											  bool *selectedBlockMask);
static uint32 StripeSkipListRowCount(StripeSkipList *stripeSkipList);
static bool * ProjectedColumnMask(uint32 columnCount, List *projectedColumnList);
static void DeserializeBoolArray(StringInfo boolArrayBuffer, bool *boolArray,
								 uint32 boolArrayLength);
static void DeserializeDatumArray(StringInfo datumBuffer, bool *existsArray,
								  uint32 datumCount, bool datumTypeByValue,
								  int datumTypeLength, char datumTypeAlign,
								  Datum *datumArray);
static void DeserializeBlockData(StripeBuffers *stripeBuffers, uint64 blockIndex,
								 Form_pg_attribute attributeFormArray,
								 uint32 rowCount, ColumnBlockData **blockDataArray,
								 TupleDesc tupleDescriptor);
static Datum ColumnDefaultValue(TupleConstr *tupleConstraints,
								Form_pg_attribute attributeForm);
static StringInfo ReadFromFile(Relation relation, uint64 offset, uint32 size);
static void ResetUncompressedBlockData(ColumnBlockData **blockDataArray,
									   uint32 columnCount);
static uint64 StripeRowCount(Relation relation, StripeMetadata *stripeMetadata);


/*
 * CStoreBeginRead initializes a cstore read operation. This function returns a
 * read handle that's used during reading rows and finishing the read operation.
 */
TableReadState *
CStoreBeginRead(Relation relation, TupleDesc tupleDescriptor,
				List *projectedColumnList, List *whereClauseList)
{
	TableReadState *readState = NULL;
	TableFooter *tableFooter = NULL;
	MemoryContext stripeReadContext = NULL;
	uint32 columnCount = 0;
	bool *projectedColumnMask = NULL;
	ColumnBlockData **blockDataArray  = NULL;

	tableFooter = CStoreReadFooter(relation);

	if (tableFooter == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
						errmsg("Could not read table file.")));
	}

	/*
	 * We allocate all stripe specific data in the stripeReadContext, and reset
	 * this memory context before loading a new stripe. This is to avoid memory
	 * leaks.
	 */
	stripeReadContext = AllocSetContextCreate(CurrentMemoryContext,
											  "Stripe Read Memory Context",
											  ALLOCSET_DEFAULT_MINSIZE,
											  ALLOCSET_DEFAULT_INITSIZE,
											  ALLOCSET_DEFAULT_MAXSIZE);

	columnCount = tupleDescriptor->natts;
	projectedColumnMask = ProjectedColumnMask(columnCount, projectedColumnList);
	blockDataArray = CreateEmptyBlockDataArray(columnCount, projectedColumnMask,
										 	   tableFooter->blockRowCount);

	readState = palloc0(sizeof(TableReadState));
	readState->tableFooter = tableFooter;
	readState->projectedColumnList = projectedColumnList;
	readState->whereClauseList = whereClauseList;
	readState->stripeBuffers = NULL;
	readState->readStripeCount = 0;
	readState->stripeReadRowCount = 0;
	readState->tupleDescriptor = tupleDescriptor;
	readState->stripeReadContext = stripeReadContext;
	readState->blockDataArray = blockDataArray;
	readState->deserializedBlockIndex = -1;
	readState->relation = relation;

	return readState;
}


/*
 * CStoreReadFooter reads the cstore table footer for the given relation. First, the
 * function reads the last byte of record as the postscript size. Then, the
 * function reads the postscript. Last, the function reads and deserializes the
 * footer.
 *
 * Footer data looks like following
 * | table_footer (char[]) | postscript (char [postscriptSize]) | postscriptSize(uint8) |
 * Parser first checks the last byte of the buffer to read postscript info that
 * contains cstore major/minor version, cstore magic number, and byte length of
 * the footer.
 */
TableFooter *
CStoreReadFooter(Relation relation)
{
	TableFooter *tableFooter = NULL;
	StringInfo footerData = NULL;
	uint64 footerLength = 0;
	StringInfo postscriptBuffer = NULL;
	uint64 postscriptSizeOffset = 0;
	uint8 postscriptSize = 0;
	uint64 postscriptOffset = 0;

	footerData = ReadFooterData(relation);
	if (footerData == NULL)
	{
		return NULL;
	}

	postscriptSizeOffset = footerData->len - CSTORE_POSTSCRIPT_SIZE_LENGTH;
	memcpy(&postscriptSize, footerData->data + postscriptSizeOffset,
		   CSTORE_POSTSCRIPT_SIZE_LENGTH);

	/* make sure data contains enough bytes to contain postscript */
	if (postscriptSize + CSTORE_POSTSCRIPT_SIZE_LENGTH > footerData->len)
	{
		ereport(ERROR, (errmsg("invalid postscript size")));
	}

	postscriptOffset = postscriptSizeOffset - postscriptSize;

	postscriptBuffer = makeStringInfo();
	appendBinaryStringInfo(postscriptBuffer, footerData->data + postscriptOffset,
						   postscriptSize);

	DeserializePostScript(postscriptBuffer, &footerLength);

	/* make sure data contains enough bytes to contain whole footer */
	if (footerLength + postscriptSize + CSTORE_POSTSCRIPT_SIZE_LENGTH != footerData->len)
	{
		ereport(ERROR, (errmsg("invalid footer size")));
	}

	/*
	 * DeserializeTableFooter expects footer data without postscript, reduce len
	 * field to trim the right end of footer data.
	 */
	footerData->len = footerLength;
	tableFooter = DeserializeTableFooter(footerData);

	pfree(footerData->data);
	pfree(footerData);

	return tableFooter;
}


/*
 * ReadFooterData reads and returns footer data. The first page contains the
 * metadata about footer data. The function validates the metadata, reads
 * footer data from where it is stored and returns it.
 */
static StringInfo
ReadFooterData(Relation relation)
{
	int32 totalFooterLength = 0;
	BlockNumber totalBlockCount = 0;
	BlockNumber blockIndex = 0;
	const int sizeofFooterLengthType = sizeof(int32);
	StringInfo footerData = NULL;
	Buffer firstBuffer = InvalidBuffer;
	Page page = NULL;
	char *pageData = NULL;
	int pageDataLength = 0;
	PageHeader pageHeader = NULL;
	Buffer footerMetadataBuffer = InvalidBuffer;
	Page headerPage = NULL;
	char *headerData = NULL;
	uint32 headerDataLength = 0;
	uint32 startingBlock = 0;
	uint32 blockCount = 0;

	totalBlockCount = RelationGetNumberOfBlocksInFork(relation, FOOTER_FORKNUM);

	if (totalBlockCount == 0)
	{
		return NULL;
	}

	footerMetadataBuffer = ReadBufferExtended(relation, FOOTER_FORKNUM, 0, RBM_NORMAL,
											  NULL);
	LockBuffer(footerMetadataBuffer, BUFFER_LOCK_SHARE);
	headerPage = BufferGetPage(footerMetadataBuffer);
	headerData = PageGetContents(headerPage);
	pageHeader = (PageHeader) headerPage;
	headerDataLength = pageHeader->pd_lower - SizeOfPageHeaderData;

	DeserializeTableFooterMetadata(headerData, headerDataLength, &startingBlock,
								   &blockCount);

	firstBuffer = ReadBufferExtended(relation, FOOTER_FORKNUM, startingBlock,
									 RBM_NORMAL, NULL);

	LockBuffer(firstBuffer, BUFFER_LOCK_SHARE);
	page = BufferGetPage(firstBuffer);
	pageHeader = (PageHeader) page;
	pageData = PageGetContents(page);

	memcpy(&totalFooterLength, pageData, sizeofFooterLengthType);

	if (totalFooterLength < CSTORE_POSTSCRIPT_SIZE_LENGTH)
	{
		ereport(ERROR, (errmsg("invalid cstore file")));
	}

	footerData = makeStringInfo();

	/* allocate sufficiently large string for footer to prevent reallocations */
	enlargeStringInfo(footerData, blockCount * BLCKSZ);

	pageDataLength = pageHeader->pd_lower - SizeOfPageHeaderData - sizeofFooterLengthType;

	appendBinaryStringInfo(footerData, pageData + sizeofFooterLengthType, pageDataLength);

	UnlockReleaseBuffer(firstBuffer);

	for (blockIndex = 1; blockIndex < blockCount; blockIndex++)
	{
		Buffer buffer = ReadBufferExtended(relation, FOOTER_FORKNUM,
										   blockIndex + startingBlock,
										   RBM_NORMAL, NULL);
		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buffer);
		pageHeader = (PageHeader) page;
		pageData = PageGetContents(page);
		pageDataLength = pageHeader->pd_lower - SizeOfPageHeaderData;
		appendBinaryStringInfo(footerData, pageData, pageDataLength);
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		ReleaseBuffer(buffer);
	}

	UnlockReleaseBuffer(footerMetadataBuffer);

	return footerData;
}


/*
 * CStoreReadNextRow tries to read a row from the cstore file. On success, it sets
 * column values and nulls, and returns true. If there are no more rows to read,
 * the function returns false.
 */
bool
CStoreReadNextRow(TableReadState *readState, Datum *columnValues, bool *columnNulls)
{
	uint32 blockIndex = 0;
	uint32 blockRowIndex = 0;
	TableFooter *tableFooter = readState->tableFooter;
	Form_pg_attribute attributeFormArray = readState->tupleDescriptor->attrs;
	MemoryContext oldContext = NULL;

	/*
	 * If no stripes are loaded, load the next non-empty stripe. Note that when
	 * loading stripes, we skip over blocks whose contents can be filtered with
	 * the query's restriction qualifiers. So, even when a stripe is physically
	 * not empty, we may end up loading it as an empty stripe.
	 */
	while (readState->stripeBuffers == NULL)
	{
		StripeBuffers *stripeBuffers = NULL;
		StripeMetadata *stripeMetadata = NULL;
		List *stripeMetadataList = tableFooter->stripeMetadataList;
		uint32 stripeCount = list_length(stripeMetadataList);

		/* if we have read all stripes, return false */
		if (readState->readStripeCount == stripeCount)
		{
			return false;
		}

		oldContext = MemoryContextSwitchTo(readState->stripeReadContext);
		MemoryContextReset(readState->stripeReadContext);

		stripeMetadata = list_nth(stripeMetadataList, readState->readStripeCount);

		stripeBuffers = LoadFilteredStripeBuffers(readState->relation, stripeMetadata,
												  readState->tupleDescriptor,
												  readState->projectedColumnList,
												  readState->whereClauseList);
		readState->readStripeCount++;

		MemoryContextSwitchTo(oldContext);

		if (stripeBuffers->rowCount != 0)
		{
			readState->stripeBuffers = stripeBuffers;
			readState->stripeReadRowCount = 0;
			readState->deserializedBlockIndex = -1;
			ResetUncompressedBlockData(readState->blockDataArray,
									   stripeBuffers->columnCount);
			break;
		}
	}

	blockIndex = readState->stripeReadRowCount / tableFooter->blockRowCount;
	blockRowIndex = readState->stripeReadRowCount % tableFooter->blockRowCount;

	if (blockIndex != readState->deserializedBlockIndex)
	{
		uint32 lastBlockIndex = 0;
		uint32 blockRowCount = 0;
		uint32 stripeRowCount = 0;

		stripeRowCount = readState->stripeBuffers->rowCount;
		lastBlockIndex = stripeRowCount / tableFooter->blockRowCount;
		if (blockIndex == lastBlockIndex)
		{
			blockRowCount = stripeRowCount % tableFooter->blockRowCount;
		}
		else
		{
			blockRowCount = tableFooter->blockRowCount;
		}

		oldContext = MemoryContextSwitchTo(readState->stripeReadContext);

		DeserializeBlockData(readState->stripeBuffers, blockIndex, attributeFormArray,
							 blockRowCount, readState->blockDataArray,
							 readState->tupleDescriptor);

		MemoryContextSwitchTo(oldContext);

		readState->deserializedBlockIndex = blockIndex;
	}

	ReadStripeNextRow(readState->stripeBuffers, readState->projectedColumnList,
					  blockIndex, blockRowIndex, readState->blockDataArray,
					  columnValues, columnNulls);

	/*
	 * If we finished reading the current stripe, set stripe data to NULL. That
	 * way, we will load a new stripe the next time this function gets called.
	 */
	readState->stripeReadRowCount++;
	if (readState->stripeReadRowCount == readState->stripeBuffers->rowCount)
	{
		readState->stripeBuffers = NULL;
	}

	return true;
}


/* Finishes a cstore read operation. */
void
CStoreEndRead(TableReadState *readState)
{
	int columnCount = readState->tupleDescriptor->natts;

	MemoryContextDelete(readState->stripeReadContext);
	list_free_deep(readState->tableFooter->stripeMetadataList);
	FreeColumnBlockDataArray(readState->blockDataArray, columnCount);
	pfree(readState->tableFooter);
	pfree(readState);
}


/*
 * CreateEmptyBlockDataArray creates data buffers to keep deserialized exist and
 * value arrays for requested columns in columnMask.
 */
ColumnBlockData **
CreateEmptyBlockDataArray(uint32 columnCount, bool *columnMask, uint32 blockRowCount)
{
	uint32 columnIndex = 0;
	ColumnBlockData **blockDataArray = palloc0(columnCount * sizeof(ColumnBlockData*));

	/* allocate block memory for deserialized data */
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		if (columnMask[columnIndex])
		{
			ColumnBlockData *blockData = palloc0(sizeof(ColumnBlockData));

			blockData->existsArray = palloc0(blockRowCount * sizeof(bool));
			blockData->valueArray = palloc0(blockRowCount * sizeof(Datum));
			blockData->valueBuffer = NULL;
			blockDataArray[columnIndex] = blockData;
		}
	}
	
	return blockDataArray;
}


/*
 * FreeColumnBlockDataArray deallocates data buffers to keep deserialized exist and
 * value arrays for requested columns in columnMask.
 * ColumnBlockData->serializedValueBuffer lives in memory read/write context
 * so it is deallocated automatically when the context is deleted.
 */
void
FreeColumnBlockDataArray(ColumnBlockData **blockDataArray, uint32 columnCount)
{
	uint32 columnIndex = 0;
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		ColumnBlockData *blockData = blockDataArray[columnIndex];
		if (blockData != NULL)
		{
			pfree(blockData->existsArray);
			pfree(blockData->valueArray);
			pfree(blockData);
		}
	}
	
	pfree(blockDataArray);
}


/* CStoreTableRowCount returns the exact row count of a table using skiplists */
uint64
CStoreTableRowCount(Relation relation)
{
	TableFooter *tableFooter = NULL;
	ListCell *stripeMetadataCell = NULL;
	uint64 totalRowCount = 0;

	tableFooter = CStoreReadFooter(relation);

	if (tableFooter == NULL)
	{
		ereport(ERROR, (ERRCODE_FDW_ERROR, errmsg("Could not read table footer.")));
	}

	foreach(stripeMetadataCell, tableFooter->stripeMetadataList)
	{
		StripeMetadata *stripeMetadata = (StripeMetadata *) lfirst(stripeMetadataCell);
		totalRowCount += StripeRowCount(relation, stripeMetadata);
	}

	return totalRowCount;
}


/*
 * StripeRowCount reads serialized stripe footer, the first column's
 * skip list, and returns number of rows for given stripe.
 */
static uint64
StripeRowCount(Relation relation, StripeMetadata *stripeMetadata)
{
	uint64 rowCount = 0;
	StripeFooter *stripeFooter = NULL;
	StringInfo footerBuffer = NULL;
	StringInfo firstColumnSkipListBuffer = NULL;
	uint64 footerOffset = 0;

	footerOffset += stripeMetadata->fileOffset;
	footerOffset += stripeMetadata->skipListLength;
	footerOffset += stripeMetadata->dataLength;

	footerBuffer = ReadFromFile(relation, footerOffset, stripeMetadata->footerLength);
	stripeFooter = DeserializeStripeFooter(footerBuffer);

	firstColumnSkipListBuffer = ReadFromFile(relation, stripeMetadata->fileOffset,
											 stripeFooter->skipListSizeArray[0]);
	rowCount = DeserializeRowCount(firstColumnSkipListBuffer);

	return rowCount;
}


/*
 * LoadFilteredStripeBuffers reads serialized stripe data for given relation.
 * The function skips over blocks whose rows are refuted by restriction qualifiers,
 * and only loads columns that are projected in the query.
 */
static StripeBuffers *
LoadFilteredStripeBuffers(Relation relation, StripeMetadata *stripeMetadata,
						  TupleDesc tupleDescriptor, List *projectedColumnList,
						  List *whereClauseList)
{
	StripeBuffers *stripeBuffers = NULL;
	ColumnBuffers **columnBuffersArray = NULL;
	uint64 currentColumnFileOffset = 0;
	uint32 columnIndex = 0;
	Form_pg_attribute attributeFormArray = tupleDescriptor->attrs;
	uint32 columnCount = tupleDescriptor->natts;

	bool *projectedColumnMask = ProjectedColumnMask(columnCount, projectedColumnList);

	StripeFooter *stripeFooter = LoadStripeFooter(relation, stripeMetadata,
												  columnCount);

	StripeSkipList *stripeSkipList = LoadStripeSkipList(relation, stripeMetadata,
														stripeFooter, columnCount,
														projectedColumnMask,
														attributeFormArray);

	bool *selectedBlockMask = SelectedBlockMask(stripeSkipList, projectedColumnList,
												whereClauseList);

	StripeSkipList *selectedBlockSkipList =
		SelectedBlockSkipList(stripeSkipList, projectedColumnMask,
							  selectedBlockMask);

	/* load column data for projected columns */
	columnBuffersArray = palloc0(columnCount * sizeof(ColumnBuffers *));
	currentColumnFileOffset = stripeMetadata->fileOffset + stripeMetadata->skipListLength;

	for (columnIndex = 0; columnIndex < stripeFooter->columnCount; columnIndex++)
	{
		uint64 existsSize = stripeFooter->existsSizeArray[columnIndex];
		uint64 valueSize = stripeFooter->valueSizeArray[columnIndex];
		uint64 existsFileOffset = currentColumnFileOffset;
		uint64 valueFileOffset = currentColumnFileOffset + existsSize;

		if (projectedColumnMask[columnIndex])
		{
			ColumnBlockSkipNode *blockSkipNode =
				selectedBlockSkipList->blockSkipNodeArray[columnIndex];
			Form_pg_attribute attributeForm = &attributeFormArray[columnIndex];
			uint32 blockCount = selectedBlockSkipList->blockCount;

			ColumnBuffers *columnBuffers = LoadColumnBuffers(relation, blockSkipNode,
															 blockCount,
															 existsFileOffset,
															 valueFileOffset,
															 attributeForm);

			columnBuffersArray[columnIndex] = columnBuffers;
		}

		currentColumnFileOffset += existsSize;
		currentColumnFileOffset += valueSize;
	}

	stripeBuffers = palloc0(sizeof(StripeBuffers));
	stripeBuffers->columnCount = columnCount;
	stripeBuffers->rowCount = StripeSkipListRowCount(selectedBlockSkipList);
	stripeBuffers->columnBuffersArray = columnBuffersArray;

	return stripeBuffers;
}


/*
 * ReadStripeNextRow reads the next row from the given stripe, finds the projected
 * column values within this row, and accordingly sets the column values and nulls.
 * Note that this function sets the values for all non-projected columns to null.
 */
static void
ReadStripeNextRow(StripeBuffers *stripeBuffers, List *projectedColumnList,
				  uint64 blockIndex, uint64 blockRowIndex,
				  ColumnBlockData **blockDataArray, Datum *columnValues,
				  bool *columnNulls)
{
	ListCell *projectedColumnCell = NULL;

	/* set all columns to null by default */
	memset(columnNulls, 1, stripeBuffers->columnCount * sizeof(bool));

	foreach(projectedColumnCell, projectedColumnList)
	{
		Var *projectedColumn = lfirst(projectedColumnCell);
		uint32 projectedColumnIndex = projectedColumn->varattno - 1;
		ColumnBlockData *blockData = blockDataArray[projectedColumnIndex];

		if (blockData->existsArray[blockRowIndex])
		{
			columnValues[projectedColumnIndex] = blockData->valueArray[blockRowIndex];
			columnNulls[projectedColumnIndex] = false;
		}
	}
}


/*
 * LoadColumnBuffers reads serialized column data for given relation. These
 * column data are laid out as sequential blocks in the file; and block positions
 * and lengths are retrieved from the column block skip node array.
 */
static ColumnBuffers *
LoadColumnBuffers(Relation relation, ColumnBlockSkipNode *blockSkipNodeArray,
				  uint32 blockCount, uint64 existsFileOffset, uint64 valueFileOffset,
				  Form_pg_attribute attributeForm)
{
	ColumnBuffers *columnBuffers = NULL;
	uint32 blockIndex = 0;
	ColumnBlockBuffers **blockBuffersArray =
			palloc0(blockCount * sizeof(ColumnBlockBuffers *));

	for (blockIndex = 0; blockIndex < blockCount; blockIndex++)
	{
		blockBuffersArray[blockIndex] = palloc0(sizeof(ColumnBlockBuffers));
	}

	/*
	 * We first read the "exists" blocks. We don't read "values" array here,
	 * because "exists" blocks are stored sequentially on disk, and we want to
	 * minimize disk seeks.
	 */
	for (blockIndex = 0; blockIndex < blockCount; blockIndex++)
	{
		ColumnBlockSkipNode *blockSkipNode = &blockSkipNodeArray[blockIndex];
		uint64 existsOffset = existsFileOffset + blockSkipNode->existsBlockOffset;
		StringInfo rawExistsBuffer = ReadFromFile(relation, existsOffset,
												  blockSkipNode->existsLength);

		blockBuffersArray[blockIndex]->existsBuffer = rawExistsBuffer;
	}

	/* then read "values" blocks, which are also stored sequentially on disk */
	for (blockIndex = 0; blockIndex < blockCount; blockIndex++)
	{
		ColumnBlockSkipNode *blockSkipNode = &blockSkipNodeArray[blockIndex];
		CompressionType compressionType = blockSkipNode->valueCompressionType;
		uint64 valueOffset = valueFileOffset + blockSkipNode->valueBlockOffset;
		StringInfo rawValueBuffer = ReadFromFile(relation, valueOffset,
												 blockSkipNode->valueLength);

		blockBuffersArray[blockIndex]->valueBuffer = rawValueBuffer;
		blockBuffersArray[blockIndex]->valueCompressionType = compressionType;
	}

	columnBuffers = palloc0(sizeof(ColumnBuffers));
	columnBuffers->blockBuffersArray = blockBuffersArray;

	return columnBuffers;
}


/* Reads and returns the given stripe's footer. */
static StripeFooter *
LoadStripeFooter(Relation relation, StripeMetadata *stripeMetadata,
				 uint32 columnCount)
{
	StripeFooter *stripeFooter = NULL;
	StringInfo footerBuffer = NULL;
	uint64 footerOffset = 0;

	footerOffset += stripeMetadata->fileOffset;
	footerOffset += stripeMetadata->skipListLength;
	footerOffset += stripeMetadata->dataLength;

	footerBuffer = ReadFromFile(relation, footerOffset, stripeMetadata->footerLength);
	stripeFooter = DeserializeStripeFooter(footerBuffer);
	if (stripeFooter->columnCount > columnCount)
	{
		ereport(ERROR, (errmsg("stripe footer column count and table column count "
							   "don't match")));
	}

	return stripeFooter;
}


/* Reads the skip list for the given stripe. */
static StripeSkipList *
LoadStripeSkipList(Relation relation, StripeMetadata *stripeMetadata,
				   StripeFooter *stripeFooter, uint32 columnCount,
				   bool *projectedColumnMask,
				   Form_pg_attribute attributeFormArray)
{
	StripeSkipList *stripeSkipList = NULL;
	ColumnBlockSkipNode **blockSkipNodeArray = NULL;
	StringInfo firstColumnSkipListBuffer = NULL;
	uint64 currentColumnSkipListFileOffset = 0;
	uint32 columnIndex = 0;
	uint32 stripeBlockCount = 0;
	uint32 stripeColumnCount = stripeFooter->columnCount;

	/* deserialize block count */
	firstColumnSkipListBuffer = ReadFromFile(relation, stripeMetadata->fileOffset,
											 stripeFooter->skipListSizeArray[0]);
	stripeBlockCount = DeserializeBlockCount(firstColumnSkipListBuffer);

	/* deserialize column skip lists */
	blockSkipNodeArray = palloc0(columnCount * sizeof(ColumnBlockSkipNode *));
	currentColumnSkipListFileOffset = stripeMetadata->fileOffset;

	for (columnIndex = 0; columnIndex < stripeColumnCount; columnIndex++)
	{
		uint64 columnSkipListSize = stripeFooter->skipListSizeArray[columnIndex];

		bool firstColumn = columnIndex == 0;

		/*
		 * Only selected columns' column skip lists are read. However, the first
		 * column's skip list is read regardless of being selected. It is used by
		 * StripeSkipListRowCount later.
		 */
		if (projectedColumnMask[columnIndex] || firstColumn)
		{
			Form_pg_attribute attributeForm = &attributeFormArray[columnIndex];

			StringInfo columnSkipListBuffer =
				ReadFromFile(relation, currentColumnSkipListFileOffset, columnSkipListSize);

			ColumnBlockSkipNode *columnSkipList =
					DeserializeColumnSkipList(columnSkipListBuffer, attributeForm->attbyval,
											  attributeForm->attlen, stripeBlockCount);
			blockSkipNodeArray[columnIndex] = columnSkipList;
		}

		currentColumnSkipListFileOffset += columnSkipListSize;
	}

	/* table contains additional columns added after this stripe is created */
	for (columnIndex = stripeColumnCount; columnIndex < columnCount; columnIndex++)
	{
		ColumnBlockSkipNode *columnSkipList = NULL;
		uint32 blockIndex = 0;
		bool firstColumn = columnIndex == 0;

		/* no need to create ColumnBlockSkipList if the column is not selected */
		if (!projectedColumnMask[columnIndex] && !firstColumn)
		{
			blockSkipNodeArray[columnIndex] = NULL;
			continue;
		}

		/* create empty ColumnBlockSkipNode for missing columns*/
		columnSkipList = palloc0(stripeBlockCount * sizeof(ColumnBlockSkipNode));

		for (blockIndex = 0; blockIndex < stripeBlockCount; blockIndex++)
		{
			columnSkipList[blockIndex].rowCount = 0;
			columnSkipList[blockIndex].hasMinMax = false;
			columnSkipList[blockIndex].minimumValue = 0;
			columnSkipList[blockIndex].maximumValue = 0;
			columnSkipList[blockIndex].existsBlockOffset = 0;
			columnSkipList[blockIndex].valueBlockOffset = 0;
			columnSkipList[blockIndex].existsLength = 0;
			columnSkipList[blockIndex].valueLength = 0;
			columnSkipList[blockIndex].valueCompressionType = COMPRESSION_NONE;
		}
		blockSkipNodeArray[columnIndex] = columnSkipList;
	}

	stripeSkipList = palloc0(sizeof(StripeSkipList));
	stripeSkipList->blockSkipNodeArray = blockSkipNodeArray;
	stripeSkipList->columnCount = columnCount;
	stripeSkipList->blockCount = stripeBlockCount;

	return stripeSkipList;
}


/*
 * SelectedBlockMask walks over each column's blocks and checks if a block can
 * be filtered without reading its data. The filtering happens when all rows in
 * the block can be refuted by the given qualifier conditions.
 */
static bool *
SelectedBlockMask(StripeSkipList *stripeSkipList, List *projectedColumnList,
				  List *whereClauseList)
{
	bool *selectedBlockMask = NULL;
	ListCell *columnCell = NULL;
	uint32 blockIndex = 0;
	List *restrictInfoList = BuildRestrictInfoList(whereClauseList);

	selectedBlockMask = palloc0(stripeSkipList->blockCount * sizeof(bool));
	memset(selectedBlockMask, true, stripeSkipList->blockCount * sizeof(bool));

	foreach(columnCell, projectedColumnList)
	{
		Var *column = lfirst(columnCell);
		uint32 columnIndex = column->varattno - 1;
		FmgrInfo *comparisonFunction = NULL;
		Node *baseConstraint = NULL;

		/* if this column's data type doesn't have a comparator, skip it */
		comparisonFunction = GetFunctionInfoOrNull(column->vartype, BTREE_AM_OID,
												   BTORDER_PROC);
		if (comparisonFunction == NULL)
		{
			continue;
		}

		baseConstraint = BuildBaseConstraint(column);
		for (blockIndex = 0; blockIndex < stripeSkipList->blockCount; blockIndex++)
		{
			bool predicateRefuted = false;
			List *constraintList = NIL;
			ColumnBlockSkipNode *blockSkipNodeArray =
				stripeSkipList->blockSkipNodeArray[columnIndex];
			ColumnBlockSkipNode *blockSkipNode = &blockSkipNodeArray[blockIndex];

			/*
			 * A column block with comparable data type can miss min/max values
			 * if all values in the block are NULL.
			 */
			if (!blockSkipNode->hasMinMax)
			{
				continue;
			}

			UpdateConstraint(baseConstraint, blockSkipNode->minimumValue,
							 blockSkipNode->maximumValue);

			constraintList = list_make1(baseConstraint);
#if (PG_VERSION_NUM >= 100000)
			predicateRefuted = predicate_refuted_by(constraintList, restrictInfoList, false);
#else
			predicateRefuted = predicate_refuted_by(constraintList, restrictInfoList);
#endif
			if (predicateRefuted)
			{
				selectedBlockMask[blockIndex] = false;
			}
		}
	}

	return selectedBlockMask;
}


/*
 * GetFunctionInfoOrNull first resolves the operator for the given data type,
 * access method, and support procedure. The function then uses the resolved
 * operator's identifier to fill in a function manager object, and returns
 * this object. This function is based on a similar function from CitusDB's code.
 */
FmgrInfo *
GetFunctionInfoOrNull(Oid typeId, Oid accessMethodId, int16 procedureId)
{
	FmgrInfo *functionInfo = NULL;
	Oid operatorClassId = InvalidOid;
	Oid operatorFamilyId = InvalidOid;
	Oid operatorId = InvalidOid;

	/* get default operator class from pg_opclass for datum type */
	operatorClassId = GetDefaultOpClass(typeId, accessMethodId);
	if (operatorClassId == InvalidOid)
	{
		return NULL;
	}

	operatorFamilyId = get_opclass_family(operatorClassId);
	if (operatorFamilyId == InvalidOid)
	{
		return NULL;
	}

	operatorId = get_opfamily_proc(operatorFamilyId, typeId, typeId, procedureId);
	if (operatorId != InvalidOid)
	{
		functionInfo = (FmgrInfo *) palloc0(sizeof(FmgrInfo));

		/* fill in the FmgrInfo struct using the operatorId */
		fmgr_info(operatorId, functionInfo);
	}

	return functionInfo;
}


/*
 * BuildRestrictInfoList builds restrict info list using the selection criteria,
 * and then return this list. The function is copied from CitusDB's shard pruning
 * logic.
 */
static List *
BuildRestrictInfoList(List *whereClauseList)
{
	List *restrictInfoList = NIL;

	ListCell *qualCell = NULL;
	foreach(qualCell, whereClauseList)
	{
		RestrictInfo *restrictInfo = NULL;
		Node *qualNode = (Node *) lfirst(qualCell);

		restrictInfo = make_simple_restrictinfo((Expr *) qualNode);
		restrictInfoList = lappend(restrictInfoList, restrictInfo);
	}

	return restrictInfoList;
}


/*
 * BuildBaseConstraint builds and returns a base constraint. This constraint
 * implements an expression in the form of (var <= max && var >= min), where
 * min and max values represent a block's min and max values. These block
 * values are filled in after the constraint is built. This function is based
 * on a similar function from CitusDB's shard pruning logic.
 */
static Node *
BuildBaseConstraint(Var *variable)
{
	Node *baseConstraint = NULL;
	OpExpr *lessThanExpr = NULL;
	OpExpr *greaterThanExpr = NULL;

	lessThanExpr = MakeOpExpression(variable, BTLessEqualStrategyNumber);
	greaterThanExpr = MakeOpExpression(variable, BTGreaterEqualStrategyNumber);

	baseConstraint = make_and_qual((Node *) lessThanExpr, (Node *) greaterThanExpr);

	return baseConstraint;
}


/*
 * MakeOpExpression builds an operator expression node. This operator expression
 * implements the operator clause as defined by the variable and the strategy
 * number. The function is copied from CitusDB's shard pruning logic.
 */
static OpExpr *
MakeOpExpression(Var *variable, int16 strategyNumber)
{
	Oid typeId = variable->vartype;
	Oid typeModId = variable->vartypmod;
	Oid collationId = variable->varcollid;

	Oid accessMethodId = BTREE_AM_OID;
	Oid operatorId = InvalidOid;
	Const  *constantValue = NULL;
	OpExpr *expression = NULL;

	/* Load the operator from system catalogs */
	operatorId = GetOperatorByType(typeId, accessMethodId, strategyNumber);

	constantValue = makeNullConst(typeId, typeModId, collationId);

	/* Now make the expression with the given variable and a null constant */
	expression = (OpExpr *) make_opclause(operatorId,
										  InvalidOid, /* no result type yet */
										  false,	  /* no return set */
										  (Expr *) variable,
										  (Expr *) constantValue,
										  InvalidOid, collationId);

	/* Set implementing function id and result type */
	expression->opfuncid = get_opcode(operatorId);
	expression->opresulttype = get_func_rettype(expression->opfuncid);

	return expression;
}


/*
 * GetOperatorByType returns operator Oid for the given type, access method,
 * and strategy number. Note that this function incorrectly errors out when
 * the given type doesn't have its own operator but can use another compatible
 * type's default operator. The function is copied from CitusDB's shard pruning
 * logic.
 */
static Oid
GetOperatorByType(Oid typeId, Oid accessMethodId, int16 strategyNumber)
{
	/* Get default operator class from pg_opclass */
	Oid operatorClassId = GetDefaultOpClass(typeId, accessMethodId);

	Oid operatorFamily = get_opclass_family(operatorClassId);

	Oid operatorId = get_opfamily_member(operatorFamily, typeId, typeId, strategyNumber);

	return operatorId;
}


/* 
 * UpdateConstraint updates the base constraint with the given min/max values.
 * The function is copied from CitusDB's shard pruning logic.
 */
static void
UpdateConstraint(Node *baseConstraint, Datum minValue, Datum maxValue)
{
	BoolExpr *andExpr = (BoolExpr *) baseConstraint;
	Node *lessThanExpr = (Node *) linitial(andExpr->args);
	Node *greaterThanExpr = (Node *) lsecond(andExpr->args);

	Node *minNode = get_rightop((Expr *) greaterThanExpr);
	Node *maxNode = get_rightop((Expr *) lessThanExpr);
	Const *minConstant = NULL;
	Const *maxConstant = NULL;

	Assert(IsA(minNode, Const));
	Assert(IsA(maxNode, Const));

	minConstant = (Const *) minNode;
	maxConstant = (Const *) maxNode;

	minConstant->constvalue = minValue;
	maxConstant->constvalue = maxValue;

	minConstant->constisnull = false;
	maxConstant->constisnull = false;

	minConstant->constbyval = true;
	maxConstant->constbyval = true;
}


/*
 * SelectedBlockSkipList constructs a new StripeSkipList in which the
 * non-selected blocks are removed from the given stripeSkipList.
 */
static StripeSkipList *
SelectedBlockSkipList(StripeSkipList *stripeSkipList, bool *projectedColumnMask,
					  bool *selectedBlockMask)
{
	StripeSkipList *SelectedBlockSkipList = NULL;
	ColumnBlockSkipNode **selectedBlockSkipNodeArray = NULL;
	uint32 selectedBlockCount = 0;
	uint32 blockIndex = 0;
	uint32 columnIndex = 0;
	uint32 columnCount = stripeSkipList->columnCount;

	for (blockIndex = 0; blockIndex < stripeSkipList->blockCount; blockIndex++)
	{
		if (selectedBlockMask[blockIndex])
		{
			selectedBlockCount++;
		}
	}

	selectedBlockSkipNodeArray = palloc0(columnCount * sizeof(ColumnBlockSkipNode *));
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		uint32 selectedBlockIndex = 0;
		bool firstColumn = columnIndex == 0;

		/* first column's block skip node is always read */
		if (!projectedColumnMask[columnIndex] && !firstColumn)
		{
			selectedBlockSkipNodeArray[columnIndex] = NULL;
			continue;
		}

		Assert(stripeSkipList->blockSkipNodeArray[columnIndex] != NULL);

		selectedBlockSkipNodeArray[columnIndex] = palloc0(selectedBlockCount *
														  sizeof(ColumnBlockSkipNode));

		for (blockIndex = 0; blockIndex < stripeSkipList->blockCount; blockIndex++)
		{
			if (selectedBlockMask[blockIndex])
			{
				selectedBlockSkipNodeArray[columnIndex][selectedBlockIndex] =
					stripeSkipList->blockSkipNodeArray[columnIndex][blockIndex];
				selectedBlockIndex++;
			}
		}
	}

	SelectedBlockSkipList = palloc0(sizeof(StripeSkipList));
	SelectedBlockSkipList->blockSkipNodeArray = selectedBlockSkipNodeArray;
	SelectedBlockSkipList->blockCount = selectedBlockCount;
	SelectedBlockSkipList->columnCount = stripeSkipList->columnCount;

	return SelectedBlockSkipList;
}


/*
 * StripeSkipListRowCount counts the number of rows in the given stripeSkipList.
 * To do this, the function finds the first column, and sums up row counts across
 * all blocks for that column.
 */
static uint32
StripeSkipListRowCount(StripeSkipList *stripeSkipList)
{
	uint32 stripeSkipListRowCount = 0;
	uint32 blockIndex = 0;
	ColumnBlockSkipNode *firstColumnSkipNodeArray =
		stripeSkipList->blockSkipNodeArray[0];

	for (blockIndex = 0; blockIndex < stripeSkipList->blockCount; blockIndex++)
	{
		uint32 blockRowCount = firstColumnSkipNodeArray[blockIndex].rowCount;
		stripeSkipListRowCount += blockRowCount;
	}

	return stripeSkipListRowCount;
}


/*
 * ProjectedColumnMask returns a boolean array in which the projected columns
 * from the projected column list are marked as true.
 */
static bool *
ProjectedColumnMask(uint32 columnCount, List *projectedColumnList)
{
	bool *projectedColumnMask = palloc0(columnCount * sizeof(bool));
	ListCell *columnCell = NULL;

	foreach(columnCell, projectedColumnList)
	{
		Var *column = (Var *) lfirst(columnCell);
		uint32 columnIndex = column->varattno - 1;
		projectedColumnMask[columnIndex] = true;
	}

	return projectedColumnMask;
}


/*
 * DeserializeBoolArray reads an array of bits from the given buffer and stores
 * it in provided bool array.
 */
static void
DeserializeBoolArray(StringInfo boolArrayBuffer, bool *boolArray,
					 uint32 boolArrayLength)
{
	uint32 boolArrayIndex = 0;

	uint32 maximumBoolCount = boolArrayBuffer->len * 8;
	if (boolArrayLength > maximumBoolCount)
	{
		ereport(ERROR, (errmsg("insufficient data for reading boolean array")));
	}

	for (boolArrayIndex = 0; boolArrayIndex < boolArrayLength; boolArrayIndex++)
	{
		uint32 byteIndex = boolArrayIndex / 8;
		uint32 bitIndex = boolArrayIndex % 8;
		uint8 bitmask = (1 << bitIndex);

		uint8 shiftedBit = (boolArrayBuffer->data[byteIndex] & bitmask);
		if (shiftedBit == 0)
		{
			boolArray[boolArrayIndex] = false;
		}
		else
		{
			boolArray[boolArrayIndex] = true;
		}
	}
}


/*
 * DeserializeDatumArray reads an array of datums from the given buffer and stores
 * them in provided datumArray. If a value is marked as false in the exists array,
 * the function assumes that the datum isn't in the buffer, and simply skips it.
 */
static void
DeserializeDatumArray(StringInfo datumBuffer, bool *existsArray, uint32 datumCount,
					  bool datumTypeByValue, int datumTypeLength,
					  char datumTypeAlign, Datum *datumArray)
{
	uint32 datumIndex = 0;
	uint32 currentDatumDataOffset = 0;

	for (datumIndex = 0; datumIndex < datumCount; datumIndex++)
	{
		char *currentDatumDataPointer = NULL;

		if (!existsArray[datumIndex])
		{
			continue;
		}

		currentDatumDataPointer = datumBuffer->data + currentDatumDataOffset;

		datumArray[datumIndex] = fetch_att(currentDatumDataPointer, datumTypeByValue,
										   datumTypeLength);
		currentDatumDataOffset = att_addlength_datum(currentDatumDataOffset,
													 datumTypeLength,
													 currentDatumDataPointer);
		currentDatumDataOffset = att_align_nominal(currentDatumDataOffset,
												   datumTypeAlign);

		if (currentDatumDataOffset > datumBuffer->len)
		{
			ereport(ERROR, (errmsg("insufficient data left in datum buffer")));
		}
	}
}


/*
 * DeserializeBlockData deserializes requested data block for all columns and
 * stores in blockDataArray. It uncompresses serialized data if necessary. The
 * function also deallocates data buffers used for previous block, and compressed
 * data buffers for the current block which will not be needed again. If a column
 * data is not present serialized buffer, then default value (or null) is used
 * to fill value array.
 */
static void
DeserializeBlockData(StripeBuffers *stripeBuffers, uint64 blockIndex,
					 Form_pg_attribute attributeFormArray, uint32 rowCount,
					 ColumnBlockData **blockDataArray, TupleDesc tupleDescriptor)
{
	int columnIndex = 0;
	for (columnIndex = 0; columnIndex < stripeBuffers->columnCount; columnIndex++)
	{
		ColumnBlockData *blockData = blockDataArray[columnIndex];
		Form_pg_attribute attributeForm = &attributeFormArray[columnIndex];
		ColumnBuffers *columnBuffers = stripeBuffers->columnBuffersArray[columnIndex];
		bool columnAdded = false;

		if ((columnBuffers == NULL) && (blockData != NULL))
		{
			columnAdded = true;
		}

		if (columnBuffers != NULL)
		{
			ColumnBlockBuffers *blockBuffers = columnBuffers->blockBuffersArray[blockIndex];
			StringInfo valueBuffer = NULL;

			/* free previous block's data buffers */
			pfree(blockData->valueBuffer->data);
			pfree(blockData->valueBuffer);

			/* decompress and deserialize current block's data */
			valueBuffer = DecompressBuffer(blockBuffers->valueBuffer,
										   blockBuffers->valueCompressionType);

			if (blockBuffers->valueCompressionType != COMPRESSION_NONE)
			{
				/* compressed data is not needed anymore */
				pfree(blockBuffers->valueBuffer->data);
				pfree(blockBuffers->valueBuffer);
			}

			DeserializeBoolArray(blockBuffers->existsBuffer, blockData->existsArray,
								 rowCount);
			DeserializeDatumArray(valueBuffer, blockData->existsArray,
								  rowCount, attributeForm->attbyval,
								  attributeForm->attlen, attributeForm->attalign,
								  blockData->valueArray);

			/* store current block's data buffer to be freed at next block read */
			blockData->valueBuffer = valueBuffer;
		}
		else if (columnAdded)
		{
			/*
			 * This is a column that was added after creation of this stripe.
			 * So we use either the default value or NULL.
			 */
			if (attributeForm->atthasdef)
			{
				int rowIndex = 0;

				Datum defaultValue = ColumnDefaultValue(tupleDescriptor->constr,
														attributeForm);

				for (rowIndex = 0; rowIndex < rowCount; rowIndex++)
				{
					blockData->existsArray[rowIndex] = true;
					blockData->valueArray[rowIndex] = defaultValue;
				}
			}
			else
			{
				memset(blockData->existsArray, false, rowCount);
			}

		}
	}
}


/*
 * ColumnDefaultValue returns default value for given column. Only const values
 * are supported. The function errors on any other default value expressions.
 */
static Datum
ColumnDefaultValue(TupleConstr *tupleConstraints, Form_pg_attribute attributeForm)
{
	Datum defaultValue = 0;
	Node *defaultValueNode = NULL;
	int defValIndex = 0;

	for (defValIndex = 0; defValIndex < tupleConstraints->num_defval; defValIndex++)
	{
		AttrDefault defaultValue = tupleConstraints->defval[defValIndex];
		if (defaultValue.adnum == attributeForm->attnum)
		{
			defaultValueNode = stringToNode(defaultValue.adbin);
			break;
		}
	}

	Assert(defaultValueNode != NULL);

	/* try reducing the default value node to a const node */
	defaultValueNode = eval_const_expressions(NULL, defaultValueNode);
	if (IsA(defaultValueNode, Const))
	{
		Const *constNode = (Const *) defaultValueNode;
		defaultValue = constNode->constvalue;
	}
	else
	{
		const char *columnName = NameStr(attributeForm->attname);
		ereport(ERROR, (errmsg("unsupported default value for column \"%s\"", columnName),
						errhint("Expression is either mutable or "
								"does not evaluate to constant value")));
	}

	return defaultValue;
}


/* ReadFromFile reads the given segment from the relation data file. */
static StringInfo
ReadFromFile(Relation relation, uint64 offset, uint32 size)
{
	int blockCapacity = CSTORE_PAGE_DATA_SIZE;
	uint32 blockNumber = offset / blockCapacity;
	uint32 blockOffset = offset % blockCapacity;
	uint32 remainingSize = size;
	uint32 resultOffset = 0;
	uint32 blockCount = 0;
	StringInfo resultBuffer = NULL;
	BufferAccessStrategy strategy = GetAccessStrategy(BAS_BULKREAD);

	if (size == 0)
	{
		return NULL;
	}

	resultBuffer = makeStringInfo();
	enlargeStringInfo(resultBuffer, size);

	blockCount = RelationGetNumberOfBlocksInFork(relation, DATA_FORKNUM);
	if (blockNumber > blockCount)
	{
		ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
						errmsg("could not read from file")));
	}

	while (remainingSize > 0)
	{
		Buffer buffer = ReadBufferExtended(relation, DATA_FORKNUM, blockNumber,
										   RBM_NORMAL, strategy);
		Page page = NULL;
		char *pageContents = NULL;
		PageHeader pageHeader = NULL;
		uint32 bufferSize = 0;
		uint32 copySize = remainingSize;

		LockBuffer(buffer, BUFFER_LOCK_SHARE);

		page = BufferGetPage(buffer);
		pageHeader = (PageHeader) page;
		pageContents = PageGetContents(page);
		bufferSize = pageHeader->pd_lower - SizeOfPageHeaderData;

		if (copySize > (bufferSize - blockOffset))
		{
			copySize = bufferSize - blockOffset;
		}

		appendBinaryStringInfo(resultBuffer, pageContents + blockOffset, copySize);
		remainingSize -= copySize;

		UnlockReleaseBuffer(buffer);

		blockNumber++;
		blockOffset = 0;
		resultOffset += copySize;
	}

	return resultBuffer;
}


/*
 * ResetUncompressedBlockData iterates over deserialized column block data
 * and sets valueBuffer field to empty buffer. This field is allocated in stripe
 * memory context and becomes invalid once memory context is reset.
 */
static void
ResetUncompressedBlockData(ColumnBlockData **blockDataArray, uint32 columnCount)
{
	uint32 columnIndex = 0;
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		ColumnBlockData *blockData = blockDataArray[columnIndex];
		if (blockData != NULL)
		{
			blockData->valueBuffer = makeStringInfo();
		}
	}
}
