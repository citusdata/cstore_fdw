/*-------------------------------------------------------------------------
 *
 * cstore_fdw.c
 *
 * This file contains the function definitions for scanning, analyzing, and
 * copying into cstore_fdw foreign tables. Note that this file uses the API
 * provided by cstore_reader and cstore_writer for reading and writing cstore
 * files.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "cstore_fdw.h"

#include <sys/stat.h>
#include <unistd.h>
#include <limits.h>
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/tuptoaster.h"
#include "catalog/pg_attribute.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_namespace.h"
#include "catalog/storage.h"
#include "commands/copy.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "commands/event_trigger.h"
#include "commands/explain.h"
#include "commands/extension.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "parser/parser.h"
#include "parser/parsetree.h"
#include "parser/parse_coerce.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "storage/bufmgr.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#if PG_VERSION_NUM >= 100000
#include "utils/regproc.h"
#endif
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/tqual.h"


#define PREVIOUS_UTILITY (PreviousProcessUtilityHook != NULL \
						  ? PreviousProcessUtilityHook : standard_ProcessUtility)
#if PG_VERSION_NUM >= 100000
#define CALL_PREVIOUS_UTILITY(parseTree, queryString, context, paramListInfo, \
							  destReceiver, completionTag) \
	PREVIOUS_UTILITY(plannedStatement, queryString, context, paramListInfo, \
					 queryEnvironment, destReceiver, completionTag)
#else
#define CALL_PREVIOUS_UTILITY(parseTree, queryString, context, paramListInfo, \
							  destReceiver, completionTag) \
	PREVIOUS_UTILITY(parseTree, queryString, context, paramListInfo, destReceiver, \
					 completionTag)
#endif

/* local functions forward declarations */
static void RegisterCStoreTable(Oid relationId, Oid fileNodeOid);
static void UnregisterCStoreTable(Oid relationId);
static Oid CStoreFileNode(Oid relationId);

#if PG_VERSION_NUM >= 100000
static void CStoreProcessUtility(PlannedStmt *plannedStatement, const char *queryString,
								ProcessUtilityContext context,
								ParamListInfo paramListInfo,
								QueryEnvironment *queryEnvironment,
								DestReceiver *destReceiver, char *completionTag);
#else
static void CStoreProcessUtility(Node *parseTree, const char *queryString,
								ProcessUtilityContext context,
								ParamListInfo paramListInfo,
								DestReceiver *destReceiver, char *completionTag);
#endif
static bool CopyCStoreTableStatement(CopyStmt* copyStatement);
static void CheckSuperuserPrivilegesForCopy(const CopyStmt* copyStatement);
static void CStoreProcessCopyCommand(CopyStmt *copyStatement, const char *queryString,
									 char *completionTag);
static uint64 CopyIntoCStoreTable(const CopyStmt *copyStatement,
								  const char *queryString);
static uint64 CopyOutCStoreTable(CopyStmt* copyStatement, const char* queryString);
static void CStoreProcessAlterTableCommand(AlterTableStmt *alterStatement);
static List * CStoreTableList(void);
static void RemoveRelationStorage(Oid relationId, Oid fileNodeOid);
static List * FindCStoreTables(List *tableList);
static List * OpenRelationsForTruncate(List *cstoreTableList);
static void TruncateCStoreTables(List *cstoreRelationList);
static void InitializeCStoreTableFile(Oid relationId, Relation relation);
static bool CStoreTable(Oid relationId);
static bool CStoreServer(ForeignServer *server);
static bool DistributedTable(Oid relationId);
static bool DistributedWorkerCopy(CopyStmt *copyStatement);
static StringInfo OptionNamesString(Oid currentContextId);
static CStoreFdwOptions * CStoreGetOptions(Oid foreignTableId);
static char * CStoreGetOptionValue(Oid foreignTableId, const char *optionName);
static void ValidateForeignTableOptions(char *compressionTypeString,
										char *stripeRowCountString,
										char *blockRowCountString,
										char *logging);
static CompressionType ParseCompressionType(const char *compressionTypeString);
static void CStoreGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
									Oid foreignTableId);
static void CStoreGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
								  Oid foreignTableId);
#if PG_VERSION_NUM >= 90500
static ForeignScan * CStoreGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel,
										  Oid foreignTableId, ForeignPath *bestPath,
										  List *targetList, List *scanClauses,
										  Plan *outerPlan);
#else
static ForeignScan * CStoreGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel,
										  Oid foreignTableId, ForeignPath *bestPath,
										  List *targetList, List *scanClauses);
#endif
static double TupleCountEstimate(RelOptInfo *baserel, Relation relation);
static BlockNumber PageCount(Relation relation);
static List * ColumnList(RelOptInfo *baserel, Oid foreignTableId);
static void CStoreExplainForeignScan(ForeignScanState *scanState,
									 ExplainState *explainState);
static void CStoreBeginForeignScan(ForeignScanState *scanState, int executorFlags);
static TupleTableSlot * CStoreIterateForeignScan(ForeignScanState *scanState);
static void CStoreEndForeignScan(ForeignScanState *scanState);
static void CStoreReScanForeignScan(ForeignScanState *scanState);
static bool CStoreAnalyzeForeignTable(Relation relation,
									  AcquireSampleRowsFunc *acquireSampleRowsFunc,
									  BlockNumber *totalPageCount);
static int CStoreAcquireSampleRows(Relation relation, int logLevel,
								   HeapTuple *sampleRows, int targetRowCount,
								   double *totalRowCount, double *totalDeadRowCount);
static List * CStorePlanForeignModify(PlannerInfo *plannerInfo, ModifyTable *plan,
									 Index resultRelation, int subplanIndex);
static void CStoreBeginForeignModify(ModifyTableState *modifyTableState,
									 ResultRelInfo *relationInfo, List *fdwPrivate,
									 int subplanIndex, int executorflags);
static TupleTableSlot * CStoreExecForeignInsert(EState *executorState,
												ResultRelInfo *relationInfo,
												TupleTableSlot *tupleSlot,
												TupleTableSlot *planSlot);
static void CStoreEndForeignModify(EState *executorState, ResultRelInfo *relationInfo);


/* declarations for dynamic loading */
PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(cstore_ddl_event_end_trigger);
PG_FUNCTION_INFO_V1(cstore_table_size);
PG_FUNCTION_INFO_V1(cstore_fdw_handler);
PG_FUNCTION_INFO_V1(cstore_fdw_validator);
PG_FUNCTION_INFO_V1(cstore_clean_table_resources);


/* saved hook value in case of unload */
static ProcessUtility_hook_type PreviousProcessUtilityHook = NULL;


/*
 * _PG_init is called when the module is loaded. In this function we save the
 * previous utility hook, and then install our hook to pre-intercept calls to
 * the copy command.
 */
void
_PG_init(void)
{
	PreviousProcessUtilityHook = ProcessUtility_hook;
	ProcessUtility_hook = CStoreProcessUtility;
}


/*
 * _PG_fini is called when the module is unloaded. This function uninstalls the
 * extension's hooks.
 */
void
_PG_fini(void)
{
	ProcessUtility_hook = PreviousProcessUtilityHook;
}


/*
 * cstore_ddl_event_end_trigger is the event trigger function which is called on
 * ddl_command_end event. This function initializes data and footer storage
 * after the CREATE FOREIGN TABLE statement.
 */
Datum
cstore_ddl_event_end_trigger(PG_FUNCTION_ARGS)
{
	EventTriggerData *triggerData = NULL;
	Node *parseTree = NULL;

	/* error if event trigger manager did not call this function */
	if (!CALLED_AS_EVENT_TRIGGER(fcinfo))
	{
		ereport(ERROR, (errmsg("trigger not fired by event trigger manager")));
	}

	triggerData = (EventTriggerData *) fcinfo->context;
	parseTree = triggerData->parsetree;

	if (nodeTag(parseTree) == T_CreateForeignTableStmt)
	{
		CreateForeignTableStmt *createStatement = (CreateForeignTableStmt *) parseTree;
		char *serverName = createStatement->servername;

		bool missingOK = false;
		ForeignServer *server = GetForeignServerByName(serverName, missingOK);
		if (CStoreServer(server))
		{
			Oid relationId = RangeVarGetRelid(createStatement->base.relation,
											  AccessShareLock, false);
			Relation relation = heap_open(relationId, AccessExclusiveLock);

			RelationCreateStorage(relation->rd_node, relation->rd_rel->relpersistence);

			InitializeCStoreTableFile(relationId, relation);
			heap_close(relation, AccessExclusiveLock);

			RegisterCStoreTable(relationId, relation->rd_node.relNode);
		}
	}

	PG_RETURN_NULL();
}


/*
 * RegisterCStoreTable registers provided cstore table relation id by
 * adding a new row to pg_cstore_tables.
 */
static void
RegisterCStoreTable(Oid relationId, Oid fileNodeOid)
{
	Oid pgCstoreTablesOid = InvalidOid;
	Relation pgCstoreTables = NULL;
	TupleDesc tupleDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	Datum values[] = { ObjectIdGetDatum(relationId), ObjectIdGetDatum(fileNodeOid) };
	bool isNulls[] = { false, false };

	pgCstoreTablesOid = get_relname_relid(CSTORE_TABLES_NAME, PG_CATALOG_NAMESPACE);
	if (pgCstoreTablesOid == InvalidOid)
	{
		ereport(ERROR, (errmsg("could not find table %s", CSTORE_TABLES_NAME)));
	}

	/* open shard relation and insert new tuple */
	pgCstoreTables = heap_open(pgCstoreTablesOid, RowExclusiveLock);
	tupleDescriptor = RelationGetDescr(pgCstoreTables);
	Assert(tupleDescriptor->natts == 2);
	heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

#if PG_VERSION_NUM >= 100000
	CatalogTupleInsert(pgCstoreTables, heapTuple);
#else
	simple_heap_insert(pgCstoreTables, heapTuple);
	CatalogUpdateIndexes(pgCstoreTables, heapTuple);
#endif

	heap_close(pgCstoreTables, NoLock);
}


/*
 * UnregisterCStoreTable unregisters a cstore_fdw table by removing
 * the row from table pg_cstore_tables.
 */
static void
UnregisterCStoreTable(Oid relationId)
{
	Oid pgCstoreTablesOid = InvalidOid;
	Relation pgCstoreTables = NULL;
	HeapTuple heapTuple = NULL;
	int scanKeyCount = 1;
	ScanKeyData scanKey[1];
	HeapScanDesc scanDescriptor = NULL;

	pgCstoreTablesOid = get_relname_relid(CSTORE_TABLES_NAME, PG_CATALOG_NAMESPACE);
	if (pgCstoreTablesOid == InvalidOid)
	{
		ereport(ERROR, (errmsg("could not find table %s", CSTORE_TABLES_NAME)));
	}

	pgCstoreTables = heap_open(pgCstoreTablesOid, RowExclusiveLock);

	ScanKeyInit(&scanKey[0], 1, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relationId));

	scanDescriptor = heap_beginscan(pgCstoreTables, GetTransactionSnapshot(),
									scanKeyCount, scanKey);

	heapTuple = heap_getnext(scanDescriptor, ForwardScanDirection);

	while (HeapTupleIsValid(heapTuple))
	{
		simple_heap_delete(pgCstoreTables, &heapTuple->t_self);
		heapTuple = heap_getnext(scanDescriptor, ForwardScanDirection);
	}

	heap_endscan(scanDescriptor);

	heap_close(pgCstoreTables, NoLock);
}


/*
 * CStoreFileNode returns file node oid for given cstore relation id.
 * The function returns InvalidOid if no cstore table is registered with this
 * relation id.
 */
static Oid
CStoreFileNode(Oid relationId)
{
	Oid fileNodeOid = InvalidOid;
	Oid pgCstoreTablesOid = InvalidOid;
	Relation pgCstoreTables = NULL;
	HeapTuple heapTuple = NULL;
	int scanKeyCount = 1;
	ScanKeyData scanKey[1];
	HeapScanDesc scanDescriptor = NULL;

	pgCstoreTablesOid = get_relname_relid(CSTORE_TABLES_NAME, PG_CATALOG_NAMESPACE);

	if (pgCstoreTablesOid == InvalidOid)
	{
		ereport(ERROR, (errmsg("could not find table %s", CSTORE_TABLES_NAME)));
	}

	pgCstoreTables = heap_open(pgCstoreTablesOid, RowExclusiveLock);

	ScanKeyInit(&scanKey[0], 1, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relationId));

	scanDescriptor = heap_beginscan(pgCstoreTables, GetTransactionSnapshot(),
									scanKeyCount, scanKey);

	heapTuple = heap_getnext(scanDescriptor, ForwardScanDirection);

	if (HeapTupleIsValid(heapTuple))
	{
		TupleDesc tupleDescriptor = RelationGetDescr(pgCstoreTables);
		bool isNull = false;
		Datum fileNodeDatum = heap_getattr(heapTuple, 2,
										   tupleDescriptor, &isNull);

		Assert(!isNull);

		fileNodeOid = DatumGetObjectId(fileNodeDatum);
	}

	heap_endscan(scanDescriptor);
	heap_close(pgCstoreTables, NoLock);

	return fileNodeOid;
}


/*
 * CStoreProcessUtility is the hook for handling utility commands. This function
 * customizes the behaviour of "COPY cstore_table" and "DROP FOREIGN TABLE
 * cstore_table" commands. For all other utility statements, the function calls
 * the previous utility hook or the standard utility command via macro
 * CALL_PREVIOUS_UTILITY.
 */
#if PG_VERSION_NUM >= 100000
static void
CStoreProcessUtility(PlannedStmt *plannedStatement, const char *queryString,
					 ProcessUtilityContext context,
					 ParamListInfo paramListInfo,
					 QueryEnvironment *queryEnvironment,
					 DestReceiver *destReceiver, char *completionTag)
#else
static void
CStoreProcessUtility(Node * parseTree, const char *queryString,
					 ProcessUtilityContext context,
					 ParamListInfo paramListInfo,
					 DestReceiver *destReceiver, char *completionTag)
#endif
{
#if PG_VERSION_NUM >= 100000
	Node *parseTree = plannedStatement->utilityStmt;
#endif

	if (nodeTag(parseTree) == T_CopyStmt)
	{
		CopyStmt *copyStatement = (CopyStmt *) parseTree;

		if (CopyCStoreTableStatement(copyStatement))
		{
			CStoreProcessCopyCommand(copyStatement, queryString, completionTag);
		}
		else
		{
			CALL_PREVIOUS_UTILITY(parseTree, queryString, context, paramListInfo,
								  destReceiver, completionTag);
		}
	}
	else if (nodeTag(parseTree) == T_DropStmt)
	{
		DropStmt *dropStmt = (DropStmt *) parseTree;

		if (dropStmt->removeType == OBJECT_EXTENSION)
		{
			ListCell *objectCell = NULL;
			List *cstoreTableList = NIL;
			List *cstoreFileNodeList = NIL;
			ListCell *relationIdCell = NULL;
			ListCell *fileNodeCell = NULL;

			foreach(objectCell, dropStmt->objects)
			{
				Node *object = (Node *) lfirst(objectCell);
				char *objectName = NULL;

#if PG_VERSION_NUM >= 100000
				Assert(IsA(object, String));
				objectName = strVal(object);
#else
				Assert(IsA(object, List));
				objectName = strVal(linitial((List *) object));
#endif
				/* record cstore relation ids if cstore extension is to be dropped */
				if (strncmp(CSTORE_FDW_NAME, objectName, NAMEDATALEN) == 0)
				{
					cstoreTableList = CStoreTableList();
				}
			}

			foreach(relationIdCell, cstoreTableList)
			{
				Oid relationId = lfirst_oid(relationIdCell);
				cstoreFileNodeList = lappend_oid(cstoreFileNodeList,
												 CStoreFileNode(relationId));
			}


			CALL_PREVIOUS_UTILITY(parseTree, queryString, context,
					paramListInfo, destReceiver, completionTag);

			/* mark cached cstore relations' storage for deletion*/
			forboth(relationIdCell, cstoreTableList, fileNodeCell, cstoreFileNodeList)
			{
				Oid relationId = lfirst_oid(relationIdCell);
				Oid fileNodeOid = lfirst_oid(fileNodeCell);
				RemoveRelationStorage(relationId, fileNodeOid);
			}
		}
		else
		{
			CALL_PREVIOUS_UTILITY(parseTree, queryString, context,
					paramListInfo, destReceiver, completionTag);
		}
	}
	else if (nodeTag(parseTree) == T_TruncateStmt)
	{
		TruncateStmt *truncateStatement = (TruncateStmt *) parseTree;
		List *allTablesList = truncateStatement->relations;
		List *cstoreTablesList = FindCStoreTables(allTablesList);
		List *otherTablesList = list_difference(allTablesList, cstoreTablesList);
		List *cstoreRelationList = OpenRelationsForTruncate(cstoreTablesList);
		ListCell *cstoreRelationCell = NULL;

		if (otherTablesList != NIL)
		{
			truncateStatement->relations = otherTablesList;

			CALL_PREVIOUS_UTILITY(parseTree, queryString, context, paramListInfo,
								  destReceiver, completionTag);
		}

		TruncateCStoreTables(cstoreRelationList);

		foreach(cstoreRelationCell, cstoreRelationList)
		{
			Relation relation = (Relation) lfirst(cstoreRelationCell);
			heap_close(relation, AccessExclusiveLock);
		}
	}
	else if (nodeTag(parseTree) == T_AlterTableStmt)
	{
		AlterTableStmt *alterTable = (AlterTableStmt *) parseTree;
		CStoreProcessAlterTableCommand(alterTable);
		CALL_PREVIOUS_UTILITY(parseTree, queryString, context, paramListInfo,
							  destReceiver, completionTag);
	}
	/* handle other utility statements */
	else
	{
		CALL_PREVIOUS_UTILITY(parseTree, queryString, context, paramListInfo,
							  destReceiver, completionTag);
	}
}


/*
 * CopyCStoreTableStatement check whether the COPY statement is a "COPY cstore_table FROM
 * ..." or "COPY cstore_table TO ...." statement. If it is then the function returns
 * true. The function returns false otherwise.
 */
static bool
CopyCStoreTableStatement(CopyStmt* copyStatement)
{
	bool copyCStoreTableStatement = false;

	if (copyStatement->relation != NULL)
	{
		Oid relationId = RangeVarGetRelid(copyStatement->relation,
										  AccessShareLock, true);
		bool cstoreTable = CStoreTable(relationId);
		if (cstoreTable)
		{
			bool distributedTable = DistributedTable(relationId);
			bool distributedCopy = DistributedWorkerCopy(copyStatement);

			if (distributedTable || distributedCopy)
			{
				/* let COPY on distributed tables fall through to Citus */
				copyCStoreTableStatement = false;
			}
			else
			{
				copyCStoreTableStatement = true;
			}
		}
	}

	return copyCStoreTableStatement;
}


/*
 * CheckSuperuserPrivilegesForCopy checks if superuser privilege is required by
 * copy operation and reports error if user does not have superuser rights.
 */
static void
CheckSuperuserPrivilegesForCopy(const CopyStmt* copyStatement)
{
	/*
	 * We disallow copy from file or program except to superusers. These checks
	 * are based on the checks in DoCopy() function of copy.c.
	 */
	if (copyStatement->filename != NULL && !superuser())
	{
		if (copyStatement->is_program)
		{
			ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to COPY to or from a program"),
					 errhint("Anyone can COPY to stdout or from stdin. "
							 "psql's \\copy command also works for anyone.")));
		}
		else
		{
			ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to COPY to or from a file"),
					 errhint("Anyone can COPY to stdout or from stdin. "
							 "psql's \\copy command also works for anyone.")));
		}
	}
}


/*
 * CStoreProcessCopyCommand handles COPY <cstore_table> FROM/TO ... statements.
 * It determines the copy direction and forwards execution to appropriate function.
 */
static void
CStoreProcessCopyCommand(CopyStmt *copyStatement, const char* queryString,
						 char *completionTag)
{
	uint64 processedCount = 0;

	if (copyStatement->is_from)
	{
		processedCount = CopyIntoCStoreTable(copyStatement, queryString);
	}
	else
	{
		processedCount = CopyOutCStoreTable(copyStatement, queryString);
	}

	if (completionTag != NULL)
	{
		snprintf(completionTag, COMPLETION_TAG_BUFSIZE, "COPY " UINT64_FORMAT,
				 processedCount);
	}
}


/*
 * CopyIntoCStoreTable handles a "COPY cstore_table FROM" statement. This
 * function uses the COPY command's functions to read and parse rows from
 * the data source specified in the COPY statement. The function then writes
 * each row to the file specified in the cstore foreign table options. Finally,
 * the function returns the number of copied rows.
 */
static uint64
CopyIntoCStoreTable(const CopyStmt *copyStatement, const char *queryString)
{
	uint64 processedRowCount = 0;
	Relation relation = NULL;
	Oid relationId = InvalidOid;
	TupleDesc tupleDescriptor = NULL;
	uint32 columnCount = 0;
	CopyState copyState = NULL;
	bool nextRowFound = true;
	Datum *columnValues = NULL;
	bool *columnNulls = NULL;
	TableWriteState *writeState = NULL;
	CStoreFdwOptions *cstoreFdwOptions = NULL;
	MemoryContext tupleContext = NULL;

	/* Only superuser can copy from or to local file */
	CheckSuperuserPrivilegesForCopy(copyStatement);

	Assert(copyStatement->relation != NULL);

	/*
	 * Open and lock the relation. We acquire ShareUpdateExclusiveLock to allow
	 * concurrent reads, but block concurrent writes.
	 */
	relation = heap_openrv(copyStatement->relation, ShareUpdateExclusiveLock);
	relationId = RelationGetRelid(relation);

	/* allocate column values and nulls arrays */
	tupleDescriptor = RelationGetDescr(relation);
	columnCount = tupleDescriptor->natts;
	columnValues = palloc0(columnCount * sizeof(Datum));
	columnNulls = palloc0(columnCount * sizeof(bool));

	cstoreFdwOptions = CStoreGetOptions(relationId);

	/*
	 * We create a new memory context called tuple context, and read and write
	 * each row's values within this memory context. After each read and write,
	 * we reset the memory context. That way, we immediately release memory
	 * allocated for each row, and don't bloat memory usage with large input
	 * files.
	 */
	tupleContext = AllocSetContextCreate(CurrentMemoryContext,
										 "CStore COPY Row Memory Context",
										 ALLOCSET_DEFAULT_MINSIZE,
										 ALLOCSET_DEFAULT_INITSIZE,
										 ALLOCSET_DEFAULT_MAXSIZE);

	/* init state to read from COPY data source */
#if (PG_VERSION_NUM >= 100000)
	{
		ParseState *pstate = make_parsestate(NULL);
		pstate->p_sourcetext = queryString;

		copyState = BeginCopyFrom(pstate, relation, copyStatement->filename,
								  copyStatement->is_program,
								  NULL,
								  copyStatement->attlist,
								  copyStatement->options);
		free_parsestate(pstate);
	}
#else
	copyState = BeginCopyFrom(relation, copyStatement->filename,
							  copyStatement->is_program,
							  copyStatement->attlist,
							  copyStatement->options);
#endif

	/* init state to write to the cstore file */
	writeState = CStoreBeginWrite(relation, cstoreFdwOptions->compressionType,
								  cstoreFdwOptions->stripeRowCount,
								  cstoreFdwOptions->blockRowCount,
								  cstoreFdwOptions->logging,
								  tupleDescriptor);

	while (nextRowFound)
	{
		/* read the next row in tupleContext */
		MemoryContext oldContext = MemoryContextSwitchTo(tupleContext);
		nextRowFound = NextCopyFrom(copyState, NULL, columnValues, columnNulls, NULL);
		MemoryContextSwitchTo(oldContext);

		/* write the row to the cstore file */
		if (nextRowFound)
		{
			CStoreWriteRow(writeState, columnValues, columnNulls);
			processedRowCount++;
		}

		MemoryContextReset(tupleContext);

		CHECK_FOR_INTERRUPTS();
	}

	/* end read/write sessions and close the relation */
	EndCopyFrom(copyState);
	CStoreEndWrite(writeState);
	heap_close(relation, ShareUpdateExclusiveLock);

	return processedRowCount;
}


/*
 * CopyFromCStoreTable handles a "COPY cstore_table TO ..." statement. Statement
 * is converted to "COPY (SELECT * FROM cstore_table) TO ..." and forwarded to
 * postgres native COPY handler. Function returns number of files copied to external
 * stream. Copying selected columns from cstore table is not currently supported.
 */
static uint64
CopyOutCStoreTable(CopyStmt* copyStatement, const char* queryString)
{
	uint64 processedCount = 0;
	RangeVar *relation = NULL;
	char *qualifiedName = NULL;
	List *queryList = NIL;
	Node *rawQuery = NULL;

	StringInfo newQuerySubstring = makeStringInfo();

	if (copyStatement->attlist != NIL)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("copy column list is not supported"),
						errhint("use 'copy (select <columns> from <table>) to "
								"...' instead")));
	}

	relation = copyStatement->relation;
	qualifiedName = quote_qualified_identifier(relation->schemaname,
											   relation->relname);
	appendStringInfo(newQuerySubstring, "select * from %s", qualifiedName);
	queryList = raw_parser(newQuerySubstring->data);

	/* take the first parse tree */
	rawQuery = linitial(queryList);

	/*
	 * Set the relation field to NULL so that COPY command works on
	 * query field instead.
	 */
	copyStatement->relation = NULL;

#if (PG_VERSION_NUM >= 100000)
	/*
	 * raw_parser returns list of RawStmt* in PG 10+ we need to
	 * extract actual query from it.
	 */
	{
		ParseState *pstate = make_parsestate(NULL);
		RawStmt *rawStatement = (RawStmt *) rawQuery;

		pstate->p_sourcetext = newQuerySubstring->data;
		copyStatement->query = rawStatement->stmt;

		DoCopy(pstate, copyStatement, -1, -1, &processedCount);
		free_parsestate(pstate);
	}
#else
	copyStatement->query = rawQuery;

	DoCopy(copyStatement, queryString, &processedCount);
#endif

	return processedCount;
}


/*
 * CStoreProcessAlterTableCommand checks if given alter table statement is
 * compatible with underlying data structure. Currently it only checks alter
 * column type. The function errors out if current column type can not be safely
 * converted to requested column type. This check is more restrictive than
 * PostgreSQL's because we can not change existing data.
 */
static void
CStoreProcessAlterTableCommand(AlterTableStmt *alterStatement)
{
	ObjectType objectType = alterStatement->relkind;
	RangeVar *relationRangeVar = alterStatement->relation;
	Oid relationId = InvalidOid;
	List *commandList = alterStatement->cmds;
	ListCell *commandCell = NULL;

	/* we are only interested in foreign table changes */
	if (objectType != OBJECT_TABLE && objectType != OBJECT_FOREIGN_TABLE)
	{
		return;
	}

	relationId = RangeVarGetRelid(relationRangeVar, AccessShareLock, true);
	if (!CStoreTable(relationId))
	{
		return;
	}

	foreach(commandCell, commandList)
	{
		AlterTableCmd *alterCommand = (AlterTableCmd *) lfirst(commandCell);
		if(alterCommand->subtype == AT_AlterColumnType)
		{
			char *columnName = alterCommand->name;
			ColumnDef *columnDef = (ColumnDef *) alterCommand->def;
			Oid targetTypeId = typenameTypeId(NULL, columnDef->typeName);
			char *typeName = TypeNameToString(columnDef->typeName);
			AttrNumber attributeNumber = get_attnum(relationId, columnName);
			Oid currentTypeId = InvalidOid;

			if (attributeNumber <= 0)
			{
				/* let standard utility handle this */
				continue;
			}

			currentTypeId = get_atttype(relationId, attributeNumber);

			/*
			 * We are only interested in implicit coersion type compatibility.
			 * Erroring out here to prevent further processing.
			 */
			if (!can_coerce_type(1, &currentTypeId, &targetTypeId, COERCION_IMPLICIT))
			{
				ereport(ERROR, (errmsg("Column %s cannot be cast automatically to "
									   "type %s", columnName, typeName)));
			}
		}
	}
}


/* CStoreTableList returns oids of cstore_fdw tables stored in pg_cstore_tables */
static List *
CStoreTableList(void)
{
	List *cstoreRelationIdList = NIL;
	Oid cstoreTablesOid = InvalidOid;
	Relation heapRelation = NULL;
	HeapScanDesc scanDesc = NULL;
	HeapTuple heapTuple = NULL;
	TupleDesc tupleDescriptor = NULL;

	cstoreTablesOid = get_relname_relid(CSTORE_TABLES_NAME, PG_CATALOG_NAMESPACE);
	if (cstoreTablesOid == InvalidOid)
	{
		/* the pg_cstore_tables table does not exist */
		return NIL;
	}

	heapRelation = heap_open(cstoreTablesOid, AccessExclusiveLock);
	tupleDescriptor = RelationGetDescr(heapRelation);

	scanDesc = heap_beginscan(heapRelation, GetTransactionSnapshot(), 0, NULL);

	heapTuple = heap_getnext(scanDesc, ForwardScanDirection);
	while (HeapTupleIsValid(heapTuple))
	{
		bool isNull = false;
		Datum logicalrelidDatum = heap_getattr(heapTuple, 1, tupleDescriptor, &isNull);
		Oid relationId = DatumGetObjectId(logicalrelidDatum);

		cstoreRelationIdList = lappend_oid(cstoreRelationIdList, relationId);

		heapTuple = heap_getnext(scanDesc, ForwardScanDirection);
	}

	heap_endscan(scanDesc);
	heap_close(heapRelation, NoLock);

	return cstoreRelationIdList;
}


/*
 * RemoveRelationStorage marks storage nodes for a relation for deletion.
 *
 * Postgres does not mark foreign tables' file nodes for deletion since
 * it never creates it. Here we create a fake relation, associate this
 * relation with cstore tables file node, then mark this relation for
 * deletion so that file node is dropped when the transaction commits.
 */
static void
RemoveRelationStorage(Oid relationId, Oid fileNodeOid)
{
	elog(WARNING, "Skipping RelationDropStorage ...");
	// RelFileNode relFileNode = { MyDatabaseTableSpace, MyDatabaseId, fileNodeOid };
	// Relation relation = palloc0(sizeof(RelationData));
	// relation->rd_node = relFileNode;
	// relation->rd_backend = InvalidBackendId;

	// RelationDropStorage(relation);
}


/* FindCStoreTables returns list of CStore tables from given table list */
static List *
FindCStoreTables(List *tableList)
{
	List *cstoreTableList = NIL;
	ListCell *relationCell = NULL;
	foreach(relationCell, tableList)
	{
		RangeVar *rangeVar = (RangeVar *) lfirst(relationCell);
		Oid relationId = RangeVarGetRelid(rangeVar, AccessShareLock, true);
		if (CStoreTable(relationId) && !DistributedTable(relationId))
		{
			cstoreTableList = lappend(cstoreTableList, rangeVar);
		}
	}

	return cstoreTableList;
}


/* 
 * OpenRelationsForTruncate opens and locks relations for tables to be truncated.
 *
 * It also performs a permission checks to see if the user has truncate privilege
 * on tables.
 */
static List *
OpenRelationsForTruncate(List *cstoreTableList)
{
	ListCell *relationCell = NULL;
	List *relationIdList = NIL;
	List *relationList = NIL;
	foreach(relationCell, cstoreTableList)
	{
		RangeVar *rangeVar = (RangeVar *) lfirst(relationCell);
		Relation relation = heap_openrv(rangeVar, AccessExclusiveLock);
		Oid relationId = relation->rd_id;
		AclResult aclresult = pg_class_aclcheck(relationId, GetUserId(),
											   ACL_TRUNCATE);
		if (aclresult != ACLCHECK_OK)
		{
			aclcheck_error(aclresult, ACL_KIND_CLASS, get_rel_name(relationId));
		}

		/* check if this relation is repeated */
		if (list_member_oid(relationIdList, relationId))
		{
			heap_close(relation, AccessExclusiveLock);
		}
		else
		{
			relationIdList = lappend_oid(relationIdList, relationId);
			relationList = lappend(relationList, relation);
		}
	}

	return relationList;
}


/* TruncateCStoreTable truncates given cstore tables */
static void
TruncateCStoreTables(List *cstoreRelationList)
{
	ListCell *relationCell = NULL;

	MultiXactId minmulti = GetOldestMultiXactId();

	foreach(relationCell, cstoreRelationList)
	{
		Relation relation = (Relation) lfirst(relationCell);
		Oid relationId = relation->rd_id;

		Assert(CStoreTable(relationId));

		RelationSetNewRelfilenode(relation,
#if PG_VERSION_NUM >= 90500
								  relation->rd_rel->relpersistence,
#endif
								  RecentXmin, minmulti);

		UnregisterCStoreTable(relationId);
		RegisterCStoreTable(relationId, relation->rd_node.relNode);

		InitializeCStoreTableFile(relationId, relation);
	}
}


/*
 * InitializeCStoreTableFile creates an initializes data and footer files
 * for a cstore table. Files are allocated in postgres internal storage.
 */
static void
InitializeCStoreTableFile(Oid relationId, Relation relation)
{
	TableWriteState *writeState = NULL;
	TupleDesc tupleDescriptor = RelationGetDescr(relation);
	CStoreFdwOptions *cstoreFdwOptions = CStoreGetOptions(relationId);

	/*
	 * Initialize state to write to the cstore file. This creates an
	 * empty data file and a valid footer file for the table.
	 */
	writeState = CStoreBeginWrite(relation,
								  cstoreFdwOptions->compressionType,
								  cstoreFdwOptions->stripeRowCount,
								  cstoreFdwOptions->blockRowCount,
								  cstoreFdwOptions->logging,
								  tupleDescriptor);

	CStoreEndWrite(writeState);
}


/*
 * CStoreTable checks if the given table name belongs to a foreign columnar store
 * table. If it does, the function returns true. Otherwise, it returns false.
 */
static bool
CStoreTable(Oid relationId)
{
	bool cstoreTable = false;
	char relationKind = 0;

	if (relationId == InvalidOid)
	{
		return false;
	}

	relationKind = get_rel_relkind(relationId);
	if (relationKind == RELKIND_FOREIGN_TABLE)
	{
		ForeignTable *foreignTable = GetForeignTable(relationId);
		ForeignServer *server = GetForeignServer(foreignTable->serverid);
		if (CStoreServer(server))
		{
			cstoreTable = true;
		}
	}

	return cstoreTable;
}


/*
 * CStoreServer checks if the given foreign server belongs to cstore_fdw. If it
 * does, the function returns true. Otherwise, it returns false.
 */
static bool
CStoreServer(ForeignServer *server)
{
	ForeignDataWrapper *foreignDataWrapper = GetForeignDataWrapper(server->fdwid);
	bool cstoreServer = false;

	char *foreignWrapperName = foreignDataWrapper->fdwname;
	if (strncmp(foreignWrapperName, CSTORE_FDW_NAME, NAMEDATALEN) == 0)
	{
		cstoreServer = true;
	}

	return cstoreServer;
}


/*
 * DistributedTable checks if the given relationId is the OID of a distributed table,
 * which may also be a cstore_fdw table, but in that case COPY should be handled by
 * Citus.
 */
static bool
DistributedTable(Oid relationId)
{
	bool distributedTable = false;
	Oid partitionOid = InvalidOid;
	Relation heapRelation = NULL;
	HeapScanDesc scanDesc = NULL;
	const int scanKeyCount = 1;
	ScanKeyData scanKey[scanKeyCount];
	HeapTuple heapTuple = NULL;

	bool missingOK = true;
	Oid extensionOid = get_extension_oid(CITUS_EXTENSION_NAME, missingOK);
	if (extensionOid == InvalidOid)
	{
		/* if the citus extension isn't created, no tables are distributed */
		return false;
	}

	partitionOid = get_relname_relid(CITUS_PARTITION_TABLE_NAME, PG_CATALOG_NAMESPACE);
	if (partitionOid == InvalidOid)
	{
		/* the pg_dist_partition table does not exist */
		return false;
	}

	heapRelation = heap_open(partitionOid, AccessShareLock);

	ScanKeyInit(&scanKey[0], ATTR_NUM_PARTITION_RELATION_ID, InvalidStrategy,
				F_OIDEQ, ObjectIdGetDatum(relationId));

	scanDesc = heap_beginscan(heapRelation, SnapshotSelf, scanKeyCount, scanKey);

	heapTuple = heap_getnext(scanDesc, ForwardScanDirection);

	distributedTable = HeapTupleIsValid(heapTuple);

	heap_endscan(scanDesc);
	relation_close(heapRelation, AccessShareLock);

	return distributedTable;
}


/*
 * DistributedWorkerCopy returns whether the Citus-specific master_host option is
 * present in the COPY options.
 */
static bool
DistributedWorkerCopy(CopyStmt *copyStatement)
{
	ListCell *optionCell = NULL;
	foreach(optionCell, copyStatement->options)
	{
		DefElem *defel = (DefElem *) lfirst(optionCell);
		if (strncmp(defel->defname, "master_host", NAMEDATALEN) == 0)
		{
			return true;
		}
	}

	return false;
}


/*
 * cstore_table_size returns the total on-disk size of a cstore table in bytes.
 * The result includes the sizes of data and footer.
 */
Datum
cstore_table_size(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);

	int64 tableSize = 0;
	BlockNumber pageCount = 0;

	Relation relation = NULL;

	bool cstoreTable = CStoreTable(relationId);
	if (!cstoreTable)
	{
		ereport(ERROR, (errmsg("relation is not a cstore table")));
	}

	relation = heap_open(relationId, AccessShareLock);
	pageCount = PageCount(relation);
	heap_close(relation, NoLock);

	tableSize = pageCount * BLCKSZ;

	PG_RETURN_INT64(tableSize);
}


/*
 * cstore_fdw_handler creates and returns a struct with pointers to foreign
 * table callback functions.
 */
Datum
cstore_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwRoutine = makeNode(FdwRoutine);

	fdwRoutine->GetForeignRelSize = CStoreGetForeignRelSize;
	fdwRoutine->GetForeignPaths = CStoreGetForeignPaths;
	fdwRoutine->GetForeignPlan = CStoreGetForeignPlan;
	fdwRoutine->ExplainForeignScan = CStoreExplainForeignScan;
	fdwRoutine->BeginForeignScan = CStoreBeginForeignScan;
	fdwRoutine->IterateForeignScan = CStoreIterateForeignScan;
	fdwRoutine->ReScanForeignScan = CStoreReScanForeignScan;
	fdwRoutine->EndForeignScan = CStoreEndForeignScan;
	fdwRoutine->AnalyzeForeignTable = CStoreAnalyzeForeignTable;
	fdwRoutine->PlanForeignModify = CStorePlanForeignModify;
	fdwRoutine->BeginForeignModify = CStoreBeginForeignModify;
	fdwRoutine->ExecForeignInsert = CStoreExecForeignInsert;
	fdwRoutine->EndForeignModify = CStoreEndForeignModify;

	PG_RETURN_POINTER(fdwRoutine);
}


/*
 * cstore_fdw_validator validates options given to one of the following commands:
 * foreign data wrapper, server, user mapping, or foreign table. This function
 * errors out if the given option name or its value is considered invalid.
 */
Datum
cstore_fdw_validator(PG_FUNCTION_ARGS)
{
	Datum optionArray = PG_GETARG_DATUM(0);
	Oid optionContextId = PG_GETARG_OID(1);
	List *optionList = untransformRelOptions(optionArray);
	ListCell *optionCell = NULL;
	char *compressionTypeString = NULL;
	char *stripeRowCountString = NULL;
	char *blockRowCountString = NULL;
	char *loggingString = NULL;

	foreach(optionCell, optionList)
	{
		DefElem *optionDef = (DefElem *) lfirst(optionCell);
		char *optionName = optionDef->defname;
		bool optionValid = false;

		int32 optionIndex = 0;
		for (optionIndex = 0; optionIndex < ValidOptionCount; optionIndex++)
		{
			const CStoreValidOption *validOption = &(ValidOptionArray[optionIndex]);

			if ((optionContextId == validOption->optionContextId) &&
				(strncmp(optionName, validOption->optionName, NAMEDATALEN) == 0))
			{
				optionValid = true;
				break;
			}
		}

		/* if invalid option, display an informative error message */
		if (!optionValid)
		{
			StringInfo optionNamesString = OptionNamesString(optionContextId);

			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
							errmsg("invalid option \"%s\"", optionName),
							errhint("Valid options in this context are: %s",
									optionNamesString->data)));
		}

		if (strncmp(optionName, OPTION_NAME_COMPRESSION_TYPE, NAMEDATALEN) == 0)
		{
			compressionTypeString = defGetString(optionDef);
		}
		else if (strncmp(optionName, OPTION_NAME_STRIPE_ROW_COUNT, NAMEDATALEN) == 0)
		{
			stripeRowCountString = defGetString(optionDef);
		}
		else if (strncmp(optionName, OPTION_NAME_BLOCK_ROW_COUNT, NAMEDATALEN) == 0)
		{
			blockRowCountString = defGetString(optionDef);
		}
		else if (strncmp(optionName, OPTION_NAME_LOGGING, NAMEDATALEN) == 0)
		{
			loggingString = defGetString(optionDef);
		}
	}

	if (optionContextId == ForeignTableRelationId)
	{
		ValidateForeignTableOptions(compressionTypeString,
									stripeRowCountString, blockRowCountString,
									loggingString);
	}

	PG_RETURN_VOID();
}


/*
 * cstore_clean_table_resources cleans up table data and metadata with provided
 * relation id. It also removes entry in table pg_cstore_tables.
 *
 * The function is meant to be called from drop_event_trigger. Although it is
 * not enforced.
 */
Datum
cstore_clean_table_resources(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);
	Oid fileNodeOid = CStoreFileNode(relationId);
	RemoveRelationStorage(relationId, fileNodeOid);
	UnregisterCStoreTable(relationId);

	PG_RETURN_VOID();
}


/*
 * OptionNamesString finds all options that are valid for the current context,
 * and concatenates these option names in a comma separated string. The function
 * is unchanged from mongo_fdw.
 */
static StringInfo
OptionNamesString(Oid currentContextId)
{
	StringInfo optionNamesString = makeStringInfo();
	bool firstOptionAppended = false;

	int32 optionIndex = 0;
	for (optionIndex = 0; optionIndex < ValidOptionCount; optionIndex++)
	{
		const CStoreValidOption *validOption = &(ValidOptionArray[optionIndex]);

		/* if option belongs to current context, append option name */
		if (currentContextId == validOption->optionContextId)
		{
			if (firstOptionAppended)
			{
				appendStringInfoString(optionNamesString, ", ");
			}

			appendStringInfoString(optionNamesString, validOption->optionName);
			firstOptionAppended = true;
		}
	}

	return optionNamesString;
}


/*
 * CStoreGetOptions returns the option values to be used when reading and writing
 * the cstore file. To resolve these values, the function checks options for the
 * foreign table, and if not present, falls back to default values. This function
 * errors out if given option values are considered invalid.
 */
static CStoreFdwOptions *
CStoreGetOptions(Oid foreignTableId)
{
	CStoreFdwOptions *cstoreFdwOptions = NULL;
	CompressionType compressionType = DEFAULT_COMPRESSION_TYPE;
	int32 stripeRowCount = DEFAULT_STRIPE_ROW_COUNT;
	int32 blockRowCount = DEFAULT_BLOCK_ROW_COUNT;
	bool logging = true;
	char *compressionTypeString = NULL;
	char *stripeRowCountString = NULL;
	char *blockRowCountString = NULL;
	char *loggingString = NULL;

	compressionTypeString = CStoreGetOptionValue(foreignTableId,
												 OPTION_NAME_COMPRESSION_TYPE);
	stripeRowCountString = CStoreGetOptionValue(foreignTableId,
												OPTION_NAME_STRIPE_ROW_COUNT);
	blockRowCountString = CStoreGetOptionValue(foreignTableId,
											   OPTION_NAME_BLOCK_ROW_COUNT);

	loggingString = CStoreGetOptionValue(foreignTableId,
										 OPTION_NAME_LOGGING);

	ValidateForeignTableOptions(compressionTypeString,
								stripeRowCountString, blockRowCountString, loggingString);

	/* parse provided options */
	if (compressionTypeString != NULL)
	{
		compressionType = ParseCompressionType(compressionTypeString);
	}
	if (stripeRowCountString != NULL)
	{
		stripeRowCount = pg_atoi(stripeRowCountString, sizeof(int32), 0);
	}
	if (blockRowCountString != NULL)
	{
		blockRowCount = pg_atoi(blockRowCountString, sizeof(int32), 0);
	}
	if (loggingString != NULL)
	{
		logging = strncmp(loggingString, LOGGING_STRING_TRUE, NAMEDATALEN) == 0;
	}

	cstoreFdwOptions = palloc0(sizeof(CStoreFdwOptions));
	cstoreFdwOptions->compressionType = compressionType;
	cstoreFdwOptions->stripeRowCount = stripeRowCount;
	cstoreFdwOptions->blockRowCount = blockRowCount;
	cstoreFdwOptions->logging = logging;

	return cstoreFdwOptions;
}


/*
 * CStoreGetOptionValue walks over foreign table and foreign server options, and
 * looks for the option with the given name. If found, the function returns the
 * option's value. This function is unchanged from mongo_fdw.
 */
static char *
CStoreGetOptionValue(Oid foreignTableId, const char *optionName)
{
	ForeignTable *foreignTable = NULL;
	ForeignServer *foreignServer = NULL;
	List *optionList = NIL;
	ListCell *optionCell = NULL;
	char *optionValue = NULL;

	foreignTable = GetForeignTable(foreignTableId);
	foreignServer = GetForeignServer(foreignTable->serverid);

	optionList = list_concat(optionList, foreignTable->options);
	optionList = list_concat(optionList, foreignServer->options);

	foreach(optionCell, optionList)
	{
		DefElem *optionDef = (DefElem *) lfirst(optionCell);
		char *optionDefName = optionDef->defname;

		if (strncmp(optionDefName, optionName, NAMEDATALEN) == 0)
		{
			optionValue = defGetString(optionDef);
			break;
		}
	}

	return optionValue;
}


/*
 * ValidateForeignTableOptions verifies if given options are valid cstore_fdw
 * foreign table options. This function errors out if given option value is
 * considered invalid.
 */
static void
ValidateForeignTableOptions(char *compressionTypeString,
							char *stripeRowCountString, char *blockRowCountString,
							char *loggingString)
{
	/* check if the provided compression type is valid */
	if (compressionTypeString != NULL)
	{
		CompressionType compressionType = ParseCompressionType(compressionTypeString);
		if (compressionType == COMPRESSION_TYPE_INVALID)
		{
			ereport(ERROR, (errmsg("invalid compression type"),
							errhint("Valid options are: %s",
									COMPRESSION_STRING_DELIMITED_LIST)));
		}
	}

	/* check if the provided stripe row count has correct format and range */
	if (stripeRowCountString != NULL)
	{
		/* pg_atoi() errors out if the given string is not a valid 32-bit integer */
		int32 stripeRowCount = pg_atoi(stripeRowCountString, sizeof(int32), 0);
		if (stripeRowCount < STRIPE_ROW_COUNT_MINIMUM ||
			stripeRowCount > STRIPE_ROW_COUNT_MAXIMUM)
		{
			ereport(ERROR, (errmsg("invalid stripe row count"),
							errhint("Stripe row count must be an integer between "
									"%d and %d", STRIPE_ROW_COUNT_MINIMUM,
									STRIPE_ROW_COUNT_MAXIMUM)));
		}
	}

	/* check if the provided block row count has correct format and range */
	if (blockRowCountString != NULL)
	{
		/* pg_atoi() errors out if the given string is not a valid 32-bit integer */
		int32 blockRowCount = pg_atoi(blockRowCountString, sizeof(int32), 0);
		if (blockRowCount < BLOCK_ROW_COUNT_MINIMUM ||
			blockRowCount > BLOCK_ROW_COUNT_MAXIMUM)
		{
			ereport(ERROR, (errmsg("invalid block row count"),
							errhint("Block row count must be an integer between "
									"%d and %d", BLOCK_ROW_COUNT_MINIMUM,
									BLOCK_ROW_COUNT_MAXIMUM)));
		}
	}

	/* check if the provided logging flag is valid */
	if (loggingString != NULL)
	{
		if (strncmp(LOGGING_STRING_TRUE, loggingString, NAMEDATALEN) != 0 &&
			strncmp(LOGGING_STRING_FALSE, loggingString, NAMEDATALEN) != 0)
		{
			ereport(ERROR, (errmsg("invalid logging flag"),
							errhint("Valid options are: %s",
									LOGGING_STRING_DELIMITED_LIST)));
		}
	}
}


/* ParseCompressionType converts a string to a compression type. */
static CompressionType
ParseCompressionType(const char *compressionTypeString)
{
	CompressionType compressionType = COMPRESSION_TYPE_INVALID;
	Assert(compressionTypeString != NULL);

	if (strncmp(compressionTypeString, COMPRESSION_STRING_NONE, NAMEDATALEN) == 0)
	{
		compressionType = COMPRESSION_NONE;
	}
	else if (strncmp(compressionTypeString, COMPRESSION_STRING_PG_LZ, NAMEDATALEN) == 0)
	{
		compressionType = COMPRESSION_PG_LZ;
	}

	return compressionType;
}


/*
 * CStoreGetForeignRelSize obtains relation size estimates for a foreign table and
 * puts its estimate for row count into baserel->rows.
 */
static void
CStoreGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreignTableId)
{
	Relation relation = heap_open(foreignTableId, AccessShareLock);
	double tupleCountEstimate = TupleCountEstimate(baserel, relation);
	double rowSelectivity = clauselist_selectivity(root, baserel->baserestrictinfo,
												   0, JOIN_INNER, NULL);

	double outputRowCount = clamp_row_est(tupleCountEstimate * rowSelectivity);
	baserel->rows = outputRowCount;
	heap_close(relation, AccessShareLock);
}


/*
 * CStoreGetForeignPaths creates possible access paths for a scan on the foreign
 * table. We currently have one possible access path. This path filters out row
 * blocks that are refuted by where clauses, and only returns values for the
 * projected columns.
 */
static void
CStoreGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreignTableId)
{
	Path *foreignScanPath = NULL;
	Relation relation = heap_open(foreignTableId, AccessShareLock);

	/*
	 * We skip reading columns that are not in query. Here we assume that all
	 * columns in relation have the same width, and estimate the number pages
	 * that will be read by query.
	 *
	 * Ideally, we should also take into account the row blocks that will be
	 * suppressed. But for that we need to know which columns are used for
	 * sorting. If we wrongly assume that we are sorted by a specific column
	 * and underestimate the page count, planner may choose nested loop join
	 * in a place it shouldn't be used. Choosing merge join or hash join is
	 * usually safer than nested loop join, so we take the more conservative
	 * approach and assume all rows in the columnar store file will be read.
	 * We intend to fix this in later version by improving the row sampling
	 * algorithm and using the correlation statistics to detect which columns
	 * are in stored in sorted order.
	 */
	List *queryColumnList = ColumnList(baserel, foreignTableId);
	uint32 queryColumnCount = list_length(queryColumnList);
	BlockNumber relationPageCount = PageCount(relation);
	uint32 relationColumnCount = RelationGetNumberOfAttributes(relation);

	double queryColumnRatio = (double) queryColumnCount / relationColumnCount;
	double queryPageCount = relationPageCount * queryColumnRatio;
	double totalDiskAccessCost = seq_page_cost * queryPageCount;

	double tupleCountEstimate = TupleCountEstimate(baserel, relation);

	/*
	 * We estimate costs almost the same way as cost_seqscan(), thus assuming
	 * that I/O costs are equivalent to a regular table file of the same size.
	 */
	double filterCostPerTuple = baserel->baserestrictcost.per_tuple;
	double cpuCostPerTuple = cpu_tuple_cost + filterCostPerTuple;
	double totalCpuCost = cpuCostPerTuple * tupleCountEstimate;

	double startupCost = baserel->baserestrictcost.startup;
	double totalCost  = startupCost + totalCpuCost + totalDiskAccessCost;

	/* create a foreign path node and add it as the only possible path */
#if PG_VERSION_NUM >= 90600
	foreignScanPath = (Path *) create_foreignscan_path(root, baserel,
													   NULL, /* path target */
													   baserel->rows,
													   startupCost, totalCost,
													   NIL,  /* no known ordering */
													   NULL, /* not parameterized */
													   NULL, /* no outer path */
													   NIL); /* no fdw_private */

#elif PG_VERSION_NUM >= 90500
	foreignScanPath = (Path *) create_foreignscan_path(root, baserel, baserel->rows,
													   startupCost, totalCost,
													   NIL,  /* no known ordering */
													   NULL, /* not parameterized */
													   NULL, /* no outer path */
													   NIL); /* no fdw_private */
#else
	foreignScanPath = (Path *) create_foreignscan_path(root, baserel, baserel->rows,
													   startupCost, totalCost,
													   NIL,  /* no known ordering */
													   NULL, /* not parameterized */
													   NIL); /* no fdw_private */
#endif

	add_path(baserel, foreignScanPath);
	heap_close(relation, AccessShareLock);
}


/*
 * CStoreGetForeignPlan creates a ForeignScan plan node for scanning the foreign
 * table. We also add the query column list to scan nodes private list, because
 * we need it later for skipping over unused columns in the query.
 */
#if PG_VERSION_NUM >= 90500
static ForeignScan *
CStoreGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreignTableId,
					 ForeignPath *bestPath, List *targetList, List *scanClauses,
					 Plan *outerPlan)
#else
static ForeignScan *
CStoreGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreignTableId,
					 ForeignPath *bestPath, List *targetList, List *scanClauses)
#endif
{
	ForeignScan *foreignScan = NULL;
	List *columnList = NIL;
	List *foreignPrivateList = NIL;

	/*
	 * Although we skip row blocks that are refuted by the WHERE clause, but
	 * we have no native ability to evaluate restriction clauses and make sure
	 * that all non-related rows are filtered out. So we just put all of the
	 * scanClauses into the plan node's qual list for the executor to check.
	 */
	scanClauses = extract_actual_clauses(scanClauses,
										 false); /* extract regular clauses */

	/*
	 * As an optimization, we only read columns that are present in the query.
	 * To find these columns, we need baserel. We don't have access to baserel
	 * in executor's callback functions, so we get the column list here and put
	 * it into foreign scan node's private list.
	 */
	columnList = ColumnList(baserel, foreignTableId);
	foreignPrivateList = list_make1(columnList);

	/* create the foreign scan node */
#if PG_VERSION_NUM >= 90500
	foreignScan = make_foreignscan(targetList, scanClauses, baserel->relid,
								   NIL, /* no expressions to evaluate */
								   foreignPrivateList,
								   NIL,
								   NIL,
								   NULL); /* no outer path */
#else
	foreignScan = make_foreignscan(targetList, scanClauses, baserel->relid,
								   NIL, /* no expressions to evaluate */
								   foreignPrivateList);
#endif

	return foreignScan;
}


/*
 * TupleCountEstimate estimates the number of base relation tuples in the given
 * file.
 */
static double
TupleCountEstimate(RelOptInfo *baserel, Relation relation)
{
	double tupleCountEstimate = 0.0;

	/* check if the user executed Analyze on this foreign table before */
	if (baserel->pages > 0)
	{
		/*
		 * We have number of pages and number of tuples from pg_class (from a
		 * previous ANALYZE), so compute a tuples-per-page estimate and scale
		 * that by the current file size.
		 */
		double tupleDensity = baserel->tuples / (double) baserel->pages;
		BlockNumber pageCount = PageCount(relation);

		tupleCountEstimate = clamp_row_est(tupleDensity * (double) pageCount);
	}
	else
	{
		tupleCountEstimate = (double) CStoreTableRowCount(relation);
	}

	return tupleCountEstimate;
}


/* PageCount calculates and returns the number of pages in a file. */
static BlockNumber
PageCount(Relation relation)
{
	BlockNumber pageCount = 0;
	BlockNumber footerBlockCount = 0;
	BlockNumber dataBlockCount = 0;

	footerBlockCount = RelationGetNumberOfBlocksInFork(relation, FOOTER_FORKNUM);
	dataBlockCount = RelationGetNumberOfBlocksInFork(relation, DATA_FORKNUM);

	pageCount = footerBlockCount + dataBlockCount;

	/* if the relation is empty, use default estimate for its size */
	if (pageCount == 0)
	{
		pageCount = 10;
	}

	return pageCount;
}


/*
 * ColumnList takes in the planner's information about this foreign table. The
 * function then finds all columns needed for query execution, including those
 * used in projections, joins, and filter clauses, de-duplicates these columns,
 * and returns them in a new list. This function is taken from mongo_fdw with
 * slight modifications.
 */
static List *
ColumnList(RelOptInfo *baserel, Oid foreignTableId)
{
	List *columnList = NIL;
	List *neededColumnList = NIL;
	AttrNumber columnIndex = 1;
	AttrNumber columnCount = baserel->max_attr;
#if PG_VERSION_NUM >= 90600
	List *targetColumnList = baserel->reltarget->exprs;
#else
	List *targetColumnList = baserel->reltargetlist;
#endif
	ListCell *targetColumnCell = NULL;
	List *restrictInfoList = baserel->baserestrictinfo;
	ListCell *restrictInfoCell = NULL;
	const AttrNumber wholeRow = 0;
	Relation relation = heap_open(foreignTableId, AccessShareLock);
	TupleDesc tupleDescriptor = RelationGetDescr(relation);
	FormData_pg_attribute *attributeFormArray = tupleDescriptor->attrs;

	/* first add the columns used in joins and projections */
	foreach(targetColumnCell, targetColumnList)
	{
		List *targetVarList = NIL;
		Node *targetExpr = (Node *) lfirst(targetColumnCell);

#if PG_VERSION_NUM >= 90600
		targetVarList = pull_var_clause(targetExpr,
										PVC_RECURSE_AGGREGATES |
										PVC_RECURSE_PLACEHOLDERS);
#else
		targetVarList = pull_var_clause(targetExpr,
										PVC_RECURSE_AGGREGATES,
										PVC_RECURSE_PLACEHOLDERS);
#endif

		neededColumnList = list_union(neededColumnList, targetVarList);
	}

	/* then walk over all restriction clauses, and pull up any used columns */
	foreach(restrictInfoCell, restrictInfoList)
	{
		RestrictInfo *restrictInfo = (RestrictInfo *) lfirst(restrictInfoCell);
		Node *restrictClause = (Node *) restrictInfo->clause;
		List *clauseColumnList = NIL;

		/* recursively pull up any columns used in the restriction clause */
#if PG_VERSION_NUM >= 90600
		clauseColumnList = pull_var_clause(restrictClause,
										   PVC_RECURSE_AGGREGATES |
										   PVC_RECURSE_PLACEHOLDERS);
#else
		clauseColumnList = pull_var_clause(restrictClause,
										   PVC_RECURSE_AGGREGATES,
										   PVC_RECURSE_PLACEHOLDERS);
#endif

		neededColumnList = list_union(neededColumnList, clauseColumnList);
	}

	/* walk over all column definitions, and de-duplicate column list */
	for (columnIndex = 1; columnIndex <= columnCount; columnIndex++)
	{
		ListCell *neededColumnCell = NULL;
		Var *column = NULL;

		/* look for this column in the needed column list */
		foreach(neededColumnCell, neededColumnList)
		{
			Var *neededColumn = (Var *) lfirst(neededColumnCell);
			if (neededColumn->varattno == columnIndex)
			{
				column = neededColumn;
				break;
			}
			else if (neededColumn->varattno == wholeRow)
			{
				Form_pg_attribute attributeForm = &attributeFormArray[columnIndex - 1];
				Index tableId = neededColumn->varno;

				column = makeVar(tableId, columnIndex, attributeForm->atttypid,
								 attributeForm->atttypmod, attributeForm->attcollation,
								 0);
				break;
			}
		}

		if (column != NULL)
		{
			columnList = lappend(columnList, column);
		}
	}

	heap_close(relation, AccessShareLock);

	return columnList;
}


/* CStoreExplainForeignScan produces extra output for the Explain command. */
static void
CStoreExplainForeignScan(ForeignScanState *scanState, ExplainState *explainState)
{
	/* supress file size if we're not showing cost details */
	if (explainState->costs)
	{
		Oid foreignTableId = RelationGetRelid(scanState->ss.ss_currentRelation);
		Relation relation = heap_open(foreignTableId, AccessShareLock);
		BlockNumber pageCount = PageCount(relation);
		long fileSize = pageCount * BLCKSZ;

		heap_close(relation, NoLock);

		ExplainPropertyLong("CStore File Size", fileSize, explainState);
	}
}


/* CStoreBeginForeignScan starts reading the underlying cstore file. */
static void
CStoreBeginForeignScan(ForeignScanState *scanState, int executorFlags)
{
	TableReadState *readState = NULL;
	TupleTableSlot *tupleSlot = scanState->ss.ss_ScanTupleSlot;
	TupleDesc tupleDescriptor = tupleSlot->tts_tupleDescriptor;
	List *columnList = NIL;
	ForeignScan *foreignScan = NULL;
	List *foreignPrivateList = NIL;
	List *whereClauseList = NIL;
	Relation currentRelation = scanState->ss.ss_currentRelation;

	/* if Explain with no Analyze, do nothing */
	if (executorFlags & EXEC_FLAG_EXPLAIN_ONLY)
	{
		return;
	}

	foreignScan = (ForeignScan *) scanState->ss.ps.plan;
	foreignPrivateList = (List *) foreignScan->fdw_private;
	whereClauseList = foreignScan->scan.plan.qual;

	columnList = (List *) linitial(foreignPrivateList);
	readState = CStoreBeginRead(currentRelation, tupleDescriptor, columnList,
								whereClauseList);

	scanState->fdw_state = (void *) readState;
}


/*
 * CStoreIterateForeignScan reads the next record from the cstore file, converts
 * it to a Postgres tuple, and stores the converted tuple into the ScanTupleSlot
 * as a virtual tuple.
 */
static TupleTableSlot *
CStoreIterateForeignScan(ForeignScanState *scanState)
{
	TableReadState *readState = (TableReadState *) scanState->fdw_state;
	TupleTableSlot *tupleSlot = scanState->ss.ss_ScanTupleSlot;
	bool nextRowFound = false;

	TupleDesc tupleDescriptor = tupleSlot->tts_tupleDescriptor;
	Datum *columnValues = tupleSlot->tts_values;
	bool *columnNulls = tupleSlot->tts_isnull;
	uint32 columnCount = tupleDescriptor->natts;

	/* initialize all values for this row to null */
	memset(columnValues, 0, columnCount * sizeof(Datum));
	memset(columnNulls, true, columnCount * sizeof(bool));

	ExecClearTuple(tupleSlot);

	nextRowFound = CStoreReadNextRow(readState, columnValues, columnNulls);
	if (nextRowFound)
	{
		ExecStoreVirtualTuple(tupleSlot);
	}

	return tupleSlot;
}


/* CStoreEndForeignScan finishes scanning the foreign table. */
static void
CStoreEndForeignScan(ForeignScanState *scanState)
{
	TableReadState *readState = (TableReadState *) scanState->fdw_state;
	if (readState != NULL)
	{
		CStoreEndRead(readState);
	}
}


/* CStoreReScanForeignScan rescans the foreign table. */
static void
CStoreReScanForeignScan(ForeignScanState *scanState)
{
	CStoreEndForeignScan(scanState);
	CStoreBeginForeignScan(scanState, 0);
}


/*
 * CStoreAnalyzeForeignTable sets the total page count and the function pointer
 * used to acquire a random sample of rows from the foreign file.
 */
static bool
CStoreAnalyzeForeignTable(Relation relation,
						  AcquireSampleRowsFunc *acquireSampleRowsFunc,
						  BlockNumber *totalPageCount)
{
	(*totalPageCount) = PageCount(relation);
	(*acquireSampleRowsFunc) = CStoreAcquireSampleRows;

	return true;
}


/*
 * CStoreAcquireSampleRows acquires a random sample of rows from the foreign
 * table. Selected rows are returned in the caller allocated sampleRows array,
 * which must have at least target row count entries. The actual number of rows
 * selected is returned as the function result. We also count the number of rows
 * in the collection and return it in total row count. We also always set dead
 * row count to zero.
 *
 * Note that the returned list of rows does not always follow their actual order
 * in the cstore file. Therefore, correlation estimates derived later could be
 * inaccurate, but that's OK. We currently don't use correlation estimates (the
 * planner only pays attention to correlation for index scans).
 */
static int
CStoreAcquireSampleRows(Relation relation, int logLevel,
						HeapTuple *sampleRows, int targetRowCount,
						double *totalRowCount, double *totalDeadRowCount)
{
	int sampleRowCount = 0;
	double rowCount = 0.0;
	double rowCountToSkip = -1;	/* -1 means not set yet */
	double selectionState = 0;
	MemoryContext oldContext = CurrentMemoryContext;
	MemoryContext tupleContext = NULL;
	Datum *columnValues = NULL;
	bool *columnNulls = NULL;
	TupleTableSlot *scanTupleSlot = NULL;
	List *columnList = NIL;
	List *foreignPrivateList = NULL;
	ForeignScanState *scanState = NULL;
	ForeignScan *foreignScan = NULL;
	char *relationName = NULL;
	int executorFlags = 0;

	TupleDesc tupleDescriptor = RelationGetDescr(relation);
	uint32 columnCount = tupleDescriptor->natts;
	FormData_pg_attribute *attributeFormArray = tupleDescriptor->attrs;

	/* create list of columns of the relation */
	uint32 columnIndex = 0;
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		Form_pg_attribute attributeForm = &attributeFormArray[columnIndex];
		const Index tableId = 1;

		if (!attributeForm->attisdropped)
		{
			Var *column = makeVar(tableId, columnIndex + 1, attributeForm->atttypid,
								  attributeForm->atttypmod, attributeForm->attcollation, 0);
			columnList = lappend(columnList, column);
		}
	}

	/* setup foreign scan plan node */
	foreignPrivateList = list_make1(columnList);
	foreignScan = makeNode(ForeignScan);
	foreignScan->fdw_private = foreignPrivateList;

	/* set up tuple slot */
	columnValues = palloc0(columnCount * sizeof(Datum));
	columnNulls = palloc0(columnCount * sizeof(bool));
	scanTupleSlot = MakeTupleTableSlot();
	scanTupleSlot->tts_tupleDescriptor = tupleDescriptor;
	scanTupleSlot->tts_values = columnValues;
	scanTupleSlot->tts_isnull = columnNulls;

	/* setup scan state */
	scanState = makeNode(ForeignScanState);
	scanState->ss.ss_currentRelation = relation;
	scanState->ss.ps.plan = (Plan *) foreignScan;
	scanState->ss.ss_ScanTupleSlot = scanTupleSlot;

	/*
	 * Use per-tuple memory context to prevent leak of memory used to read and
	 * parse rows from the file.
	 */
	tupleContext = AllocSetContextCreate(CurrentMemoryContext,
										 "cstore_fdw temporary context",
										 ALLOCSET_DEFAULT_MINSIZE,
										 ALLOCSET_DEFAULT_INITSIZE,
										 ALLOCSET_DEFAULT_MAXSIZE);

	CStoreBeginForeignScan(scanState, executorFlags);

	/* prepare for sampling rows */
	selectionState = anl_init_selection_state(targetRowCount);

	for (;;)
	{
		/* check for user-requested abort or sleep */
		vacuum_delay_point();

		memset(columnValues, 0, columnCount * sizeof(Datum));
		memset(columnNulls, true, columnCount * sizeof(bool));

		MemoryContextReset(tupleContext);
		MemoryContextSwitchTo(tupleContext);

		/* read the next record */
		CStoreIterateForeignScan(scanState);

		MemoryContextSwitchTo(oldContext);

		/* if there are no more records to read, break */
		if (scanTupleSlot->tts_isempty)
		{
			break;
		}

		/*
		 * The first targetRowCount sample rows are simply copied into the
		 * reservoir. Then we start replacing tuples in the sample until we
		 * reach the end of the relation. This algorithm is from Jeff Vitter's
		 * paper (see more info in commands/analyze.c).
		 */
		if (sampleRowCount < targetRowCount)
		{
			sampleRows[sampleRowCount] = heap_form_tuple(tupleDescriptor, columnValues,
														 columnNulls);
			sampleRowCount++;
		}
		else
		{
			/*
			 * t in Vitter's paper is the number of records already processed.
			 * If we need to compute a new S value, we must use the "not yet
			 * incremented" value of rowCount as t.
			 */
			if (rowCountToSkip < 0)
			{
				rowCountToSkip = anl_get_next_S(rowCount, targetRowCount,
												&selectionState);
			}

			if (rowCountToSkip <= 0)
			{
				/*
				 * Found a suitable tuple, so save it, replacing one old tuple
				 * at random.
				 */
				int rowIndex = (int) (targetRowCount * anl_random_fract());
				Assert(rowIndex >= 0);
				Assert(rowIndex < targetRowCount);

				heap_freetuple(sampleRows[rowIndex]);
				sampleRows[rowIndex] = heap_form_tuple(tupleDescriptor,
													   columnValues, columnNulls);
			}

			rowCountToSkip--;
		}

		rowCount++;
	}

	/* clean up */
	MemoryContextDelete(tupleContext);
	pfree(columnValues);
	pfree(columnNulls);

	CStoreEndForeignScan(scanState);

	/* emit some interesting relation info */
	relationName = RelationGetRelationName(relation);
	ereport(logLevel, (errmsg("\"%s\": file contains %.0f rows; %d rows in sample",
							  relationName, rowCount, sampleRowCount)));

	(*totalRowCount) = rowCount;
	(*totalDeadRowCount) = 0;

	return sampleRowCount;
}


/*
 * CStorePlanForeignModify checks if operation is supported. Only insert
 * command with subquery (ie insert into <table> select ...) is supported.
 * Other forms of insert, delete, and update commands are not supported. It
 * throws an error when the command is not supported.
 */
static List *
CStorePlanForeignModify(PlannerInfo *plannerInfo, ModifyTable *plan,
						Index resultRelation, int subplanIndex)
{
	bool operationSupported = false;

	if (plan->operation == CMD_INSERT)
	{
		ListCell *tableCell = NULL;
		Query *query = NULL;

		/*
		 * Only insert operation with select subquery is supported. Other forms
		 * of insert, update, and delete operations are not supported.
		 */
		query = plannerInfo->parse;
		foreach(tableCell, query->rtable)
		{
			RangeTblEntry *tableEntry = lfirst(tableCell);

			if (tableEntry->rtekind == RTE_SUBQUERY &&
				tableEntry->subquery != NULL &&
				tableEntry->subquery->commandType == CMD_SELECT)
			{
				operationSupported = true;
				break;
			}
		}
	}

	if (!operationSupported)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("operation is not supported")));
	}

	return NIL;
}


/* CStoreBeginForeignModify prepares cstore table for insert operation. */
static void
CStoreBeginForeignModify(ModifyTableState *modifyTableState,
						 ResultRelInfo *relationInfo, List *fdwPrivate,
						 int subplanIndex, int executorFlags)
{
	Oid  foreignTableOid = InvalidOid;
	CStoreFdwOptions *cstoreFdwOptions = NULL;
	TupleDesc tupleDescriptor = NULL;
	TableWriteState *writeState = NULL;
	Relation relation = NULL;

	/* if Explain with no Analyze, do nothing */
	if (executorFlags & EXEC_FLAG_EXPLAIN_ONLY)
	{
		return;
	}

	Assert (modifyTableState->operation == CMD_INSERT);

	foreignTableOid = RelationGetRelid(relationInfo->ri_RelationDesc);
	relation = heap_open(foreignTableOid, ShareUpdateExclusiveLock);
	cstoreFdwOptions = CStoreGetOptions(foreignTableOid);
	tupleDescriptor = RelationGetDescr(relationInfo->ri_RelationDesc);

	writeState = CStoreBeginWrite(relation,
								  cstoreFdwOptions->compressionType,
								  cstoreFdwOptions->stripeRowCount,
								  cstoreFdwOptions->blockRowCount,
								  cstoreFdwOptions->logging,
								  tupleDescriptor);

	relationInfo->ri_FdwState = (void *) writeState;
}


/*
 * CStoreExecForeignInsert inserts a single row to cstore table
 * and returns inserted row's data values.
 */
static TupleTableSlot *
CStoreExecForeignInsert(EState *executorState, ResultRelInfo *relationInfo,
						TupleTableSlot *tupleSlot, TupleTableSlot *planSlot)
{
	TableWriteState *writeState = (TableWriteState*) relationInfo->ri_FdwState;

	Assert(writeState != NULL);

	if(HeapTupleHasExternal(tupleSlot->tts_tuple))
	{
		/* detoast any toasted attributes */
		tupleSlot->tts_tuple = toast_flatten_tuple(tupleSlot->tts_tuple,
												   tupleSlot->tts_tupleDescriptor);
	}

	slot_getallattrs(tupleSlot);

	CStoreWriteRow(writeState, tupleSlot->tts_values, tupleSlot->tts_isnull);

	return tupleSlot;
}


/* CStoreEndForeignModify ends the current insert operation. */
static void
CStoreEndForeignModify(EState *executorState, ResultRelInfo *relationInfo)
{
	TableWriteState *writeState = (TableWriteState*) relationInfo->ri_FdwState;

	/* writeState is NULL during Explain queries */
	if (writeState != NULL)
	{
		Relation relation = writeState->relation;

		CStoreEndWrite(writeState);
		heap_close(relation, NoLock);
	}
}

