#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "libpq/pqformat.h"
#include "parser/parse_coerce.h"
#include "catalog/pg_type.h"
#include "utils/array.h"
#include "utils/numeric.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "executor/executor.h"
#include <math.h>

#include "access/hash.h"

#ifndef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#undef StartCopyFromTuple
#undef CopyDatumToHeapTuple
#undef CopyFromTuple

#define StartCopyToHeapTuple(datumArr, nullArr) { \
Datum *__datumArr = datumArr; int __tupleItem = 0; bool *__nullArr = nullArr
#define CopyToHeapTuple(src, GetDatum) \
do { \
if (__nullArr[__tupleItem] == false) \
__datumArr[__tupleItem++] = GetDatum(src); \
else \
__datumArr[__tupleItem++] = PointerGetDatum(NULL); \
} while (0)
#define CopyDatumToHeapTuple(src) \
do { \
if (__nullArr[__tupleItem] == false) \
__datumArr[__tupleItem++] = src; \
else \
__datumArr[__tupleItem++] = PointerGetDatum(NULL); \
} while (0)
#define EndCopyToDatum }

/*************************
AT TIME QUERY
*************************/

Datum at_time_sfunc(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(at_time_sfunc);
Datum at_time_sfunc(PG_FUNCTION_ARGS)
{
	HeapTupleHeader input;
	bool isNull, isnull[6];
	
	TypeFuncClass funcClass;
	Oid resultType;
	TupleDesc resultDesc;
	Datum resultDatum[3];
	float8 current_price = 0;
	bool type_of_quote = false;
	int64 id = 0;
	
	if(! PG_ARGISNULL(0)){
		input = PG_GETARG_HEAPTUPLEHEADER(0);
		current_price = DatumGetFloat8(GetAttributeByName(input, "value", &isNull));
		id = DatumGetInt64(GetAttributeByName(input, "id", &isNull));
		type_of_quote = DatumGetBool(GetAttributeByName(input, "type", &isNull));
	}
	
	if(!PG_ARGISNULL(3) && !PG_ARGISNULL(1)){
			type_of_quote = PG_GETARG_BOOL(3);
			if(type_of_quote){
				if(!PG_ARGISNULL(2)){
					id = PG_GETARG_INT64(2);
				}else{
					id = 0;
				}
				current_price = PG_GETARG_FLOAT8(1);
			}
	}
		
	memset(isnull, 0, sizeof(isnull));
		
	funcClass = get_call_result_type(fcinfo, &resultType, &resultDesc);
	BlessTupleDesc(resultDesc);
	StartCopyToHeapTuple(resultDatum, isnull);
	CopyToHeapTuple(current_price, Float8GetDatum);
	CopyToHeapTuple(id, Int64GetDatum);
	CopyToHeapTuple(type_of_quote, BoolGetDatum);
	EndCopyToDatum;
	
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(resultDesc, resultDatum, isnull)));
}

Datum at_time_seprareate_rows_sfunc(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(at_time_seprareate_rows_sfunc);
Datum at_time_seprareate_rows_sfunc(PG_FUNCTION_ARGS)
{
	HeapTupleHeader input;
	bool isNull, isnull[6];
	
	TypeFuncClass funcClass;
	Oid resultType;
	TupleDesc resultDesc;
	Datum resultDatum[3];
	float8 current_price = 0;
	bool type_of_quote = false;
	int64 id = 0;
	
	if(! PG_ARGISNULL(0)){
		input = PG_GETARG_HEAPTUPLEHEADER(0);
		current_price = DatumGetFloat8(GetAttributeByName(input, "value", &isNull));
		id = DatumGetInt64(GetAttributeByName(input, "id", &isNull));
		type_of_quote = DatumGetBool(GetAttributeByName(input, "type", &isNull));
	}
	
	if(!PG_ARGISNULL(2) && !PG_ARGISNULL(1)){
			type_of_quote = (bool)1;
			if(type_of_quote){
				if(!PG_ARGISNULL(2)){
					id = PG_GETARG_INT64(2);
				}else{
					id = 0;
				}
				current_price = PG_GETARG_FLOAT8(1);
			}
	}
		
	memset(isnull, 0, sizeof(isnull));
		
	funcClass = get_call_result_type(fcinfo, &resultType, &resultDesc);
	BlessTupleDesc(resultDesc);
	StartCopyToHeapTuple(resultDatum, isnull);
	CopyToHeapTuple(current_price, Float8GetDatum);
	CopyToHeapTuple(id, Int64GetDatum);
	CopyToHeapTuple(type_of_quote, BoolGetDatum);
	EndCopyToDatum;
	
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(resultDesc, resultDatum, isnull)));
}

Datum at_time_prefunc(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(at_time_prefunc);
Datum at_time_prefunc(PG_FUNCTION_ARGS)
{
	HeapTupleHeader input, input2;
	bool isNull, isnull[6];
	
	TypeFuncClass funcClass;
	Oid resultType;
	TupleDesc resultDesc;
	Datum resultDatum[3];
	float8 current_price = 0;
	bool type_of_quote = false, type_of_quote2 = false;
	int64 id = 0;
	
	if(PG_ARGISNULL(1)){
		return PG_GETARG_DATUM(0);
	}else if(PG_ARGISNULL(0)){
		return PG_GETARG_DATUM(1);
	}
	
	input2 = PG_GETARG_HEAPTUPLEHEADER(1);
	type_of_quote2 = DatumGetBool(GetAttributeByName(input2, "type", &isNull));
	
	input = PG_GETARG_HEAPTUPLEHEADER(0);
	current_price = DatumGetFloat8(GetAttributeByName(input, "value", &isNull));
	id = DatumGetInt64(GetAttributeByName(input, "id", &isNull));
	type_of_quote = DatumGetBool(GetAttributeByName(input, "type", &isNull));
	
	if(type_of_quote2){
		return PG_GETARG_DATUM(1);
	}
		
	memset(isnull, 0, sizeof(isnull));
		
	funcClass = get_call_result_type(fcinfo, &resultType, &resultDesc);
	BlessTupleDesc(resultDesc);
	StartCopyToHeapTuple(resultDatum, isnull);
	CopyToHeapTuple(current_price, Float8GetDatum);
	CopyToHeapTuple(id, Int64GetDatum);
	CopyToHeapTuple(type_of_quote2, BoolGetDatum);
	EndCopyToDatum;
	
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(resultDesc, resultDatum, isnull)));
}

Datum at_time_finalfunc(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(at_time_finalfunc);
Datum at_time_finalfunc(PG_FUNCTION_ARGS)
{
	HeapTupleHeader input;
	bool isNull, isnull[3];
	
	TypeFuncClass funcClass;
	Oid resultType;
	TupleDesc resultDesc;
	Datum resultDatum[3];
	float8 current_price = 0;
	bool type_of_quote = false;
	int64 id = 0;
	
	if(PG_ARGISNULL(0)){
		PG_RETURN_NULL();
	}
	
	input = PG_GETARG_HEAPTUPLEHEADER(0);
	type_of_quote = DatumGetBool(GetAttributeByName(input, "type", &isNull));
	
	if(type_of_quote){
		PG_RETURN_NULL();
	}
	
	current_price = DatumGetFloat8(GetAttributeByName(input, "value", &isNull));
	id = DatumGetInt64(GetAttributeByName(input, "id", &isNull));
	
	memset(isnull, 0, sizeof(isnull));
		
	funcClass = get_call_result_type(fcinfo, &resultType, &resultDesc);
	BlessTupleDesc(resultDesc);
	StartCopyToHeapTuple(resultDatum, isnull);
	CopyToHeapTuple(current_price, Float8GetDatum);
	CopyToHeapTuple(id, Int64GetDatum);
	EndCopyToDatum;
	
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(resultDesc, resultDatum, isnull)));
}

