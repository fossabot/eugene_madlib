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

/**************************
EVENT DETECTION 

SELECT 'sum(int)'::regprocedure::oid;
**************************/

Datum event_detection_passOID_finalfunc(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(event_detection_passOID_finalfunc);
Datum event_detection_passOID_finalfunc(PG_FUNCTION_ARGS)
{
	ArrayType *tmp = NULL;
	Datum test_input;
	float8 *event_array = NULL;
	Oid event_detection_function = (Oid)0;
	int *dims,ndims,nitems;
	bool input_stat = TRUE;
	
	HeapTupleHeader input;
	bool isNull;
	
	if(! PG_ARGISNULL(0)){
		input = PG_GETARG_HEAPTUPLEHEADER(0);
		event_detection_function = (Oid)DatumGetInt32(GetAttributeByName(input, "oid", &isNull));
		if(isNull) { PG_RETURN_NULL();}
		tmp = DatumGetArrayTypeP(GetAttributeByName(input, "val", &isNull));
		if(isNull) { PG_RETURN_NULL();}
		test_input = GetAttributeByName(input, "input", &isNull);
		if(isNull) { input_stat = FALSE;}
	
		dims = ARR_DIMS(tmp);
		ndims = ARR_NDIM(tmp);
	
		if (ndims == 0){
			PG_RETURN_NULL();
		}
	
		event_array = (float8*)ARR_DATA_PTR(tmp);
		nitems = ArrayGetNItems(ndims, dims);
		if (nitems == 0){
			PG_RETURN_NULL();
		}
		if(input_stat){
			return OidFunctionCall2(event_detection_function, GetAttributeByName(input, "val", &isNull), GetAttributeByName(input, "input", &isNull));
		}
		return OidFunctionCall1(event_detection_function, GetAttributeByName(input, "val", &isNull));
	}
	PG_RETURN_NULL();
}

Datum event_detection_pearson_finalfunc(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(event_detection_pearson_finalfunc);
Datum event_detection_pearson_finalfunc(PG_FUNCTION_ARGS)
{
	ArrayType *tmp = NULL, *tmp2 = NULL;
	float8 *event_array = NULL;
	float8 *event_array2 = NULL;
	float8 *event_array_min = NULL;
	float8 *event_array_max = NULL;
	int *dims,ndims,nitems;
	int *dims2,ndims2,nitems2;
	int max_nitems = 0;
	float8 result = 0;
	int i = 0; 
	float8 comp = 0.0;
 	float8 ceil_i, floor_i;
	
	float8 avg_min = 0, avg_max = 0, stdev_min = 0, stdev_max = 0;
	
	HeapTupleHeader input;
	bool isNull;
	
	if(! PG_ARGISNULL(0)){
		input = PG_GETARG_HEAPTUPLEHEADER(0);
		tmp2 = DatumGetArrayTypeP(GetAttributeByName(input, "ref", &isNull));
		if(isNull||ARR_HASNULL(tmp2)) { PG_RETURN_NULL();}
		tmp = DatumGetArrayTypeP(GetAttributeByName(input, "val", &isNull));
		if(isNull||ARR_HASNULL(tmp)) { PG_RETURN_NULL();}
	
		dims = ARR_DIMS(tmp);
		ndims = ARR_NDIM(tmp);
		
		dims2 = ARR_DIMS(tmp2);
		ndims2 = ARR_NDIM(tmp2);
	
		if ((ndims == 0)||(ndims2 == 0)){
			PG_RETURN_NULL();
		}
	
		event_array = (float8*)ARR_DATA_PTR(tmp);
		nitems = ArrayGetNItems(ndims, dims);
		if (nitems == 0){
			PG_RETURN_NULL();
		}
		
		event_array2 = (float8*)ARR_DATA_PTR(tmp2);
		nitems2 = ArrayGetNItems(ndims2, dims2);
		if (nitems2 == 0){
			PG_RETURN_NULL();
		}
		
		//first we do a quantile normalization and find avg;
		if(nitems > nitems2){
				max_nitems = nitems;
				event_array_min = (float8*)palloc(sizeof(float8)*max_nitems);
				comp = (float8)(nitems2-1)/(float8)(max_nitems-1);
				event_array_max = event_array;
				for(i = 0; i < max_nitems; ++i){
					ceil_i = ceil(i*comp);
					ceil_i = (ceil_i < (max_nitems-1))?ceil_i:(max_nitems-1);
					floor_i = floor(i*comp);
					event_array_min[i] = (floor_i != ceil_i)?
					event_array2[(int)floor_i]*(ceil_i-i*comp) + event_array2[(int)ceil_i]*(i*comp-floor_i):
					event_array2[(int)ceil_i];
					
					avg_min += event_array_min[i];
					avg_max += event_array_max[i];
				}
		}else if(nitems < nitems2){
				max_nitems = nitems2;
				event_array_min = (float8*)palloc(sizeof(float8)*max_nitems);
				comp = (float8)(nitems-1)/(float8)(max_nitems-1);
				event_array_max = event_array2;
				for(i = 0; i < max_nitems; ++i){
					ceil_i = ceil(i*comp);
					ceil_i = (ceil_i < (max_nitems-1))?ceil_i:(max_nitems-1);
					floor_i = floor(i*comp);
					event_array_min[i] = (floor_i != ceil_i)?
					event_array[(int)floor_i]*(ceil_i-i*comp) + event_array[(int)ceil_i]*(i*comp-floor_i):
					event_array[(int)ceil_i];
					             
					avg_min += event_array_min[i];
					avg_max += event_array_max[i];			
				}
		}else{
				max_nitems = nitems2;
				event_array_max = event_array2;
				event_array_min = event_array;
				for(i = 0; i < max_nitems; ++i){	
					avg_min += event_array_min[i];
					avg_max += event_array_max[i];			
				}
		}
		avg_min /= max_nitems;
		avg_max /= max_nitems;
		
		//find standard deviation
		for(i = 0; i < max_nitems; ++i){
			stdev_min += (event_array_min[i]-avg_min)*(event_array_min[i]-avg_min);
			stdev_max += (event_array_max[i]-avg_max)*(event_array_max[i]-avg_max);
			result += (event_array_max[i]-avg_max)*(event_array_min[i]-avg_min);
		}
		if((stdev_min == 0)||(stdev_max == 0)){
			PG_RETURN_NULL();
		}

		result /= (sqrt(stdev_min)*sqrt(stdev_max));
		PG_RETURN_FLOAT8(result);
	}
	PG_RETURN_NULL();
}
