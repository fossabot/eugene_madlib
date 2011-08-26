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
UNIFORM SERIES
*************************/

static bool
array_get_isnull(const bits8 *nullbitmap, int offset)
{
	if (nullbitmap == NULL)
		return false;			/* assume not null */
	if (nullbitmap[offset / 8] & (1 << (offset % 8)))
		return false;			/* not null */
	return true;
}
/*
 * unstruct
 */
Datum unstruct_series(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(unstruct_series);
Datum unstruct_series(PG_FUNCTION_ARGS)
{
	typedef struct
	{
		ArrayType  *arr;
		int			nextelem;
		int			numelems;
		char	   *elemdataptr;	/* this moves with nextelem */
		bits8	   *arraynullsptr;		/* this does not */
		int16		elmlen;
		bool		elmbyval;
		char		elmalign;
		
		ArrayType  *arr2;
		int			nextelem2;
		int			numelems2;
		char	   *elemdataptr2;	/* this moves with nextelem */
		bits8	   *arraynullsptr2;		/* this does not */
		int16		elmlen2;
		bool		elmbyval2;
		char		elmalign2;
	} array_unstruct_series_fctx;

	FuncCallContext *funcctx;
	array_unstruct_series_fctx *fctx;
	MemoryContext oldcontext;
	

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL())
	{
		ArrayType  *arr, *arr1;
		HeapTupleHeader input;
		bool isNull;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/*
		 * switch to memory context appropriate for multiple function calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/*
		 * Get the array value and detoast if needed.  We can't do this
		 * earlier because if we have to detoast, we want the detoasted copy
		 * to be in multi_call_memory_ctx, so it will go away when we're done
		 * and not before.	(If no detoast happens, we assume the originally
		 * passed array will stick around till then.)
		 */
		
		input = PG_GETARG_HEAPTUPLEHEADER(0);
		arr = DatumGetArrayTypeP(GetAttributeByName(input, "time_stamp", &isNull));
		arr1 = DatumGetArrayTypeP(GetAttributeByName(input, "value", &isNull));

		/* allocate memory for user context */
		fctx = (array_unstruct_series_fctx *) palloc(sizeof(array_unstruct_series_fctx));

		/* initialize state */
		fctx->arr = arr;
		fctx->nextelem = 0;
		fctx->numelems = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));
		
		fctx->arr2 = arr1;
		fctx->nextelem2 = 0;
		fctx->numelems2 = ArrayGetNItems(ARR_NDIM(arr1), ARR_DIMS(arr1));

		fctx->elemdataptr = ARR_DATA_PTR(arr);
		fctx->arraynullsptr = ARR_NULLBITMAP(arr);
		
		fctx->elemdataptr2 = ARR_DATA_PTR(arr1);
		fctx->arraynullsptr2 = ARR_NULLBITMAP(arr1);

		get_typlenbyvalalign(ARR_ELEMTYPE(arr),
							 &fctx->elmlen,
							 &fctx->elmbyval,
							 &fctx->elmalign);
		
		get_typlenbyvalalign(ARR_ELEMTYPE(arr1),
							 &fctx->elmlen2,
							 &fctx->elmbyval2,
							 &fctx->elmalign2);
		
		if(fctx->numelems2 != fctx->numelems){
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("Number of elements must be the same in both arrays")));
		}

		funcctx->user_fctx = fctx;
		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();
	fctx = funcctx->user_fctx;

	if (fctx->nextelem < fctx->numelems)
	{
		int			offset = fctx->nextelem++;
		int			offset2 = fctx->nextelem2++;
		Datum		elem, elem2;
		bool		isnull[2];
		TypeFuncClass funcClass;
		TupleDesc 	resultDesc;
		Oid 		resultType;
		Datum 		resultDatum[2];

		/*
		 * Check for NULL array element
		 */
		if (array_get_isnull(fctx->arraynullsptr, offset))
		{
			fcinfo->isnull = true;
			elem = (Datum) 0;
		}
		else
		{
			/*
			 * OK, get the element
			 */
			char	   *ptr = fctx->elemdataptr;

			fcinfo->isnull = false;
			elem = fetch_att(ptr, fctx->elmbyval, fctx->elmlen);

			/*
			 * Advance elemdataptr over it
			 */
			ptr = att_addlength_pointer(ptr, fctx->elmlen, ptr);
			ptr = (char *) att_align_nominal(ptr, fctx->elmalign);
			fctx->elemdataptr = ptr;
		}
		
		if (array_get_isnull(fctx->arraynullsptr2, offset2))
		{
			fcinfo->isnull = true;
			elem2 = (Datum) 0;
		}
		else
		{
			/*
			 * OK, get the element
			 */
			char	   *ptr = fctx->elemdataptr2;
			
			fcinfo->isnull = false;
			elem2 = fetch_att(ptr, fctx->elmbyval2, fctx->elmlen2);
			
			/*
			 * Advance elemdataptr over it
			 */
			ptr = att_addlength_pointer(ptr, fctx->elmlen2, ptr);
			ptr = (char *) att_align_nominal(ptr, fctx->elmalign2);
			fctx->elemdataptr2 = ptr;
		}
		
		memset(isnull, 0, sizeof(isnull));
		funcClass = get_call_result_type(fcinfo, &resultType, &resultDesc);
		
		BlessTupleDesc(resultDesc);
		StartCopyToHeapTuple(resultDatum, isnull);
		CopyDatumToHeapTuple(elem);
		CopyDatumToHeapTuple(elem2);
		EndCopyToDatum;
		
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(heap_form_tuple(resultDesc, resultDatum, isnull)));
	}
	else
	{
		/* do when there is no more left */
		SRF_RETURN_DONE(funcctx);
	}
}

Datum uniform_series_sfunc(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(uniform_series_sfunc);
Datum uniform_series_sfunc(PG_FUNCTION_ARGS)
{
	ArrayType *row_array1 = PG_GETARG_ARRAYTYPE_P(0);
	if(PG_ARGISNULL(3)){
		ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("values of the arguments cannot be null")));
	}
	
		float8 value2 = PG_GETARG_TIMESTAMP(1)/1000000.0;
		float8 *result = (float8*) palloc(sizeof(float8)*9);
		memset(result, 0, sizeof(float8)*9);
		int nitems1 = ARR_DIMS(row_array1)[0];
		float8 *array1 = NULL;
		int result_size = 9;
	
		if(PG_ARGISNULL(1)||PG_ARGISNULL(2)){
			if(nitems1 < 9){
				result_size = 2;
				pfree(result);
				result = (float8*) palloc(sizeof(float8)*2);
				memset(result, 0, sizeof(float8)*2);
			}else{
				array1 = (float8*)ARR_DATA_PTR(row_array1);
				memcpy(result,array1,sizeof(float8)*9);
				result[7] = 0;
			}
		}else{
			float8 value3 = PG_GETARG_FLOAT8(2);
		Interval *interval = PG_GETARG_INTERVAL_P(3);
		float8 value4 = 0;
		fsec_t fsec;
		struct pg_tm tt, *tm = &tt;
		if (interval2tm(*interval, tm, &fsec) == 0)
		{
			value4 = tm->tm_sec + fsec/1000000.0;
		}else{
			ereport(ERROR,
            	(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            	 errmsg("interval value parameter is not a valid interval")));
		}
			if(nitems1 < 9){
				result[0] = value2;
				result[1] = value3;
				result[2] = value2;
				result[3] = value3;
				result[4] = value2;
				result[5] = value3;
				result[6] = (int64) value4;
				result[7] = 0;
				result[8] = 0;
			}else{
				array1 = (float8*)ARR_DATA_PTR(row_array1);
				result[2]= array1[4];
				result[3]= array1[5];
				result[4]= value2;
				result[5]= value3;
				result[6]= array1[6];
				result[7]= (int64)(result[4]-array1[0])/array1[6];
				result[8]= (result[4]-result[2]);
				if(result[7] >= 1){
					result[0] = array1[0]+array1[6]*result[7];
					result[1] = (result[0]-result[2])/result[8]*result[5] + (result[4]-result[0])/result[8]*result[3];
				}else{
					result[0] = array1[0];
					result[1] = array1[1];
				}
			}
		}
				
		ArrayType *pgarray;
		pgarray = construct_array((Datum *)result,
		result_size,FLOAT8OID,
		sizeof(float8),true,'d');
		PG_RETURN_ARRAYTYPE_P(pgarray);
 }
 
Datum uniform_series_start_sfunc(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(uniform_series_start_sfunc);
Datum uniform_series_start_sfunc(PG_FUNCTION_ARGS)
{
	ArrayType *row_array1 = PG_GETARG_ARRAYTYPE_P(0);
	if(PG_ARGISNULL(3)||PG_ARGISNULL(4)){
		ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("values of the arguments cannot be null")));
	}
	Interval *interval = PG_GETARG_INTERVAL_P(3);
	float8 value4 = 0;
	fsec_t fsec;
	struct pg_tm tt, *tm = &tt;
	if (interval2tm(*interval, tm, &fsec) == 0)
	{
		value4 = tm->tm_sec + fsec/1000000.0;
	}else{
		ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("interval value parameter is not a valid interval")));
	}
	float8 value5 = PG_GETARG_TIMESTAMP(4)/1000000.0;
	
	float8 *result = (float8*) palloc(sizeof(float8)*9);
	memset(result, 0, sizeof(float8)*9);
	int nitems1 = ARR_DIMS(row_array1)[0];
	float8 *array1 = NULL;
	int result_size = 9;
	
	if(PG_ARGISNULL(1)||PG_ARGISNULL(2)){
			if(nitems1 < 9){
				result_size = 2;
				pfree(result);
				result = (float8*) palloc(sizeof(float8)*2);
				memset(result, 0, sizeof(float8)*2);
			}else{
				array1 = (float8*)ARR_DATA_PTR(row_array1);
				memcpy(result,array1,sizeof(float8)*9);
				result[7] = 0;
			}
		}else{
			float8 value2  = PG_GETARG_TIMESTAMP(1)/1000000.0;
			float8 value3 = PG_GETARG_FLOAT8(2);
	if(nitems1 < 9){
		result[0] = value5;
		result[1] = value3;
		result[2] = value5;
		result[3] = value3;
		result[4] = value2;
		result[5] = value3;
		result[6] = (int64) value4;
		result[7] = (int64)(result[4]-result[0])/result[6];
		result[8] = (result[4]-result[0]);
		if(result[7] >= 1){
			result[0] = result[0]+result[6]*result[7];
		}
	}else{
		array1 = (float8*)ARR_DATA_PTR(row_array1);
		result[2]= array1[4];
		result[3]= array1[5];
		result[4]= value2;
		result[5]= value3;
		result[6]= array1[6];
		result[7]= (int64)(result[4]-array1[0])/array1[6];
		result[8]= (result[4]-result[2]);
		if(result[7] >= 1){
			result[0] = array1[0]+array1[6]*result[7];
			result[1] = (result[0]-result[2])/result[8]*result[5] + (result[4]-result[0])/result[8]*result[3];
		}else{
			result[0] = array1[0];
			result[1] = array1[1];
		}
	}
	}
 
 	ArrayType *pgarray;
	pgarray = construct_array((Datum *)result,
		result_size,FLOAT8OID,
		sizeof(float8),true,'d');
	PG_RETURN_ARRAYTYPE_P(pgarray);
 }

Datum uniform_series_prefunc(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(uniform_series_prefunc);
Datum uniform_series_prefunc(PG_FUNCTION_ARGS)
{
	
	ArrayType *row_array1 = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType *row_array2 = PG_GETARG_ARRAYTYPE_P(1);
	
	float8 *result = (float8*) palloc(sizeof(float8)*9);
	memset(result, 0, sizeof(float8)*9);
	int nitems1 = ARR_DIMS(row_array1)[0];
	float8 *array1 = NULL;
	
	if(nitems1 < 9){
		array1 = (float8*)ARR_DATA_PTR(row_array1);
	}else{
		array1 = (float8*)ARR_DATA_PTR(row_array2);
	}
	
 	ArrayType *pgarray;
	pgarray = construct_array((Datum *)array1,
							  9,FLOAT8OID,
							  sizeof(float8),true,'d');
	PG_RETURN_ARRAYTYPE_P(pgarray);
}

Datum uniform_series_finalfunc(PG_FUNCTION_ARGS);

#undef StartCopyFromTuple
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
#define EndCopyToDatum }

PG_FUNCTION_INFO_V1(uniform_series_finalfunc);
Datum uniform_series_finalfunc(PG_FUNCTION_ARGS)
{
	TypeFuncClass funcClass;
	Oid resultType;
	TupleDesc resultDesc;
	Datum resultDatum[10];
	bool isnull[2];
	ArrayType *pgarray1, *pgarray2;
	
	ArrayType *row_array1 = PG_GETARG_ARRAYTYPE_P(0);
	float8 *result = NULL;
	float8 *rep = NULL;
	Timestamp *rep_time = NULL;
	float8 *array1 = (float8*)ARR_DATA_PTR(row_array1);
	int nitems1 = ARR_DIMS(row_array1)[0];
	int64 max_size = (int64)(nitems1 > 2)?array1[7]:0;
	max_size = (max_size > 0)?max_size:0;
	int i = max_size-1;
	
	if(max_size >= 1){
		result = (float8*) palloc(sizeof(float8)*max_size);
		rep = (float8*) palloc(sizeof(float8)*max_size);
		rep_time = (Timestamp*) palloc(sizeof(Timestamp)*max_size);
		for(;i >= 0; --i){
			rep[max_size-1-i] = array1[0]-i*array1[6];
			result[max_size-1-i] = (rep[max_size-1-i]-array1[2])/array1[8]*array1[5]+(array1[4]-rep[max_size-1-i])/array1[8]*array1[3];
			rep_time[max_size-1-i] = (Timestamp)(rep[max_size-1-i]*(double)1000000.0);
		}
	}
	
	pgarray1 = construct_array((Datum *)rep_time,
							  max_size,INT8OID,
							  sizeof(Datum),true,'d');
	pgarray2 = construct_array((Datum *)result,
							  max_size,FLOAT8OID,
							  sizeof(Datum),true,'d');
	memset(isnull, 0, sizeof(isnull));
	
	funcClass = get_call_result_type(fcinfo, &resultType, &resultDesc);
	BlessTupleDesc(resultDesc);
	
	StartCopyToHeapTuple(resultDatum, isnull);
	CopyToHeapTuple(pgarray1, PointerGetDatum);
    CopyToHeapTuple(pgarray2, PointerGetDatum);
	EndCopyToDatum;
	
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(resultDesc, resultDatum, isnull)));
}

Datum uniform_series_const_finalfunc(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(uniform_series_const_finalfunc);
Datum uniform_series_const_finalfunc(PG_FUNCTION_ARGS)
{
	TypeFuncClass funcClass;
	Oid resultType;
	TupleDesc resultDesc;
	Datum resultDatum[10];
	bool isnull[2];
	ArrayType *pgarray1, *pgarray2;
	
	ArrayType *row_array1 = PG_GETARG_ARRAYTYPE_P(0);
	float8 *result = NULL;
	float8 *rep = NULL;
	Timestamp *rep_time = NULL;
	float8 *array1 = (float8*)ARR_DATA_PTR(row_array1);
	int nitems1 = ARR_DIMS(row_array1)[0];
	int64 max_size = (int64)(nitems1 > 2)?array1[7]:0;
	max_size = (max_size > 0)?max_size:0;
	int i = max_size-1;
	
	if(max_size >= 1){
		result = (float8*) palloc(sizeof(float8)*max_size);
		rep = (float8*) palloc(sizeof(float8)*max_size);
		rep_time = (Timestamp*) palloc(sizeof(Timestamp)*max_size);
		for(;i >= 0; --i){
			rep[max_size-1-i] = array1[0]-i*array1[6];
			result[max_size-1-i] = array1[3];
			rep_time[max_size-1-i] = (Timestamp)(rep[max_size-1-i]*(double)1000000.0);
		}
	}
	
	pgarray1 = construct_array((Datum *)rep_time,
							  max_size,INT8OID,
							  sizeof(Datum),true,'d');
	pgarray2 = construct_array((Datum *)result,
							  max_size,FLOAT8OID,
							  sizeof(Datum),true,'d');
	memset(isnull, 0, sizeof(isnull));
	
	funcClass = get_call_result_type(fcinfo, &resultType, &resultDesc);
	BlessTupleDesc(resultDesc);
	
	StartCopyToHeapTuple(resultDatum, isnull);
	CopyToHeapTuple(pgarray1, PointerGetDatum);
    CopyToHeapTuple(pgarray2, PointerGetDatum);
	EndCopyToDatum;
	
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(resultDesc, resultDatum, isnull)));
}

/*************************
Uniform Series Include Original

The other parts are identical to Uniform Series, so are not replicated 
**************************/
 
Datum uniform_series_include_original_finalfunc(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(uniform_series_include_original_finalfunc);
Datum uniform_series_include_original_finalfunc(PG_FUNCTION_ARGS)
{
	TypeFuncClass funcClass;
	Oid resultType;
	TupleDesc resultDesc;
	Datum resultDatum[10];
	bool isnull[2], begin = FALSE;
	ArrayType *pgarray1, *pgarray2;
	
	ArrayType *row_array1 = PG_GETARG_ARRAYTYPE_P(0);
	float8 *result = NULL;
	float8 *rep = NULL;
	Timestamp *rep_time = NULL;
	float8 *array1 = (float8*)ARR_DATA_PTR(row_array1);
	int nitems1 = ARR_DIMS(row_array1)[0];
	int64 max_size = (int64)(nitems1 > 2)?array1[7]:0;
	max_size = (max_size > 0)?max_size:0;
	
	int i = max_size-1;
	if((array1[0]-array1[4]) != (int)(array1[0]-array1[4])){
		max_size++;
		begin = TRUE;
	}
	
	if(max_size >= 1){
		result = (float8*) palloc(sizeof(float8)*max_size);
		rep = (float8*) palloc(sizeof(float8)*max_size);
		rep_time = (Timestamp*) palloc(sizeof(Timestamp)*max_size);
		if(begin){
			rep[0] = array1[2];
			result[0] = array1[3];
			rep_time[0] = (Timestamp)(rep[0]*(double)1000000.0);
		}
		for(;i >= 0; --i){
			rep[max_size-1-i] = array1[0]-i*array1[6];
			result[max_size-1-i] = (rep[max_size-1-i]-array1[2])/array1[8]*array1[5]+(array1[4]-rep[max_size-1-i])/array1[8]*array1[3];
			rep_time[max_size-1-i] = (Timestamp)(rep[max_size-1-i]*(double)1000000.0);
		}
	}
	
	pgarray1 = construct_array((Datum *)rep_time,
							  max_size,INT8OID,
							  sizeof(Datum),true,'d');
	pgarray2 = construct_array((Datum *)result,
							  max_size,FLOAT8OID,
							  sizeof(Datum),true,'d');
	memset(isnull, 0, sizeof(isnull));
	
	funcClass = get_call_result_type(fcinfo, &resultType, &resultDesc);
	BlessTupleDesc(resultDesc);
	
	StartCopyToHeapTuple(resultDatum, isnull);
	CopyToHeapTuple(pgarray1, PointerGetDatum);
    CopyToHeapTuple(pgarray2, PointerGetDatum);
	EndCopyToDatum;
	
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(resultDesc, resultDatum, isnull)));
}

Datum uniform_series_const_include_original_finalfunc(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(uniform_series_const_include_original_finalfunc);
Datum uniform_series_const_include_original_finalfunc(PG_FUNCTION_ARGS)
{
	TypeFuncClass funcClass;
	Oid resultType;
	TupleDesc resultDesc;
	Datum resultDatum[10];
	bool isnull[2], begin = FALSE;
	ArrayType *pgarray1, *pgarray2;
	
	ArrayType *row_array1 = PG_GETARG_ARRAYTYPE_P(0);
	float8 *result = NULL;
	float8 *rep = NULL;
	Timestamp *rep_time = NULL;
	float8 *array1 = (float8*)ARR_DATA_PTR(row_array1);
	int nitems1 = ARR_DIMS(row_array1)[0];
	int64 max_size = (int64)(nitems1 > 2)?array1[7]:0;
	max_size = (max_size > 0)?max_size:0;
	int i = max_size-1;
	if((array1[0]-array1[4]) != (int)(array1[0]-array1[4])){
		max_size++;
		begin = TRUE;
	}
	
	if(max_size >= 1){
		result = (float8*) palloc(sizeof(float8)*max_size);
		rep = (float8*) palloc(sizeof(float8)*max_size);
		rep_time = (Timestamp*) palloc(sizeof(Timestamp)*max_size);
		if(begin){
			rep[0] = array1[2];
			result[0] = array1[3];
			rep_time[0] = (Timestamp)(rep[0]*(double)1000000.0);
		}
		for(;i >= 0; --i){
			rep[max_size-1-i] = array1[0]-i*array1[6];
			result[max_size-1-i] = array1[3];
			rep_time[max_size-1-i] = (Timestamp)(rep[max_size-1-i]*(double)1000000.0);
		}
	}
	
	pgarray1 = construct_array((Datum *)rep_time,
							  max_size,INT8OID,
							  sizeof(Datum),true,'d');
	pgarray2 = construct_array((Datum *)result,
							  max_size,FLOAT8OID,
							  sizeof(Datum),true,'d');
	memset(isnull, 0, sizeof(isnull));
	
	funcClass = get_call_result_type(fcinfo, &resultType, &resultDesc);
	BlessTupleDesc(resultDesc);
	
	StartCopyToHeapTuple(resultDatum, isnull);
	CopyToHeapTuple(pgarray1, PointerGetDatum);
    CopyToHeapTuple(pgarray2, PointerGetDatum);
	EndCopyToDatum;
	
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(resultDesc, resultDatum, isnull)));
}


/*************************
GAP FILLED SERIES
*************************/


Datum gapfilled_series_sfunc(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(gapfilled_series_sfunc);
Datum gapfilled_series_sfunc(PG_FUNCTION_ARGS)
{
	ArrayType *row_array1 = PG_GETARG_ARRAYTYPE_P(0);
	if(PG_ARGISNULL(3)){
		ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("values of the arguments cannot be null")));
	}
	Interval *interval = PG_GETARG_INTERVAL_P(3);
	float8 value4 = 0;
	fsec_t fsec;
	struct pg_tm tt, *tm = &tt;
	if (interval2tm(*interval, tm, &fsec) == 0)
	{
		value4 = tm->tm_sec + fsec/1000000.0;
	}else{
		ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("interval value parameter is not a valid interval")));
        value4 = 0;
	}
	
	float8 *result = (float8*) palloc(sizeof(float8)*9);
	memset(result, 0, sizeof(float8)*9);
	int nitems1 = ARR_DIMS(row_array1)[0];
	float8 *array1 = NULL;
	int result_size = 9;
		
	if(PG_ARGISNULL(1)||PG_ARGISNULL(2)){
			if(nitems1 < 9){
				result_size = 2;
				pfree(result);
				result = (float8*) palloc(sizeof(float8)*2);
				memset(result, 0, sizeof(float8)*2);
			}else{
				array1 = (float8*)ARR_DATA_PTR(row_array1);
				memcpy(result,array1,sizeof(float8)*9);
				result[7] = 0;
			}
		}else{
			float8 value2 = PG_GETARG_TIMESTAMP(1)/1000000.0;
			float8 value3 = PG_GETARG_FLOAT8(2);
			result[0] = value2;
	result[1] = value3;
	result[4] = value2;
	result[5] = value3;
	result[6] = (int64) value4;
	if(nitems1 < 9){
		result[2] = value2;
		result[3] = value3;
		result[7] = 1;
		result[8] = 0;
	}else{
		array1 = (float8*)ARR_DATA_PTR(row_array1);
		result[2]= array1[4];
		result[3]= array1[5];
		result[7]= ceil((result[4]-result[2])/result[6]);
		result[8]= (result[4]-result[2]);
	}
		}
 
 	ArrayType *pgarray;
	pgarray = construct_array((Datum *)result,
	 	result_size,FLOAT8OID,
		sizeof(float8),true,'d');
	PG_RETURN_ARRAYTYPE_P(pgarray);
 }
 
Datum gapfilled_series_start_sfunc(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(gapfilled_series_start_sfunc);
Datum gapfilled_series_start_sfunc(PG_FUNCTION_ARGS)
{
	ArrayType *row_array1 = PG_GETARG_ARRAYTYPE_P(0);
	if(PG_ARGISNULL(3)||PG_ARGISNULL(4)){
		ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("values of the arguments cannot be null")));
	}
	Interval *interval = PG_GETARG_INTERVAL_P(3);
	float8 value4 = 0;
	fsec_t fsec;
	struct pg_tm tt, *tm = &tt;
	if (interval2tm(*interval, tm, &fsec) == 0)
	{
		value4 = tm->tm_sec + fsec/1000000.0;
	}else{
		ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("interval value parameter is not a valid interval")));
	}
	float8 value5 = PG_GETARG_TIMESTAMP(4)/1000000.0;
	float8 *result = (float8*) palloc(sizeof(float8)*9);
	memset(result, 0, sizeof(float8)*9);
	int nitems1 = ARR_DIMS(row_array1)[0];
	float8 *array1 = NULL;
	int result_size = 9;
	
	if(PG_ARGISNULL(1)||PG_ARGISNULL(2)){
			if(nitems1 < 9){
				result_size = 2;
				pfree(result);
				result = (float8*) palloc(sizeof(float8)*2);
				memset(result, 0, sizeof(float8)*2);
			}else{
				array1 = (float8*)ARR_DATA_PTR(row_array1);
				memcpy(result,array1,sizeof(float8)*9);
				result[7] = 0;
			}
		}else{
			 float8 value2 = PG_GETARG_TIMESTAMP(1)/1000000.0;
	float8 value3 = PG_GETARG_FLOAT8(2);
	if(nitems1 < 9){
		result[1] = value3;
		result[2] = value5;
		result[3] = value3;
	}else{
		array1 = (float8*)ARR_DATA_PTR(row_array1);
		result[1]= array1[5];
		result[2]= array1[4];
		result[3]= array1[5];
	}
	result[0]= value2;
	result[4] = value2;
	result[5] = value3;
	result[6] = (int64) value4;
	result[7]= ceil((result[4]-result[2])/result[6]);
	result[8] = (result[4]-result[2]);
		}
 
 	ArrayType *pgarray;
	pgarray = construct_array((Datum *)result,
		result_size,FLOAT8OID,
		sizeof(float8),true,'d');
	PG_RETURN_ARRAYTYPE_P(pgarray);
 }

Datum gapfilled_series_prefunc(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(gapfilled_series_prefunc);
Datum gapfilled_series_prefunc(PG_FUNCTION_ARGS)
{
	
	ArrayType *row_array1 = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType *row_array2 = PG_GETARG_ARRAYTYPE_P(1);
	
	
	float8 *result = (float8*) palloc(sizeof(float8)*9);
	memset(result, 0, sizeof(float8)*9);
	int nitems1 = ARR_DIMS(row_array1)[0];
	float8 *array1 = NULL;
	
	if(nitems1 < 9){
		array1 = (float8*)ARR_DATA_PTR(row_array1);
	}else{
		array1 = (float8*)ARR_DATA_PTR(row_array2);
	}
	
 	ArrayType *pgarray;
	pgarray = construct_array((Datum *)array1,
							  9,FLOAT8OID,
							  sizeof(float8),true,'d');
	PG_RETURN_ARRAYTYPE_P(pgarray);
}

Datum gapfilled_series_finalfunc(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(gapfilled_series_finalfunc);
Datum gapfilled_series_finalfunc(PG_FUNCTION_ARGS)
{
	TypeFuncClass funcClass;
	Oid resultType;
	TupleDesc resultDesc;
	Datum resultDatum[10];
	bool isnull[2];
	ArrayType *pgarray1, *pgarray2;
	
	ArrayType *row_array1 = PG_GETARG_ARRAYTYPE_P(0);
	float8 *result = NULL;
	float8 *rep = NULL;
	Timestamp *rep_time = NULL;
	float8 *array1 = (float8*)ARR_DATA_PTR(row_array1);
	int nitems1 = ARR_DIMS(row_array1)[0];
	int64 max_size = (int64)(nitems1 > 2)?array1[7]:0;
	max_size = (max_size > 0)?max_size:0;
	int i = max_size-1;
	
	result = (float8*) palloc(sizeof(float8)*max_size);
	rep = (float8*) palloc(sizeof(float8)*max_size);
	rep_time = (Timestamp*) palloc(sizeof(Timestamp)*max_size);
	if(array1[8] == 0){
		rep[max_size-1-i] = array1[4]-i*array1[6];
		result[max_size-1-i] = array1[5];
		rep_time[max_size-1-i] = (Timestamp)(rep[max_size-1-i]*(double)1000000.0);
	}else{
	for(;i >= 0; --i){
		rep[max_size-1-i] = array1[4]-i*array1[6];
		result[max_size-1-i] = (rep[max_size-1-i]-array1[2])/array1[8]*array1[5]+(array1[4]-rep[max_size-1-i])/array1[8]*array1[3];
		rep_time[max_size-1-i] = (Timestamp)(rep[max_size-1-i]*(double)1000000.0);
	}
	}
	
	pgarray1 = construct_array((Datum *)rep_time,
							  max_size,INT8OID,
							  sizeof(Datum),true,'d');
	pgarray2 = construct_array((Datum *)result,
							  max_size,FLOAT8OID,
							  sizeof(Datum),true,'d');
	memset(isnull, 0, sizeof(isnull));
	
	funcClass = get_call_result_type(fcinfo, &resultType, &resultDesc);
	BlessTupleDesc(resultDesc);
	
	StartCopyToHeapTuple(resultDatum, isnull);
	CopyToHeapTuple(pgarray1, PointerGetDatum);
    CopyToHeapTuple(pgarray2, PointerGetDatum);
	EndCopyToDatum;
	
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(resultDesc, resultDatum, isnull)));
}

Datum gapfilled_series_const_finalfunc(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(gapfilled_series_const_finalfunc);
Datum gapfilled_series_const_finalfunc(PG_FUNCTION_ARGS)
{
	TypeFuncClass funcClass;
	Oid resultType;
	TupleDesc resultDesc;
	Datum resultDatum[10];
	bool isnull[2];
	ArrayType *pgarray1, *pgarray2;
	
	ArrayType *row_array1 = PG_GETARG_ARRAYTYPE_P(0);
	float8 *result = NULL;
	float8 *rep = NULL;
	Timestamp *rep_time = NULL;
	float8 *array1 = (float8*)ARR_DATA_PTR(row_array1);
	int nitems1 = ARR_DIMS(row_array1)[0];
	int64 max_size = (int64)(nitems1 > 2)?array1[7]:0;
	max_size = (max_size > 0)?max_size:0;
	int i = max_size-1;
	
	result = (float8*) palloc(sizeof(float8)*max_size);
	rep = (float8*) palloc(sizeof(float8)*max_size);
	rep_time = (Timestamp*) palloc(sizeof(Timestamp)*max_size);
	if(array1[8] == 0){
		rep[max_size-1-i] = array1[4]-i*array1[6];
		result[max_size-1-i] = array1[5];
		rep_time[max_size-1-i] = (Timestamp)(rep[max_size-1-i]*(double)1000000.0);
	}else{
	for(;i >= 0; --i){
		rep[max_size-1-i] = array1[4]-i*array1[6];
		result[max_size-1-i] = array1[3];
		rep_time[max_size-1-i] = (Timestamp)(rep[max_size-1-i]*(double)1000000.0);
	}
	}
	
	pgarray1 = construct_array((Datum *)rep_time,
							  max_size,INT8OID,
							  sizeof(Datum),true,'d');
	pgarray2 = construct_array((Datum *)result,
							  max_size,FLOAT8OID,
							  sizeof(Datum),true,'d');
	memset(isnull, 0, sizeof(isnull));
	
	funcClass = get_call_result_type(fcinfo, &resultType, &resultDesc);
	BlessTupleDesc(resultDesc);
	
	StartCopyToHeapTuple(resultDatum, isnull);
	CopyToHeapTuple(pgarray1, PointerGetDatum);
    CopyToHeapTuple(pgarray2, PointerGetDatum);
	EndCopyToDatum;
	
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(resultDesc, resultDatum, isnull)));
}

