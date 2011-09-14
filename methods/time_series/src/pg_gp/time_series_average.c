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

/*************************
WEIGHTED AVG FINANCE
*************************/

Datum weighted_avg_finalfunc(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(weighted_avg_finalfunc);
Datum weighted_avg_finalfunc(PG_FUNCTION_ARGS)
{
	ArrayType *v = PG_GETARG_ARRAYTYPE_P(0);
	float8 result = 0;
	float8 *dat = NULL;
	int *dims, ndims,nitems;
	int i = 0, sum = 0;
	
	if(PG_ARGISNULL(0)||ARR_HASNULL(v)){
		PG_RETURN_NULL();
	}
	
	dims = ARR_DIMS(v);
	ndims = ARR_NDIM(v);
	
	if (ndims == 0){
		Datum ret = 0;
		return ret;
	}
	
	dat = (float8*)ARR_DATA_PTR(v);
	nitems = ArrayGetNItems(ndims, dims);	
	for(; i < nitems; ++i){
		result += (i+1)*dat[i];
		sum += (i+1);
	}
	
	PG_RETURN_FLOAT8(result/(float8)sum);
}

/*************************
EXPONENTIAL AVG
*************************/

Datum exponential_avg_finalfunc(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(exponential_avg_finalfunc);
Datum exponential_avg_finalfunc(PG_FUNCTION_ARGS)
{
	HeapTupleHeader input;
	bool isNull;
	float8 result = 0;
	float8 *dat = NULL;
	int *dims, ndims,nitems;
	int i = 1;
	
	if(PG_ARGISNULL(0)){
		PG_RETURN_NULL();
	}
	
	input = PG_GETARG_HEAPTUPLEHEADER(0);
	float8 tmp = DatumGetFloat8(GetAttributeByName(input, "alpha", &isNull));
	if(isNull) { PG_RETURN_NULL(); }
	ArrayType *tmp2 = DatumGetArrayTypeP(GetAttributeByName(input, "val", &isNull));
	if(isNull||ARR_HASNULL(tmp2)) { PG_RETURN_NULL(); }
	
	dims = ARR_DIMS(tmp2);
	ndims = ARR_NDIM(tmp2);
	
	if (ndims == 0){
		PG_RETURN_FLOAT8(0.0);
	}
	
	dat = (float8*)ARR_DATA_PTR(tmp2);
	nitems = ArrayGetNItems(ndims, dims);
	if (nitems == 0){
		PG_RETURN_FLOAT8(result);
	}
	
	result = dat[0];
	for(; i < nitems; ++i){
		result = tmp*dat[i]+(1-tmp)*result;
	}
	
	PG_RETURN_FLOAT8(result);
}

/*************************
WEIGHTED AVG ARRAY BASED
*************************/

Datum weighted_avg_finalfunc_full(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(weighted_avg_finalfunc_full);
Datum weighted_avg_finalfunc_full(PG_FUNCTION_ARGS)
{
	HeapTupleHeader input;
	bool isNull;
	float8 result = 0;
	float8 *dat1 = NULL, *dat2 = NULL, sum_so_far = 0.0;
	int *dims1,ndims1,nitems1,*dims2,ndims2,nitems2 ;
	int i=0, j=0;
	
	if(PG_ARGISNULL(0)){
		PG_RETURN_NULL();
	}
	input = PG_GETARG_HEAPTUPLEHEADER(0);
	ArrayType *tmp1 = DatumGetArrayTypeP(GetAttributeByName(input, "val", &isNull));
	if(isNull||ARR_HASNULL(tmp1)) { PG_RETURN_NULL(); }
	ArrayType *tmp2 = DatumGetArrayTypeP(GetAttributeByName(input, "weight", &isNull));
	if(isNull||ARR_HASNULL(tmp2)) { PG_RETURN_NULL(); }
	
	dims1 = ARR_DIMS(tmp1);
	ndims1 = ARR_NDIM(tmp1);
	dims2 = ARR_DIMS(tmp2);
	ndims2 = ARR_NDIM(tmp2);
	
	if(ndims1 != ndims2){
		ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("size of window and size of weight vector must be the same")));
	}
	if (ndims1 == 0){
		PG_RETURN_FLOAT8(result);
	}
	
	dat1 = (float8*)ARR_DATA_PTR(tmp1);
	dat2 = (float8*)ARR_DATA_PTR(tmp2);
	nitems1 = ArrayGetNItems(ndims1, dims1);
	nitems2 = ArrayGetNItems(ndims2, dims2);
	if(nitems1 > nitems2){
		ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("size of window and size of weight vector must be the same")));
	}
	if(nitems1 == 0){
		PG_RETURN_FLOAT8(result);
	}
	
	for(i = nitems1-1, j = nitems2-1; j >= 0; --i, --j){
		result += dat2[i]*dat1[i];
		sum_so_far += dat2[i];
	}
	
	PG_RETURN_FLOAT8(result/sum_so_far);
}

