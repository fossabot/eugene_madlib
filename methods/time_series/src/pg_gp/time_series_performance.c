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
#include "nodes/execnodes.h"
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
PERFORMANCE QUERY
**************************/

typedef struct{
	int32 row_size;
	Oid sfunc;
	int64 time_interval_to_preserve;
	int64 last_signal_time;
	int64 last_cleanup;
	float8 last_trade_price;
	int64 number_live_signal;
	int64 number_elements;
	int64 number_reserved;
	int64 number_dead;
	float8 final_value;
} header_performance_struct_array;

#define INIT_STRUCT_SIZE 2000

#define ELEM_ARR(x,y,z)\
&(z)[(y)*(x)]
#define ELEM_ARR_TRADE(x,y,z)\
DatumGetBool(((Datum*)&(z)[(y)*(x)])[0])
#define ELEM_ARR_TIMESTAMP(x,y,z)\
DatumGetInt64(((Datum*)&(z)[(y)*(x)])[1])
#define ELEM_ARR_PRICE(x,y,z)\
DatumGetFloat8(((Datum*)&(z)[(y)*(x)])[2])
#define ELEM_ARR_PRICE_DATUM(x,y,z)\
((Datum*)&(z)[(y)*(x)])[2]
#define ELEM_ARR_STATUS(x,y,z)\
DatumGetBool(((Datum*)&(z)[(y)*(x)])[((y)/sizeof(Datum))-1])
#define ELEM_ARR_STATUS_DATUM(x,y,z)\
((Datum*)&(z)[(y)*(x)])[((y)/sizeof(Datum))-1]
#define ELEM_ARR_GET_DATUM(x,y,z,p)\
((Datum*)&(z)[(y)*(x)])[p]

//These macros work only if the layout of the data is as 
//assumed by the header
#define ELEM_ARR_QUONT(x,y,z)\
DatumGetInt64(((Datum*)&(z)[(y)*(x)])[3])
#define ELEM_ARR_SIGNAL(x,y,z)\
DatumGetFloat8(((Datum*)&(z)[(y)*(x)])[4])

#define DATA_RAW(x)\
((char*)VARDATA(x))
#define DATA_HEADER(x)\
(header_performance_struct_array *)(((char*)(x))+VARHDRSZ)
#define DATA_ROW_ARRAY(x, y)\
((char*)(x))+VARHDRSZ+(y)

Datum performance_window(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(performance_window);
Datum performance_window(PG_FUNCTION_ARGS){
	bytea *e;
	char *elements = NULL;
	int64 elements_size = 0, row_size = 0;
	int64 i = 0;
	float8 max = 0, min = 0, last = 0;
	int result_size = 8;
	float8* result = NULL;
	float8 init_price = 0;
	int64 init_time = 0;
	float8 init_signal = 0;
	int64 total_time = 0;
	ArrayType *pgarray;
	
	result = (float8 *)palloc(sizeof(float8)*result_size);
	memset(result, 0, sizeof(float8)*result_size); //WORK
	
	if(PG_ARGISNULL(0)||PG_ARGISNULL(1)){
		pgarray = construct_array((Datum *)result,
		result_size,FLOAT8OID,
		sizeof(float8),true,'d');
		PG_RETURN_ARRAYTYPE_P(pgarray);
	}
	
	e = PG_GETARG_BYTEA_P(0); 
	row_size = PG_GETARG_INT64(1);
	
	if(row_size <= 0){
		pgarray = construct_array((Datum *)result,
		result_size,FLOAT8OID,
		sizeof(float8),true,'d');
		PG_RETURN_ARRAYTYPE_P(pgarray);
	}else{
		elements_size =  VARSIZE_ANY_EXHDR(e)/row_size;
		if(elements_size == 0){
			ArrayType *pgarray;
			pgarray = construct_array((Datum *)result,
			result_size,FLOAT8OID,
			sizeof(float8),true,'d');
			PG_RETURN_ARRAYTYPE_P(pgarray);
		}
	}
	
	elements = DATA_RAW(PG_GETARG_BYTEA_P(0));
	init_price = ELEM_ARR_PRICE((elements_size-1),row_size,elements);
	min = init_price;
	max = init_price;
	init_time = ELEM_ARR_TIMESTAMP((elements_size-1),row_size,elements);
	init_signal = ELEM_ARR_SIGNAL((elements_size-1),row_size,elements);
	total_time = init_time;
	
	//do actual integration
	for(i = elements_size-1; i >= 0; --i){
		if(ELEM_ARR_TRADE(i,row_size,elements)){
			result[3] += (ELEM_ARR_PRICE(i,row_size,elements)-init_price)*(ELEM_ARR_TIMESTAMP(i,row_size,elements)-total_time);
			total_time = ELEM_ARR_TIMESTAMP(i,row_size,elements);
		}else{
			if(init_signal*ELEM_ARR_SIGNAL(i,row_size,elements) < 0){
				break;
			}
		}
	}	
	
	//do actual integration
	for(i = 0; i < elements_size-1; ++i){
		if(ELEM_ARR_TRADE(i,row_size,elements)){
			if(ELEM_ARR_PRICE(i,row_size,elements) > max){
				max = ELEM_ARR_PRICE(i,row_size,elements);
			}
			if(ELEM_ARR_PRICE(i,row_size,elements) < min){
				min = ELEM_ARR_PRICE(i,row_size,elements);
			}
			if(last == 0){
				last = ELEM_ARR_PRICE(i,row_size,elements);
			}
		}
	}	
	
	total_time -= init_time;
	result[0] = ((float8)init_time)/1000000.0;
	result[1] = init_signal;
	result[2] = ((init_signal > 0)?1:-1);
	result[3] = (total_time > 0)?result[1]/total_time:0;
	result[4] = (min-init_price);
	result[5] = (max-init_price);
	result[6] = (last-init_price);
	result[7] = init_price;
	
	pgarray = construct_array((Datum *)result,
	result_size,FLOAT8OID,
	sizeof(float8),true,'d');
	PG_RETURN_ARRAYTYPE_P(pgarray);}

/**************************

PERFORMANCE TESTING REGRESSION WINDOW

***************************/

Datum performance_array_window_sfunc(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(performance_array_window_sfunc);
Datum performance_array_window_sfunc(PG_FUNCTION_ARGS){
	int SIZE_HEADER_PERFORMANCE_STRUCT_ARRAY = sizeof(header_performance_struct_array);
	int input_size = PG_NARGS();
	int input_struct_size = 0;
	
	header_performance_struct_array *header_array = NULL;
	char *elements = NULL; 

	/*first few elements will be:
		BYTEA of the current state
		OID of sfunc
		INTERVAL of interest after a special row
		BOOL special
		TIMESTAMP of the current row
		PRICE of the current transaciton
	*/
	bytea *t2 = NULL, *t1 = NULL;
	char* byte_array = NULL;
	int64 len = 0;
	Interval *int_in;
	bool trade_or_signal = FALSE;
	int64 timestamp_in = 0;
	int row_header_size = 3;
	
	int64 i = 0;
	
	IOFuncSelector which_func = 0;
	int16 typlen = 0;
	bool typbyval = false;
	char typalign = ' ';
	char typdelim = ' ';
	Oid typioparam = 0;
	Oid func = 0;
	bool first_alloc = FALSE;
	
	/*
		This oprtion checks all the inputs and if this is the first call
		to the sfunc initial allocation happence
	*/	
	if (PG_ARGISNULL(0)){
		first_alloc = TRUE;	
	}else{
		t1 = (bytea*)(int)PG_GETARG_INT64(0);
		len = VARSIZE_ANY_EXHDR(t1);
		if(len == 0){
			first_alloc = TRUE;	
		}
	}
	
	if(first_alloc){
		for(i = row_header_size; i < input_size; ++i){
			get_type_io_data(get_fn_expr_argtype(fcinfo->flinfo, i), 
			which_func, 
			&typlen, 
			&typbyval, 
			&typalign, 
			&typdelim, 
			&typioparam, 
			&func);
			
			if(typbyval == FALSE){
				ereport(ERROR,
	            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             	errmsg("Input must be of pass by value type, and cannot be a complex type such as ARRAY or TEXT")));
			}
		}
		input_struct_size = (input_size-row_header_size+1)*sizeof(Datum);
		
		MemoryContext aggContext = ((WindowState *) fcinfo->context)->transcontext, oldContext = NULL;
		oldContext = MemoryContextSwitchTo(aggContext);
		t1 = (bytea*)palloc(VARHDRSZ+input_struct_size*INIT_STRUCT_SIZE+SIZE_HEADER_PERFORMANCE_STRUCT_ARRAY);
		MemoryContextSwitchTo(oldContext);
		
		//t1 = (bytea*)palloc(VARHDRSZ+input_struct_size*INIT_STRUCT_SIZE+SIZE_HEADER_PERFORMANCE_STRUCT_ARRAY);
		SET_VARSIZE(t1, VARHDRSZ+input_struct_size*INIT_STRUCT_SIZE+SIZE_HEADER_PERFORMANCE_STRUCT_ARRAY);
		header_array = DATA_HEADER(t1);
		header_array[0].row_size = input_struct_size;
		
		if(PG_ARGISNULL(1)){
			ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("Interval for internal aggregation cannot be NULL")));
		}
		
		int_in = PG_GETARG_INTERVAL_P(1);
		header_array[0].time_interval_to_preserve = int_in->time + int_in->month*INT64CONST(30)*USECS_PER_DAY + int_in->day*INT64CONST(24) * USECS_PER_HOUR;
		if (header_array[0].time_interval_to_preserve <= 0)
		{
			ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("interval value parameter is not a valid interval")));
		}
		
		header_array[0].number_elements = 0;
		header_array[0].number_reserved = INIT_STRUCT_SIZE;
		header_array[0].number_dead = 0;
		header_array[0].final_value = 0;
		header_array[0].last_trade_price = 0.0;
		header_array[0].last_signal_time = 0.0;
		header_array[0].number_live_signal = 0;
		if(PG_ARGISNULL(3)){
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("Timeseries based analysis cannot have NULL for timestamp value")));
		}
		header_array[0].last_cleanup = PG_GETARG_TIMESTAMP(3);
	}else{
		header_array = DATA_HEADER(t1);
	}
	
	if(PG_ARGISNULL(2)){
		ereport(ERROR,
        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
        errmsg("Boolean indicator special/regular row cannot be NULL")));
	}
	trade_or_signal = PG_GETARG_BOOL(2);
	
	if(PG_ARGISNULL(3)){
		ereport(ERROR,
        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
        errmsg("Timeseries based analysis cannot have NULL for timestamp value")));
	}
	timestamp_in = PG_GETARG_TIMESTAMP(3);
	header_array[0].last_signal_time = timestamp_in;
	elements = DATA_ROW_ARRAY(t1, SIZE_HEADER_PERFORMANCE_STRUCT_ARRAY);
             
	if(PG_ARGISNULL(4)&&(trade_or_signal)){
		PG_RETURN_BYTEA_P(t1);
	}
	
	if(trade_or_signal){
		header_array[0].last_trade_price = PG_GETARG_FLOAT8(4);
	}
	
	if(!trade_or_signal){
		header_array[0].last_signal_time = timestamp_in;
	}
	
	/*
		At this point all the values have been checked, now we need to determine if few of the
		special or regular row points need to be discurded
	*/
	
	//now we need to check if we should grow or shrink
	if((header_array[0].number_elements == header_array[0].number_reserved)&&((header_array[0].number_dead*2)/header_array[0].number_elements >= 1)){	
		memcpy(elements, elements+header_array[0].number_dead*header_array[0].row_size, 
			   (header_array[0].number_elements-header_array[0].number_dead)*header_array[0].row_size);
		
		byte_array = DATA_RAW((char*)t1); 
		header_array = (header_performance_struct_array *)byte_array;
		elements = DATA_ROW_ARRAY(t1, SIZE_HEADER_PERFORMANCE_STRUCT_ARRAY);
		
		header_array[0].number_elements -= header_array[0].number_dead;
		header_array[0].number_dead = 0;
		
	}else if(header_array[0].number_elements == header_array[0].number_reserved){
		header_array[0].number_reserved *= 2;
		
		MemoryContext aggContext = ((WindowState *) fcinfo->context)->transcontext, oldContext = NULL;
		oldContext = MemoryContextSwitchTo(aggContext);
		
		t2 = (bytea*)palloc(VARHDRSZ+header_array[0].row_size*header_array[0].number_reserved+SIZE_HEADER_PERFORMANCE_STRUCT_ARRAY);
		MemoryContextSwitchTo(oldContext);
		
		SET_VARSIZE(t2, VARHDRSZ+header_array[0].row_size*header_array[0].number_reserved+SIZE_HEADER_PERFORMANCE_STRUCT_ARRAY);
		
		//copy content from the header
		memcpy(DATA_RAW(t2), header_array, SIZE_HEADER_PERFORMANCE_STRUCT_ARRAY);
		memcpy(DATA_RAW(t2)+SIZE_HEADER_PERFORMANCE_STRUCT_ARRAY, elements+header_array[0].number_dead*header_array[0].row_size, 
			   (header_array[0].number_elements-header_array[0].number_dead)*header_array[0].row_size);
		
		t1 = t2;
		byte_array = DATA_RAW((char*)t1); 
		header_array = (header_performance_struct_array *)byte_array;
		elements = DATA_ROW_ARRAY(t1, SIZE_HEADER_PERFORMANCE_STRUCT_ARRAY);
		
		header_array[0].number_elements -= header_array[0].number_dead;
		header_array[0].number_dead = 0;
	}
	
	header_array[0].final_value += 1;
	
	for(i = 0; i < input_size-row_header_size; ++i){
		ELEM_ARR_GET_DATUM(header_array[0].number_elements, header_array[0].row_size, elements, i) = PG_GETARG_DATUM(i+2);
	}
	ELEM_ARR_GET_DATUM(header_array[0].number_elements, header_array[0].row_size, elements, i) = BoolGetDatum(TRUE);
	
	if(!trade_or_signal){
		ELEM_ARR_PRICE_DATUM(header_array[0].number_elements, header_array[0].row_size, elements) = Float8GetDatum(header_array[0].last_trade_price);
		header_array[0].number_live_signal++;
	}
	header_array[0].number_elements++;
	
	PG_RETURN_INT64((int64)(int)t1);
}

Datum performance_array_window_fast_sfunc(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(performance_array_window_fast_sfunc);
Datum performance_array_window_fast_sfunc(PG_FUNCTION_ARGS){
	int SIZE_HEADER_PERFORMANCE_STRUCT_ARRAY = sizeof(header_performance_struct_array);
	int input_size = PG_NARGS();
	int input_struct_size = 0;
	
	header_performance_struct_array *header_array = NULL;
	char *elements = NULL; 

	/*first few elements will be:
		BYTEA of the current state
		OID of sfunc
		INTERVAL of interest after a special row
		BOOL special
		TIMESTAMP of the current row
		PRICE of the current transaciton
	*/
	bytea *t2 = NULL, *t1 = NULL;
	char* byte_array = NULL;
	int64 len = 0;
	Interval *int_in;
	bool trade_or_signal = FALSE;
	int64 timestamp_in = 0;
	int row_header_size = 3;
	
	int64 i = 0;
	
	IOFuncSelector which_func = 0;
	int16 typlen = 0;
	bool typbyval = false;
	char typalign = ' ';
	char typdelim = ' ';
	Oid typioparam = 0;
	Oid func = 0;
	bool first_alloc = FALSE;
	
	/*
		This oprtion checks all the inputs and if this is the first call
		to the sfunc initial allocation happence
	*/	
	if (PG_ARGISNULL(0)){
		first_alloc = TRUE;	
	}else{
		t1 = (bytea*)(int)PG_GETARG_INT64(0);
		len = VARSIZE_ANY_EXHDR(t1);
		if(len == 0){
			first_alloc = TRUE;	
		}
	}
	
	if(first_alloc){
		for(i = row_header_size; i < input_size; ++i){
			get_type_io_data(get_fn_expr_argtype(fcinfo->flinfo, i), 
			which_func, 
			&typlen, 
			&typbyval, 
			&typalign, 
			&typdelim, 
			&typioparam, 
			&func);
			
			if(typbyval == FALSE){
				ereport(ERROR,
	            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             	errmsg("Input must be of pass by value type, and cannot be a complex type such as ARRAY or TEXT")));
			}
		}
		input_struct_size = (input_size-row_header_size+1)*sizeof(Datum);
		
		MemoryContext aggContext = ((WindowState *) fcinfo->context)->transcontext, oldContext = NULL;
		oldContext = MemoryContextSwitchTo(aggContext);
		t1 = (bytea*)palloc(VARHDRSZ+input_struct_size*INIT_STRUCT_SIZE+SIZE_HEADER_PERFORMANCE_STRUCT_ARRAY);
		MemoryContextSwitchTo(oldContext);
		
		SET_VARSIZE(t1, VARHDRSZ+input_struct_size*INIT_STRUCT_SIZE+SIZE_HEADER_PERFORMANCE_STRUCT_ARRAY);
		header_array = DATA_HEADER(t1);
		header_array[0].row_size = input_struct_size;
		if(PG_ARGISNULL(1)){
			ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("OID of the internal functions cannot be NULL")));
		}
		header_array[0].sfunc = (Oid)PG_GETARG_INT32(1);
		
		if(PG_ARGISNULL(2)){
			ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("Interval for internal aggregation cannot be NULL")));
		}
		
		int_in = PG_GETARG_INTERVAL_P(2);
		header_array[0].time_interval_to_preserve = int_in->time + int_in->month*INT64CONST(30)*USECS_PER_DAY + int_in->day*INT64CONST(24) * USECS_PER_HOUR;
		if (header_array[0].time_interval_to_preserve <= 0)
		{
			ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("interval value parameter is not a valid interval")));
		}
		
		header_array[0].number_elements = 0;
		header_array[0].number_reserved = INIT_STRUCT_SIZE;
		header_array[0].number_dead = 0;
		header_array[0].final_value = 0;
		header_array[0].last_trade_price = 0.0;
		header_array[0].last_signal_time = 0.0;
		header_array[0].number_live_signal = 0;
		if(PG_ARGISNULL(4)){
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("Timeseries based analysis cannot have NULL for timestamp value")));
		}
		header_array[0].last_cleanup = PG_GETARG_TIMESTAMP(4);
	}else{
		header_array = DATA_HEADER(t1);
	}
	
	if(PG_ARGISNULL(3)){
		ereport(ERROR,
        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
        errmsg("Boolean indicator special/regular row cannot be NULL")));
	}
	trade_or_signal = PG_GETARG_BOOL(3);
	
	if(PG_ARGISNULL(4)){
		ereport(ERROR,
        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
        errmsg("Timeseries based analysis cannot have NULL for timestamp value")));
	}
	timestamp_in = PG_GETARG_TIMESTAMP(4);
	header_array[0].last_signal_time = timestamp_in;
	elements = DATA_ROW_ARRAY(t1, SIZE_HEADER_PERFORMANCE_STRUCT_ARRAY);
             
	if(PG_ARGISNULL(5)&&(trade_or_signal)){
		PG_RETURN_BYTEA_P(t1);
	}
	
	if(trade_or_signal){
		header_array[0].last_trade_price = PG_GETARG_FLOAT8(5);
	}
	
	if(!trade_or_signal){
		header_array[0].last_signal_time = timestamp_in;
	}
	
	/*
		At this point all the values have been checked, now we need to determine if few of the
		special or regular row points need to be discurded
	*/
	
	//now we need to check if we should grow or shrink
	if((header_array[0].number_elements == header_array[0].number_reserved)&&((header_array[0].number_dead*2)/header_array[0].number_elements >= 1)){	
		memcpy(elements, elements+header_array[0].number_dead*header_array[0].row_size, 
			   (header_array[0].number_elements-header_array[0].number_dead)*header_array[0].row_size);
		
		byte_array = DATA_RAW((char*)t1); 
		header_array = (header_performance_struct_array *)byte_array;
		elements = DATA_ROW_ARRAY(t1, SIZE_HEADER_PERFORMANCE_STRUCT_ARRAY);
		
		header_array[0].number_elements -= header_array[0].number_dead;
		header_array[0].number_dead = 0;
		
	}else if(header_array[0].number_elements == header_array[0].number_reserved){
		header_array[0].number_reserved *= 2;
		
		MemoryContext aggContext = ((WindowState *) fcinfo->context)->transcontext, oldContext = NULL;
		oldContext = MemoryContextSwitchTo(aggContext);
		
		t2 = (bytea*)palloc(VARHDRSZ+header_array[0].row_size*header_array[0].number_reserved+SIZE_HEADER_PERFORMANCE_STRUCT_ARRAY);
		MemoryContextSwitchTo(oldContext);
		
		SET_VARSIZE(t2, VARHDRSZ+header_array[0].row_size*header_array[0].number_reserved+SIZE_HEADER_PERFORMANCE_STRUCT_ARRAY);
		
		//copy content from the header
		memcpy(DATA_RAW(t2), header_array, SIZE_HEADER_PERFORMANCE_STRUCT_ARRAY);
		memcpy(DATA_RAW(t2)+SIZE_HEADER_PERFORMANCE_STRUCT_ARRAY, elements+header_array[0].number_dead*header_array[0].row_size, 
			   (header_array[0].number_elements-header_array[0].number_dead)*header_array[0].row_size);
		
		t1 = t2;
		byte_array = DATA_RAW((char*)t1); 
		header_array = (header_performance_struct_array *)byte_array;
		elements = DATA_ROW_ARRAY(t1, SIZE_HEADER_PERFORMANCE_STRUCT_ARRAY);
		
		header_array[0].number_elements -= header_array[0].number_dead;
		header_array[0].number_dead = 0;
	}
	
	header_array[0].final_value += 1;
	
	for(i = 0; i < input_size-row_header_size; ++i){
		ELEM_ARR_GET_DATUM(header_array[0].number_elements, header_array[0].row_size, elements, i) = PG_GETARG_DATUM(i+3);
	}
	ELEM_ARR_GET_DATUM(header_array[0].number_elements, header_array[0].row_size, elements, i) = BoolGetDatum(TRUE);
	
	if(!trade_or_signal){
		ELEM_ARR_PRICE_DATUM(header_array[0].number_elements, header_array[0].row_size, elements) = Float8GetDatum(header_array[0].last_trade_price);
		header_array[0].number_live_signal++;
	}
	header_array[0].number_elements++;
	
	PG_RETURN_INT64((int64)(int32)t1);
}

Datum performance_array_window_prefunc(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(performance_array_window_prefunc);
Datum performance_array_window_prefunc(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(PG_GETARG_DATUM(0));
}

Datum performance_array_window_finalfunc(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(performance_array_window_finalfunc);
Datum performance_array_window_finalfunc(PG_FUNCTION_ARGS)
{
	
	int SIZE_HEADER_PERFORMANCE_STRUCT_ARRAY = sizeof(header_performance_struct_array), i = 0;
	header_performance_struct_array *header_array = NULL;
	char *elements = NULL; 

	/*first few elements will be:
		BYTEA of the current state
		OID of sfunc
		INTERVAL of interest after a special row
		BOOL special
		TIMESTAMP of the current row
		PRICE of the current transaciton
	*/
	bytea *t1 = NULL;
	int64 len = 0, last_signal_time = -1, last_sginal = -1;
	
	/*
		This oprtion checks all the inputs and if this is the first call
		to the sfunc initial allocation happence
	*/
	
	if (PG_ARGISNULL(0)){
		PG_RETURN_NULL();
	}else{
		t1 = (bytea*)(int32)PG_GETARG_INT64(0);
		len = VARSIZE_ANY_EXHDR(t1);
		if(len == 0){
			PG_RETURN_NULL();	
		}
	}
	
	header_array = DATA_HEADER(t1);
	elements = DATA_ROW_ARRAY(t1, SIZE_HEADER_PERFORMANCE_STRUCT_ARRAY);
	last_signal_time = ELEM_ARR_TIMESTAMP((header_array[0].number_elements-1),header_array[0].row_size,elements);
	
	if((header_array[0].number_live_signal > 0)||(ELEM_ARR_TIMESTAMP(header_array[0].number_elements-1,header_array[0].row_size,elements) - header_array[0].last_cleanup > 2*header_array[0].time_interval_to_preserve)){
		header_array[0].last_cleanup = ELEM_ARR_TIMESTAMP(header_array[0].number_elements-1,header_array[0].row_size,elements);
		for(i = header_array[0].number_dead; i < header_array[0].number_elements; ++i){
			if((!ELEM_ARR_TRADE(i,header_array[0].row_size,elements))&&(ELEM_ARR_STATUS(i,header_array[0].row_size,elements))){
				last_signal_time = ELEM_ARR_TIMESTAMP(i,header_array[0].row_size,elements);
				last_sginal = i;
				break;
			}
		}
		for(i = header_array[0].number_dead; i < header_array[0].number_elements; ++i){
			if((ELEM_ARR_TIMESTAMP(i,header_array[0].row_size,elements)-last_signal_time) > header_array[0].time_interval_to_preserve){
				header_array[0].number_dead++;
			}else{
				break;
			}
		}
        
		TypeFuncClass funcClass;
		Oid resultType;
		TupleDesc resultDesc;
		Datum resultDatum[3];
		bool isnull[3];
		float8 *value_array;
		bool *trade_array;
		int64 *time_array;
		ArrayType *pgarray1 = NULL, *pgarray2 = NULL, *pgarray3 = NULL;
		int64 j = 0;
	
		if(last_sginal >= 0){
			header_array[0].number_live_signal--;
			ELEM_ARR_STATUS_DATUM(last_sginal,header_array[0].row_size,elements) = BoolGetDatum(FALSE);
			value_array = (float8 *)palloc(sizeof(float8)*(header_array[0].number_elements-header_array[0].number_dead));
			time_array = (int64 *)palloc(sizeof(int64)*(header_array[0].number_elements-header_array[0].number_dead));
			trade_array = (bool *)palloc(sizeof(bool)*(header_array[0].number_elements-header_array[0].number_dead));
		
			for(j = 0, i = header_array[0].number_elements-1; i >= header_array[0].number_dead; --i, ++j){
				value_array[j] = ELEM_ARR_PRICE(i,header_array[0].row_size,elements);
				time_array[j] = ELEM_ARR_TIMESTAMP(i,header_array[0].row_size,elements);
				trade_array[j] = ELEM_ARR_TRADE(i,header_array[0].row_size,elements);	
			}
	
			pgarray1 = construct_array((Datum *)value_array,
							  (header_array[0].number_elements-header_array[0].number_dead),FLOAT8OID,
							  sizeof(Datum),true,'d');
			pgarray2 = construct_array((Datum *)trade_array,
							  (header_array[0].number_elements-header_array[0].number_dead),BOOLOID,
							  sizeof(Datum),true,'d');
			pgarray3 = construct_array((Datum *)time_array,
							  (header_array[0].number_elements-header_array[0].number_dead),TIMESTAMPOID,
							  sizeof(Datum),true,'d');
			memset(isnull, 0, sizeof(isnull));
			funcClass = get_call_result_type(fcinfo, &resultType, &resultDesc);
						
			BlessTupleDesc(resultDesc);
			StartCopyToHeapTuple(resultDatum, isnull);
			CopyToHeapTuple(pgarray1, PointerGetDatum);
    		CopyToHeapTuple(pgarray2, PointerGetDatum);
    		CopyToHeapTuple(pgarray3, PointerGetDatum);
			EndCopyToDatum;
	
			PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(resultDesc, resultDatum, isnull)));
		}
	}
	
	PG_RETURN_NULL();
}

Datum performance_array_window_fast_finalfunc(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(performance_array_window_fast_finalfunc);
Datum performance_array_window_fast_finalfunc(PG_FUNCTION_ARGS)
{
	
	int SIZE_HEADER_PERFORMANCE_STRUCT_ARRAY = sizeof(header_performance_struct_array), i = 0;
	header_performance_struct_array *header_array = NULL;
	char *elements = NULL; 

	/*first few elements will be:
		BYTEA of the current state
		OID of sfunc
		INTERVAL of interest after a special row
		BOOL special
		TIMESTAMP of the current row
		PRICE of the current transaciton
	*/
	bytea *t1 = NULL, *t2 = NULL;
	int64 len = 0, last_signal_time = -1, last_sginal = -1;
	Datum result;
	
	
	//PG_RETURN_NULL();
	/*
		This oprtion checks all the inputs and if this is the first call
		to the sfunc initial allocation happence
	*/
	
	if (PG_ARGISNULL(0)){
		PG_RETURN_NULL();
	}else{
		t1 = (bytea*)(int)PG_GETARG_INT64(0);
		len = VARSIZE_ANY_EXHDR(t1);
		if(len == 0){
			PG_RETURN_NULL();	
		}
	}
	
	header_array = DATA_HEADER(t1);
	elements = DATA_ROW_ARRAY(t1, SIZE_HEADER_PERFORMANCE_STRUCT_ARRAY);
	last_signal_time = ELEM_ARR_TIMESTAMP((header_array[0].number_elements-1),header_array[0].row_size,elements);
	
	if((header_array[0].number_live_signal > 0)||(ELEM_ARR_TIMESTAMP(header_array[0].number_elements-1,header_array[0].row_size,elements) - header_array[0].last_cleanup > 2*header_array[0].time_interval_to_preserve)){
		header_array[0].last_cleanup = ELEM_ARR_TIMESTAMP(header_array[0].number_elements-1,header_array[0].row_size,elements);
		for(i = header_array[0].number_dead; i < header_array[0].number_elements; ++i){
			if((!ELEM_ARR_TRADE(i,header_array[0].row_size,elements))&&(ELEM_ARR_STATUS(i,header_array[0].row_size,elements))){
				last_signal_time = ELEM_ARR_TIMESTAMP(i,header_array[0].row_size,elements);
				last_sginal = i;
				break;
			}
		}
		for(i = header_array[0].number_dead; i < header_array[0].number_elements; ++i){
			if((ELEM_ARR_TIMESTAMP(i,header_array[0].row_size,elements)-last_signal_time) > header_array[0].time_interval_to_preserve){
				header_array[0].number_dead++;
			}else{
				break;
			}
		}
	
		if(last_sginal >= 0){
			header_array[0].number_live_signal--;
			ELEM_ARR_STATUS_DATUM(last_sginal,header_array[0].row_size,elements) = BoolGetDatum(FALSE);
		
			t2 = (bytea*)palloc(VARHDRSZ+header_array[0].row_size*(header_array[0].number_elements-header_array[0].number_dead));
			SET_VARSIZE(t2,VARHDRSZ+header_array[0].row_size*(header_array[0].number_elements-header_array[0].number_dead));
			memcpy(DATA_RAW(t2),ELEM_ARR(header_array[0].number_dead,header_array[0].row_size,elements),header_array[0].row_size*(header_array[0].number_elements-header_array[0].number_dead));
		
			result = OidFunctionCall2(header_array[0].sfunc, PointerGetDatum(t2), Int64GetDatum(header_array[0].row_size));
		
			pfree(t2);
			return result;
		}
	}
	
	PG_RETURN_NULL();
}
