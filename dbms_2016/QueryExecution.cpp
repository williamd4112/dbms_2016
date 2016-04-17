#include "QueryExecution.h"

QueryException::QueryException(QueryExceptionType _type, const char * _msg)
	: type(_type), msg(_msg)
{
}

QueryException::QueryException(QueryExceptionType _type)
	: type(_type), msg("")
{
}

QueryException::~QueryException()
{
}
