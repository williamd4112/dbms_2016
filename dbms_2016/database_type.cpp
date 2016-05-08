#include "database_type.h"

const char *kAttrTypeNames[] = { "NULL", "INTEGER", "VARCHAR" };

unary_op_type_t operator -(const unary_op_type_t u)
{
	return (u == POS) ? NEG : POS;
}

unary_op_type_t operator *(const unary_op_type_t a, const unary_op_type_t b)
{
	return (a != b) ? NEG : POS;
}

inline c_unique::c_unique() { current = 0; }

inline int c_unique::operator()() { return ++current; }