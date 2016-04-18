#include "database_util.h"
#include "database_util.h"
#include <cassert>

int db::parse_int(unsigned char * const record, const table_attr_desc_t &desc)
{
	// Check type
	if (desc.type != ATTR_TYPE_INTEGER)
		throw TYPE_CAST_ERROR;

	return *(reinterpret_cast<int*>(&record[desc.offset]));
}
