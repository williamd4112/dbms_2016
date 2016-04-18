#pragma once

#include "RecordTable.h"

namespace db
{
	enum DatabaseUtilException
	{
		TYPE_CAST_ERROR	
	};

	int parse_int(unsigned char * const record, const table_attr_desc_t &desc);
}