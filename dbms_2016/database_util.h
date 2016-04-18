#pragma once

#include "RecordTable.h"

namespace db
{
	enum DatabaseUtilException
	{
		TYPE_CAST_ERROR,
		UNRESOLVED_ATTR_TYPE
	};

	int parse_int(unsigned char * const record, const table_attr_desc_t &desc);
	char *parse_varchar(unsigned char * const record, const table_attr_desc_t &desc);
	void print_record(const table_attr_desc_t * pDesc, const unsigned char * pRecord);
}