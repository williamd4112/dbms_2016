#pragma once

#include "RecordTable.h"

#define INT_OUTPUT_WIDTH 20
#define LONG_LONG_OUTPUT_WIDTH 28

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