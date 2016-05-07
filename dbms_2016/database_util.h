#pragma once

#include "RecordTable.h"
#include "SQLParser.h"

#define INT_OUTPUT_WIDTH 20
#define LONG_LONG_OUTPUT_WIDTH 28
#define ERROR_COLDEF_ATTR_CONVERT 0x88

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
	uint8_t type_coldef_to_attr(sql::ColumnDefinition::DataType coldef_type);
}