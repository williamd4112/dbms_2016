#pragma once

#include "database_type.h"

#include "IndexFile.h"

/*
	table_attr_desc_t

	name: attribute name
	offset: attribute offset of record
	size: size of attribute
*/
struct table_attr_desc_t
{
	char name[ATTR_NAME_MAX];
	unsigned char type;
	unsigned int offset;
	unsigned int size;
	unsigned char constraint;
};

/*
	table_header_t

	store table name, number of attribute
*/
struct table_header_t
{
	char name[TABLE_NAME_MAX];
	unsigned int attrNum;
	unsigned int rowsize;
	int primaryKeyIndex;
};

/*
	table_index_record_t

	store attr_name, idx_filepath, idx_type
*/
struct table_index_record_t
{
	char attr_name[ATTR_NAME_MAX];
	char index_name[INDEX_FILENAME_MAX];
	IndexType index_type;

	table_index_record_t() {}
	table_index_record_t(std::string _attr_name, std::string _index_name, IndexType _index_type)
	{
		strncpy(attr_name, _attr_name.c_str(), ATTR_NAME_MAX);
		strncpy(index_name, _index_name.c_str(), INDEX_FILENAME_MAX);
		index_type = _index_type;
	}
};