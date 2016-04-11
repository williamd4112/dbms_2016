#pragma once

#include "DiskFile.h"
#include <unordered_map>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <cassert>

#define ATTR_NUM_MAX 5
#define ATTR_NAME_MAX 41
#define TABLE_NAME_MAX ATTR_NAME_MAX
#define ATTR_SIZE_MAX ATTR_NAME_MAX

#define ATTR_TYPE_UNDEFINED 0x0
#define ATTR_TYPE_INTEGER 0x1
#define ATTR_TYPE_VARCHAR 0x2

#define ATTR_CONSTRAINT_NO 0x0
#define ATTR_CONSTRAINT_PRIMARY_KEY 0x1

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
	TableFile

	store table header, table attribute descriptions

	in memory, this object construct a hashing structure to retrive the attribute property
*/
class TableFile
	: public DiskFile
{
	typedef std::unordered_map<std::string, table_attr_desc_t*> AttrDictionary;
public:
	TableFile();
	~TableFile();

	const table_header_t& get_table_header() const;
	table_attr_desc_t *get_attr_desc(const char *);
	table_attr_desc_t *get_attr_desc(unsigned int);
	const table_attr_desc_t *get_attr_descs();
	const bool get_attr_descs(const char **, unsigned int, table_attr_desc_t **);
	
	const unsigned int get_row_size() const;

	void init(const char *, unsigned int, table_attr_desc_t *, int);
	inline void write_back();
	inline void read_from();

	void dump_info();

private:
	table_header_t mHeader;
	table_attr_desc_t *mAttrDescs;

	AttrDictionary mAttrHashIndex;

	inline void build_index();
};

