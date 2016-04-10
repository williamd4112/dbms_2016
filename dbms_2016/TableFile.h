#pragma once

#include "DiskFile.h"
#include <unordered_map>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <cassert>

#define ATTR_NAME_MAX 40
#define TABLE_NAME_MAX ATTR_NAME_MAX

/*
	table_attr_desc_t

	name: attribute name
	offset: attribute offset of record
	size: size of attribute
*/
struct table_attr_desc_t
{
	char name[ATTR_NAME_MAX];
	unsigned int offset;
	unsigned int size;
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

	inline const table_header_t& get_table_header();
	inline const table_attr_desc_t *get_attr_desc(const char *);
	inline const table_attr_desc_t *get_attr_desc(unsigned int);
	
	const unsigned int get_row_size() const;

	void init(const char *, unsigned int, table_attr_desc_t *);
	inline void write_back();
	inline void read_from();

	void dump_info();

private:
	table_header_t mHeader;
	table_attr_desc_t *mAttrDescs;

	AttrDictionary mAttrHashIndex;

	inline void build_index();
};

