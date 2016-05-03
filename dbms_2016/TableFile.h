#pragma once

#include "DiskFile.h"
#include "sql/CreateStatement.h"

#include <unordered_map>
#include <vector>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <cassert>

#include "database_type.h"

#define TABLEFILE_EXCEPTION_TOO_MANY_PK -1

// Support up to 2 type of index (hash, tree)
#define INDEX_NUM 2 
#define INDEX_HASH_POS 0
#define INDEX_TREE_POS 1

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
	table_index_desc_t
	
	store index file pointer, only (tree, hash) or (hash, tree), no (phash, hash)
*/
struct table_index_desc_t
{
	struct 
	{
		std::string index_name;
		IndexFile *index_file;
	} indices[INDEX_NUM];
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
	table_attr_desc_t *get_attr_desc(const char *) const;
	table_attr_desc_t *get_attr_desc(unsigned int) const;
	const table_attr_desc_t *get_attr_descs();
	const bool get_attr_descs(const char **, unsigned int, table_attr_desc_t **);
	
	const unsigned int get_row_size() const;

	void init(const char *, unsigned int, table_attr_desc_t *, int);
	void init(const char *, unsigned int, table_attr_desc_t *);
	void init(const char *, std::vector<sql::ColumnDefinition*> &);
	inline void write_back();
	inline void read_from();

	void dump_info();

private:
	table_header_t mHeader;
	table_attr_desc_t *mAttrDescs;
	table_index_desc_t *mIndexDescs;

	AttrDictionary mAttrLookupTable;

	inline void build_lookup_table();
};

