#pragma once

#include "DiskFile.h"
#include "IndexFile.h"
#include "sql/CreateStatement.h"

#include <unordered_map>
#include <vector>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <cassert>

#include "database_type.h"
#include "database_table_type.h"

#define TABLEFILE_EXCEPTION_TOO_MANY_PK -1
#define TABLEFILE_ERROR_DUPLICATE_INDEX 0x2
#define TABLEFILE_NO_ERROR 0x0

// Support up to 2 type of index (hash, tree)
#define INDEX_NUM 2 
#define INDEX_HASH_POS 0
#define INDEX_TREE_POS 1


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

	table_index_desc_t();
	~table_index_desc_t();
};

/*
	table_exception_t

	store table exception data
*/
struct table_exception_t
{
	std::string msg;

	table_exception_t(const char *_msg) : msg(_msg) {}
	table_exception_t(){}
};

typedef std::pair<table_attr_desc_t*, int> AttrRecord;
typedef std::unordered_map<std::string, AttrRecord> AttrDictionary;

/*
	TableFile

	store table header, table attribute descriptions

	in memory, this object construct a hashing structure to retrive the attribute property
*/
class TableFile
	: public DiskFile
{
public:
	TableFile();
	~TableFile();

	const table_header_t& get_table_header() const;
	table_attr_desc_t *get_attr_desc(const char *) const;
	table_attr_desc_t *get_attr_desc(unsigned int) const;
	const table_attr_desc_t *get_attr_descs();
	const bool get_attr_descs(const char **, unsigned int, table_attr_desc_t **);
	IndexFile * get_index(const char *index_name, IndexType type);
	
	const unsigned int get_row_size() const;

	void init(const char *, unsigned int, table_attr_desc_t *, int);
	void init(const char *, unsigned int, table_attr_desc_t *);
	void init(const char *, std::vector<sql::ColumnDefinition*> &);
	uint8_t init_index(const char *attr_name, const char *index_filename, IndexType index_type);
	void update_index(void *src, uint32_t addr);
	inline void write_back();
	inline void read_from();

	void dump_info();

private:
	table_header_t mHeader;
	table_attr_desc_t *mAttrDescs;
	table_index_desc_t *mIndexDescs;

	AttrDictionary mAttrLookupTable;

	inline void build_lookup_table();
	inline void build_primary_key_index(const char *name);
	inline IndexFile * allocate_index_file(const table_attr_desc_t *desc, IndexType type);
};

