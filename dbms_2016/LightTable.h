#pragma once

#include "database_type.h"
#include "database_util.h"

#include "LightTableFile.h"
#include "SequenceFile.h"
#include "IndexFile.h"

#define ATTR_TYPE_TO_SEQ_TYPE_ERROR 0x1
#define INSERT_DUPLICATE_TUPLE 0x2

typedef std::vector<attr_t> AttrTuple;
typedef typename std::vector<AttrTuple>::iterator AttrTupleIterator;

std::ostream &operator <<(std::ostream &os, const AttrTuple tuple);

/*
	LightTable

	designed for working with LightTableFile, SequenceFile
*/
class LightTable
{
public:
	LightTable();
	~LightTable();

	void create(const char *tablename, AttrDesc *descs, int num);
	void load(const char *tablename);
	void save();

	void create_index(const char *attr_name, IndexType type);
	void insert(AttrTuple &tuple);
	uint32_t find(const char *attr_name, attr_t & attr, FindType find_type, std::vector<uint32_t> & match_addrs);

	AttrTuple &get_tuple(uint32_t index);

	void dump();

	AttrTupleIterator begin();
	AttrTupleIterator end();
private:
	std::string mTablename;
	LightTableFile mTablefile;
	std::vector<SequenceElementType> mSeqTypes;
	SequenceFile<attr_t> mDatafile;

	inline uint32_t insert_with_pk(AttrTuple &tuple);
	inline uint32_t insert_no_pk(AttrTuple &tuple);
	inline void update_index(AttrTuple &tuple, uint32_t addr);
	uint32_t find_with_index(const char *attr_name, attr_t & attr, FindType find_type, IndexFile *index_file, std::vector<uint32_t> & match_addrs);
	inline IndexFile *get_index_file(const char *name);
	inline void init_seq_types(AttrDesc *descs, int num);

};

