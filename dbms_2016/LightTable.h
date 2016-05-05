#pragma once

#include "database_type.h"
#include "database_util.h"

#include "LightTableFile.h"
#include "SequenceFile.h"
#include "IndexFile.h"

#define ATTR_TYPE_TO_SEQ_TYPE_ERROR 0x1

typedef std::vector<attr_t> AttrTuple;
typedef typename std::vector<AttrTuple>::iterator AttrTupleIterator;

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
	void insert(AttrTuple &tuple);

	void dump();

	AttrTupleIterator begin();
	AttrTupleIterator end();
private:
	std::string mTablename;
	LightTableFile mTablefile;
	std::vector<SequenceElementType> mSeqTypes;
	SequenceFile<attr_t> mDatafile;

	inline void init_seq_types(AttrDesc *descs, int num);

};

