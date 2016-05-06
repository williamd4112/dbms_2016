#pragma once

#include "database_type.h"
#include "database_util.h"

#include "LightTableFile.h"
#include "SequenceFile.h"
#include "IndexFile.h"

#define ATTR_TYPE_TO_SEQ_TYPE_ERROR 0x1
#define INSERT_DUPLICATE_TUPLE 0x2
#define JOIN_UNKNOWN_RELATION_TYPE 0x3
#define FIND_NOT_FOUND 0x4
#define UNKNOWN_ATTR 0x5
#define UNKNOWN_RELATION 0x6
#define UNSUPPORT_RELATION 0x7

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

	static void join(
		LightTable &a,
		std::string a_keyname,
		relation_type_t rel_type,
		LightTable &b,
		std::string b_keyname,
		std::vector<AddrPair> &match_pairs);

	static void join_naive(
		LightTable &a,
		std::string a_keyname,
		relation_type_t rel_type,
		LightTable &b,
		std::string b_keyname,
		std::vector<AddrPair> &match_pairs);

	static void select(
		LightTable &a,
		std::vector<std::string> &a_select_attr_names,
		LightTable &b,
		std::vector<std::string> &b_select_attr_names,
		std::vector<AddrPair> &match_pairs
		);

	void create(const char *tablename, AttrDesc *descs, int num);
	void load(const char *tablename);
	void save();

	void create_index(const char *attr_name, IndexType type);
	void insert(AttrTuple &tuple);
	uint32_t find(const char *attr_name, attr_t & attr, relation_type_t find_type, std::vector<uint32_t> & match_addrs);
	
	AttrTuple &get_tuple(uint32_t index);
	int get_attr_id(std::string attr_name);
	uint32_t size();

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
	uint32_t find_with_index(const char *attr_name, attr_t & attr, relation_type_t find_type, IndexFile *index_file, std::vector<uint32_t> & match_addrs);
	inline IndexFile *get_index_file(const char *name);
	inline void init_seq_types(AttrDesc *descs, int num);
	void get_selectid_from_names(std::vector<std::string> &names, std::vector<int> &ids);

	static inline void join_hash(
		LightTable &a,
		std::string a_keyname,
		IndexFile *a_index,
		relation_type_t rel_type,
		LightTable &b,
		std::string b_keyname,
		IndexFile *b_index,
		std::vector<AddrPair> &match_pairs);
	
	static inline void hashjoin(
		LightTable *iter_table,
		int iter_key_id,
		LightTable *fix_table,
		IndexFile *fix_index,
		std::vector<AddrPair> &match_pairs);

	static inline void hashjoin_not(
		LightTable *iter_table,
		int iter_key_id,
		LightTable *fix_table,
		IndexFile *fix_index,
		std::vector<AddrPair> &match_pairs);

	static inline void join_merge(
		LightTable &a,
		std::string a_keyname,
		IndexFile *a_index,
		relation_type_t rel_type,
		LightTable &b,
		std::string b_keyname,
		IndexFile *b_index,
		std::vector<AddrPair> &match_pairs);
};

