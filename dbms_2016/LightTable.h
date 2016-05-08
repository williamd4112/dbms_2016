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
#define UNSUPPORT_MERGE_TYPE 0x8

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

	// Cross join
	static std::pair<LightTable *, LightTable *> join_cross(
		LightTable & a,
		std::string a_keyname,
		relation_type_t rel_type,
		LightTable & b,
		std::string b_keyname,
		std::vector<AddrPair> &match_pairs);

	// Self join (Generate a reflexive pair)
	static std::pair<LightTable *, LightTable *> join_self(
		LightTable & table, 
		std::string key1, 
		relation_type_t rel_type, 
		std::string key2, 
		std::vector<AddrPair> &match_pairs);

	static std::pair<LightTable *, LightTable *> join_self(
		LightTable & table,
		std::string key,
		relation_type_t rel_type,
		attr_t & kAttr,
		std::vector<AddrPair> &match_pairs);

	static std::pair<LightTable *, LightTable *> merge(
		std::pair<LightTable *, LightTable *> a_comb,
		std::vector<AddrPair>& a,
		merge_type_t merge_type,
		std::pair<LightTable *, LightTable *> b_comb,
		std::vector<AddrPair>& b,
		std::vector<AddrPair>& c);

	static inline void map(
		std::vector<uint32_t> & addrs,
		LightTable * onto_table,
		std::vector<AddrPair> & match_pairs
	);

	static inline void product(
		LightTable *a, 
		LightTable *b, 
		std::vector<AddrPair> & match_pairs);

	static std::vector<AddrPair> product(
		std::vector<AddrPair> & reflexive_pairs, 
		LightTable * b);

	void create(const char *tablename, AttrDesc *descs, int num);
	void create_index(const char *attr_name, IndexType type);

	void load(const char *tablename);
	void save();

	void insert(AttrTuple &tuple);

	uint32_t filter(
		const char *attr_name, 
		attr_t & attr, 
		relation_type_t find_type, 
		std::vector<uint32_t> & match_addrs);
	
	AttrTuple &get_tuple(uint32_t index);
	int get_attr_id(std::string attr_name);
	bool has_attr(std::string attr_name);
	uint32_t size();
	uint32_t tuple_size();
	std::string name() { return mTablename; }

	void dump();
	static void dump(LightTable & a, LightTable & b, std::vector<AddrPair> & match_pairs);

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
	
	uint32_t filter_with_index(
		const char *attr_name, 
		attr_t & attr, 
		relation_type_t find_type, 
		IndexFile *index_file,
		std::vector<uint32_t> & match_addrs);

	uint32_t filter_naive(
		const char *attr_name, 
		attr_t & attr,
		relation_type_t find_type, 
		std::vector<uint32_t> & match_addrs);
	
	inline IndexFile *get_index_file(const char *name);
	inline void init_seq_types(AttrDesc *descs, int num);
	void get_selectid_from_names(std::vector<std::string> &names, std::vector<int> &ids);

	static void cross_naive_join(
		LightTable & a,
		std::string a_keyname,
		relation_type_t rel_type,
		LightTable & b,
		std::string b_keyname,
		std::vector<AddrPair> &match_pairs);

	static inline void cross_hash_join(
		LightTable & a,
		std::string a_keyname,
		IndexFile *a_index,
		relation_type_t rel_type,
		LightTable & b,
		std::string b_keyname,
		IndexFile *b_index,
		std::vector<AddrPair> &match_pairs);

	static inline void LightTable::cross_hash_join_eq(
		LightTable & iter_table,
		int iter_key_id,
		LightTable & fix_table,
		IndexFile * fix_index,
		std::vector<AddrPair>& match_pairs);

	static inline void LightTable::cross_hash_join_neq(
		LightTable & iter_table,
		int iter_key_id,
		LightTable & fix_table,
		IndexFile * fix_index,
		std::vector<AddrPair>& match_pairs);

	static inline void cross_two_tree_join(
		LightTable & a,
		std::string a_keyname,
		IndexFile *a_index,
		
		relation_type_t rel_type,
		
		LightTable & b,
		std::string b_keyname,
		IndexFile *b_index,
		
		std::vector<AddrPair> &match_pairs);

	static inline void cross_one_tree_join(
		LightTable & a,
		std::string a_keyname,
		IndexFile *a_index,

		relation_type_t rel_type,
		
		LightTable & b,
		std::string b_keyname,
		IndexFile *b_index,
		
		std::vector<AddrPair> &match_pairs);

	static void merge(
		std::vector<AddrPair> &a,
		merge_type_t merge_type,
		std::vector<AddrPair> &b,
		std::vector<AddrPair> & c);
};

