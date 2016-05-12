#include "LightTable.h"

#include <algorithm>
#include <functional>

#define BIT_HAS_HASH 0x1
#define BIT_HAS_TREE 0x2

#define TABLE_COMB_ERROR 0x45

std::ostream &operator <<(std::ostream &os, const AttrTuple tuple)
{
	for (auto it = tuple.begin(); it != tuple.end(); it++)
		os << *it << "\t";
	return os;
}

bool operator <(const AddrPair & p1, const AddrPair & p2)
{
	if (p1.first == p2.first)
		return p1.second < p2.second;
	else
		return p1.first < p2.first;
}

LightTable::LightTable()
{
}


LightTable::~LightTable()
{
}

/*
	static LightTable::join()

	join two tables, and store addr pair vector in return
	support three type of join operations:
	1. Hash join (at least one hashindex)
	2. Merge join (require two treeindex)
	3. Naive join (wrost case, nested loop)
*/
std::pair<LightTable *, LightTable *> LightTable::join_cross(
	LightTable & a, 
	std::string a_keyname, 
	relation_type_t rel_type, 
	LightTable & b,
	std::string b_keyname,
	std::vector<AddrPair> &match_pairs)
{
	// Join operation selection
	uint8_t a_stat = 0x0;
	uint8_t b_stat = 0x0;

	IndexFile *a_index_file = a.get_index_file(a_keyname.c_str());
	if (a_index_file != NULL)
	{
		IndexType type = a_index_file->type();
		a_stat = (type == HASH || type == PHASH) ? BIT_HAS_HASH :
			(type == TREE || type == PTREE) ? BIT_HAS_TREE : 0;
	}

	IndexFile *b_index_file = b.get_index_file(b_keyname.c_str());
	if (b_index_file != NULL)
	{
		IndexType type = b_index_file->type();
		b_stat = (type == HASH || type == PHASH) ? BIT_HAS_HASH :
			(type == TREE || type == PTREE) ? BIT_HAS_TREE : 0;
	}

	// According to relation type, choose best approach
	switch (rel_type)
	{
	case EQ: case NEQ:
		// 1. Hash join (always use a as iter_table, b as index_table)
		// 2. Tree join
		// 3. Naive
		if ((b_stat & BIT_HAS_HASH))
			cross_hash_join(a, a_keyname, a_index_file, 
				rel_type, 
				b, b_keyname, b_index_file, match_pairs);
		else if ((a_stat & BIT_HAS_TREE) && (b_stat & BIT_HAS_TREE))
			cross_two_tree_join(a, a_keyname, a_index_file,
				rel_type,
				b, b_keyname, b_index_file, match_pairs);
		else
			cross_naive_join(a, a_keyname,
				rel_type,
				b, b_keyname,
				match_pairs);
		break;
	case LESS: case LARGE:
		// 1. Two Tree
		// 2. b has tree
		// 3. Naive
		if ((a_stat & BIT_HAS_TREE) && (b_stat & BIT_HAS_TREE))
			cross_two_tree_join(a, a_keyname, a_index_file,
				rel_type,
				b, b_keyname, b_index_file, match_pairs);
		else if ((b_stat & BIT_HAS_TREE))
			cross_one_tree_join(a, a_keyname, a_index_file,
				rel_type,
				b, b_keyname, b_index_file, match_pairs);
		else
			cross_naive_join(a, a_keyname,
				rel_type,
				b, b_keyname,
				match_pairs);
		break;
	default:
		throw exception_t(UNKNOWN_RELATION, "Unknown relation type.");
	}
	return std::pair<LightTable *, LightTable *>(&a, &b);
}

std::pair<LightTable *, LightTable *> LightTable::join_self(
	LightTable & table, 
	std::string key1, 
	relation_type_t rel_type,
	std::string key2, 
	std::vector<AddrPair>& match_pairs)
{
	int id1 = table.get_attr_id(key1);
	int id2 = table.get_attr_id(key2);

	switch (rel_type)
	{
	case EQ:
	{
		for (auto it = table.begin(); it != table.end(); it++)
		{
			uint32_t addr = it - table.begin();
			if (it->at(id1) == it->at(id2))
				match_pairs.emplace_back(addr, addr);
		}
		break;
	}
	case NEQ:
	{
		for (auto it = table.begin(); it != table.end(); it++)
		{
			uint32_t addr = it - table.begin();
			if (it->at(id1) != it->at(id2))
				match_pairs.emplace_back(addr, addr);
		}
		break;
	}
	case LESS:
	{
		for (auto it = table.begin(); it != table.end(); it++)
		{
			uint32_t addr = it - table.begin();
			if (it->at(id1) < it->at(id2))
				match_pairs.emplace_back(addr, addr);
		}
		break;
	}
	case LARGE:
	{
		for (auto it = table.begin(); it != table.end(); it++)
		{
			uint32_t addr = it - table.begin();
			if (it->at(id1) > it->at(id2))
				match_pairs.emplace_back(addr, addr);
		}
		break;
	}
	default:
		throw exception_t(UNKNOWN_RELATION, "Unknown relation type.");
	}
	return std::pair<LightTable *, LightTable *>(&table, &table);
}

std::pair<LightTable *, LightTable *> LightTable::join_self(
	LightTable & table, 
	std::string key, 
	relation_type_t rel_type, 
	attr_t & kAttr, 
	std::vector<AddrPair>& match_pairs)
{
	uint8_t stat = 0x0;
	IndexFile *index = table.get_index_file(key.c_str());
	if (index != NULL)
	{
		IndexType type = index->type();
		stat = (type == HASH || type == PHASH) ? BIT_HAS_HASH :
			(type == TREE || type == PTREE) ? BIT_HAS_TREE : 0;
	}
	
	int attr_id = table.get_attr_id(key);

	switch (rel_type)
	{
	case EQ:
		if ((stat & BIT_HAS_HASH) || (stat & BIT_HAS_TREE))
		{
			index->get(kAttr, match_pairs);
		}
		else
		{
			auto begin = table.begin();
			for (auto it = table.begin(); it != table.end(); it++)
			{
				if (it->at(attr_id) == kAttr)
				{
					uint32_t addr = it - begin;
					match_pairs.emplace_back(addr, addr);
				}
			}
		}
		break;
	case NEQ:
		if ((stat & BIT_HAS_HASH) || (stat & BIT_HAS_TREE))
		{
			index->get_not(kAttr, match_pairs);
		}
		else
		{
			auto begin = table.begin();
			for (auto it = table.begin(); it != table.end(); it++)
			{
				if (it->at(attr_id) != kAttr)
				{
					uint32_t addr = it - begin;
					match_pairs.emplace_back(addr, addr);
				}
			}
		}
		break;
	case LESS:
		if ((stat & BIT_HAS_TREE))
		{
			((TreeIndexFile*)index)->get_less(kAttr, match_pairs);
		}
		else
		{
			auto begin = table.begin();
			for (auto it = table.begin(); it != table.end(); it++)
			{
				if (it->at(attr_id) < kAttr)
				{
					uint32_t addr = it - begin;
					match_pairs.emplace_back(addr, addr);
				}
			}
		}
		break;
	case LARGE:
		if ((stat & BIT_HAS_TREE))
		{
			((TreeIndexFile*)index)->get_large(kAttr, match_pairs);
		}
		else
		{
			auto begin = table.begin();
			for (auto it = table.begin(); it != table.end(); it++)
			{
				if (it->at(attr_id) > kAttr)
				{
					uint32_t addr = it - begin;
					match_pairs.emplace_back(addr, addr);
				}
			}
		}
		break;
	default:
		throw exception_t(UNKNOWN_RELATION, "Unknown relation type.");
	}
	return std::pair<LightTable *, LightTable *>(&table, &table);
}

void LightTable::create(const char * tablename, AttrDesc * descs, int num)
{
	mTablename = tablename;

	std::string tbl_path = mTablename + ".tbl";
	std::string dat_path = mTablename + ".dat";

	mTablefile.open(tbl_path.c_str(), "wb+");
	mTablefile.create(tablename, descs, num);

	init_seq_types(descs, num);

	std::vector<uint32_t> sizes(mTablefile.mTableHeader.attrNum);
	for (int i = 0; i < sizes.size(); i++)
	{
		sizes[i] = mTablefile.mAttrDescPool[i].size;
	}

	mDatafile.open(dat_path.c_str(), "wb+");
	mDatafile.init(mSeqTypes.data(), sizes, num);
}

void LightTable::load(const char * tablename)
{
	mTablename = tablename;

	std::string tbl_path = mTablename + ".tbl";
	std::string dat_path = mTablename + ".dat";
	
	mTablefile.open(tbl_path.c_str(), "rb+");
	mDatafile.open(dat_path.c_str(), "rb+");

	mTablefile.read_from();
	init_seq_types(mTablefile.mAttrDescPool.data(), mTablefile.mAttrDescPool.size());

	std::vector<uint32_t> sizes(mTablefile.mTableHeader.attrNum);
	for (int i = 0; i < sizes.size(); i++)
	{
		sizes[i] = mTablefile.mAttrDescPool[i].size;
	}

	mDatafile.init(mSeqTypes.data(), sizes, mSeqTypes.size());
	mDatafile.read_from();
}

void LightTable::save()
{
	mTablefile.write_back();
	mDatafile.write_back();
}

void LightTable::create_index(const char *attr_name, IndexType type)
{
	const AttrDesc &desc = mTablefile.get_attr_desc(attr_name);
	std::string idx_path = mTablename + "_" + std::string(attr_name) + ".idx";
	
	IndexFile & idx_file = mTablefile.create_index(desc, type, idx_path.c_str());
	if (mDatafile.size() > 0)
	{
		int attr_id = get_attr_id(desc.name);
		for(int i = 0; i < mDatafile.size(); i++)
		{
			AttrTuple & tuple = mDatafile.get(i);
			idx_file.set(tuple[attr_id], i);
		}
	}
}

void LightTable::insert(AttrTuple & tuple)
{
	const auto &header = mTablefile.get_header();
	
	uint32_t addr;
	
	// Primary key
	if (header.primaryKeyIndex >= 0)
	{
		addr = insert_with_pk(tuple);
	}
	// No primary key
	else
	{
		addr = insert_no_pk(tuple);
	}

	update_index(tuple, addr);
}

/*
	LightTable::find()
	
	if specified attribute has index, use index to find all matched addrs
	if not, use exhaustive search to find all matched addrs
*/
uint32_t LightTable::filter(
	const char * attr_name, 
	attr_t &attr, 
	relation_type_t rel_type, 
	std::vector<uint32_t> & match_addrs)
{
	uint32_t match_num = 0;

	// Check if attr has index
	IndexFile *index_file = mTablefile.get_index_file(attr_name);
	if (index_file != NULL)
	{
		filter_with_index(attr_name, attr, rel_type, index_file, match_addrs);
	}
	else
	{
		filter_naive(attr_name, attr, rel_type, match_addrs);
	}	
	return match_num;
}

AttrTuple & LightTable::get_tuple(uint32_t index)
{
	return mDatafile.get(index);
}

int LightTable::get_attr_id(std::string attr_name)
{
	int key_id = mTablefile.get_attr_id(attr_name.c_str());

	if (key_id < 0)
		throw exception_t(UNKNOWN_ATTR, attr_name.c_str());
	return key_id;
}

const AttrDescPool & LightTable::get_attr_descs()
{
	return mTablefile.get_attr_descs();
}

bool LightTable::has_attr(std::string attr_name)
{
	return mTablefile.get_attr_id(attr_name.c_str()) >= 0;
}

uint32_t LightTable::size()
{
	return mDatafile.size();
}

uint32_t LightTable::tuple_size()
{
	return mTablefile.get_header().attrNum;
}

uint8_t LightTable::get_attr_type(int i)
{
	if (i < 0 || i >= mTablefile.mTableHeader.attrNum)
		throw exception_t(0, "Array out of bound.");
	return mTablefile.mAttrDescPool[i].type;
}

void LightTable::dump()
{
	mTablefile.dump_info();
}

void LightTable::dump(LightTable & a, LightTable & b, std::vector<AddrPair>& match_pairs)
{
	for (auto pair : match_pairs)
	{
		uint32_t addr_a = pair.first;
		uint32_t addr_b = pair.second;

		auto ta = a.get_tuple(addr_a);
		auto tb = b.get_tuple(addr_b);

		for (auto e : ta)
			std::cout << e << '\t';
		for ( auto e : tb)
			std::cout << e << '\t';
		std::cout << "\n";
	}
}

AttrTupleIterator LightTable::begin()
{
	return mDatafile.begin();
}

AttrTupleIterator LightTable::end()
{
	return mDatafile.end();
}

inline uint32_t LightTable::insert_with_pk(AttrTuple & tuple)
{
	// lookup index
	int pk_index = mTablefile.mTableHeader.primaryKeyIndex;
	const char *pk_attr_name = mTablefile.get_pk_attr_name();
	PrimaryIndexFile *idx_file = static_cast<PrimaryIndexFile*>(get_index_file(pk_attr_name));

	assert(idx_file != NULL);

	if (idx_file->isExist(tuple[pk_index]))
		throw exception_t(INSERT_DUPLICATE_TUPLE, "Duplicated tuple");

	uint32_t addr = mDatafile.put(tuple);

	idx_file->set(tuple[pk_index], addr);

	return addr;
}

inline uint32_t LightTable::insert_no_pk(AttrTuple & tuple)
{
	// brute search
	auto res = std::find(mDatafile.begin(), mDatafile.end(), tuple);
	if (res != mDatafile.end())
		throw exception_t(INSERT_DUPLICATE_TUPLE, "Duplicated tuple");

	return mDatafile.put(tuple);
}

inline void LightTable::update_index(AttrTuple & tuple, uint32_t addr)
{
	const AttrDescPool & descs = mTablefile.get_attr_descs();
	for (int i = 0; i < descs.size(); i++)
	{
		const attr_t & attr = tuple[i];
		
		IndexFile *index_file = mTablefile.get_index_file(descs[i].name);
		if (index_file != NULL)
		{
			index_file->set(attr, addr);
		}
	}
}

uint32_t LightTable::filter_with_index(
	const char * attr_name, 
	attr_t & attr, 
	relation_type_t rel_type, 
	IndexFile *index_file, 
	std::vector<uint32_t>& match_addrs)
{
	switch (rel_type)
	{
	case EQ:
		index_file->get(attr, match_addrs);
		break;
	case NEQ:
		index_file->get_not(attr, match_addrs);
		break;
	case LESS: case LARGE:
		{
			if (index_file->type() == TREE || index_file->type() == PTREE)
			{
				TreeIndexFile *tindex_file = static_cast<TreeIndexFile*>(index_file);
				tindex_file->get(attr, rel_type, match_addrs);
			}
			else
				filter_naive(attr_name, attr,  rel_type, match_addrs);
		}	
		break;
	default:
		throw exception_t(UNKNOWN_RELATION, "Unknown relation type");
	}

	return match_addrs.size();
}

uint32_t LightTable::filter_naive(const char * attr_name, attr_t & attr, relation_type_t rel_type, std::vector<uint32_t>& match_addrs)
{
	int attr_id = mTablefile.get_attr_id(attr_name);
	if (attr_id < 0)
		throw exception_t(UNKNOWN_ATTR, attr_name);

	for (AttrTupleIterator it = begin(); it != end(); it++)
	{
		switch (rel_type)
		{
		case EQ:
			if (it->at(attr_id) == attr)
				match_addrs.push_back(it - begin());
			break;
		case NEQ:
			if (it->at(attr_id) != attr)
				match_addrs.push_back(it - begin());
			break;
		case LESS:
			if (it->at(attr_id) < attr)
				match_addrs.push_back(it - begin());
			break;
		case LARGE:
			if (it->at(attr_id) > attr)
				match_addrs.push_back(it - begin());
			break;
		default:
			throw exception_t(UNKNOWN_RELATION, "Unknown relation type.");
		}
	}
	return match_addrs.size();
}

inline IndexFile * LightTable::get_index_file(const char * name)
{
	auto res = mTablefile.mIndexFileMap.find(name);
	if (res != mTablefile.mIndexFileMap.end())
	{
		return res->second;
	}
	return nullptr;
}

inline void LightTable::init_seq_types(AttrDesc *descs, int num)
{
	mSeqTypes.clear();
	mSeqTypes.resize(num);
	for (int i = 0; i < num; i++)
	{
		switch (descs[i].type)
		{
		case ATTR_TYPE_INTEGER:
			mSeqTypes[i] = SEQ_INT;
			break;
		case ATTR_TYPE_VARCHAR:
			mSeqTypes[i] = SEQ_VARCHAR;
			break;
		default:
			throw exception_t(ATTR_TYPE_TO_SEQ_TYPE_ERROR, "Attr type to seq type error.");
		}
	}
}

void LightTable::get_selectid_from_names(std::vector<std::string>& names, std::vector<int>& ids)
{
	ids.clear();
	ids.resize(names.size());
	for (int i = 0; i < names.size(); i++)
	{
		int id = mTablefile.get_attr_id(names[i].c_str());
		if (id < 0)
			throw exception_t(UNKNOWN_ATTR, names[i].c_str());
		ids[i] = id;
	}
}

/*
	join_hash

	always use a to iter, b to index

*/
inline void LightTable::cross_hash_join(
	LightTable & a, std::string a_keyname, IndexFile * a_index,
	relation_type_t rel_type, 
	LightTable & b, std::string b_keyname, IndexFile * b_index,
	std::vector<AddrPair> &match_pairs)
{
	assert(rel_type == EQ || rel_type == NEQ);

	int a_key_id = a.get_attr_id(a_keyname.c_str());
	int b_key_id = b.get_attr_id(b_keyname.c_str());

	switch (rel_type)
	{
	case EQ:
		cross_hash_join_eq(a, a_key_id, b, b_index, match_pairs);
		break;
	case NEQ:
		cross_hash_join_neq(a, a_key_id, b, b_index, match_pairs);
		break;
	default:
		assert(false); // Hash index only support EQ, NEQ
	}
}

inline void LightTable::cross_hash_join_eq(
	LightTable & iter_table,
	int iter_key_id,
	LightTable & fix_table, 
	IndexFile * fix_index, 
	std::vector<AddrPair>& match_pairs)
{
	for (auto it = iter_table.begin(); it != iter_table.end(); it++)
	{
		uint32_t iter_addr = it - iter_table.begin();
		attr_t & iter_key_attr = it->at(iter_key_id);

		fix_index->get(iter_key_attr, iter_addr, match_pairs);
	}
}

inline void LightTable::cross_hash_join_neq(
	LightTable & iter_table,
	int iter_key_id, 
	LightTable & fix_table, 
	IndexFile * fix_index, 
	std::vector<AddrPair>& match_pairs)
{
	for (auto it = iter_table.begin(); it != iter_table.end(); it++)
	{
		uint32_t iter_addr = it - iter_table.begin();
		attr_t & iter_key_attr = it->at(iter_key_id);

		fix_index->get_not(iter_key_attr, iter_addr, match_pairs);
	}
}

void LightTable::cross_two_tree_join(
	LightTable & a, std::string a_keyname, IndexFile * a_index,
	relation_type_t rel_type, 
	LightTable & b, std::string b_keyname, IndexFile * b_index,
	std::vector<AddrPair> &match_pairs)
{
	assert(a_index != NULL && b_index != NULL);

	const TreeIndexFile &ta = static_cast<TreeIndexFile&>(*a_index);
	const TreeIndexFile &tb = static_cast<TreeIndexFile&>(*b_index);

	// if tb if null, map ta to null space
	switch (rel_type)
	{
	case EQ:
		TreeIndexFile::merge_eq(ta, tb, match_pairs);
		break;
	case NEQ:
		TreeIndexFile::merge_neq(ta, tb, match_pairs);
		break;
	case LESS:
		TreeIndexFile::merge_less(ta, tb, match_pairs);
		break;
	case LARGE:
		TreeIndexFile::merge_large(ta, tb, match_pairs);
		break;
	default:
		throw exception_t(UNKNOWN_RELATION, "Unknown relation type");
	}
	
}

inline void LightTable::cross_one_tree_join(
	LightTable & a, std::string a_keyname, IndexFile * a_index, 
	relation_type_t rel_type,
	LightTable & b, std::string b_keyname, IndexFile * b_index, 
	std::vector<AddrPair>& match_pairs)
{
	assert(b_index != NULL && b_index->type() == TREE);
	
	TreeIndexFile &tindex = static_cast<TreeIndexFile&>(*b_index);
	LightTable & iter_table = a;

	int iter_key_id = a.mTablefile.get_attr_id(a_keyname.c_str());
	//int b_key_id = b.mTablefile.get_attr_id(b_keyname.c_str());

	switch (rel_type)
	{
	case EQ:
		for (auto it = iter_table.begin(); it != iter_table.end(); it++)
		{
			uint32_t iter_addr = it - iter_table.begin();
			attr_t & iter_key_attr = it->at(iter_key_id);
			tindex.get(iter_key_attr, iter_addr, match_pairs);
		}
		break;
	case NEQ:
		for (auto it = iter_table.begin(); it != iter_table.end(); it++)
		{
			uint32_t iter_addr = it - iter_table.begin();
			attr_t & iter_key_attr = it->at(iter_key_id);
			tindex.get_not(iter_key_attr, iter_addr, match_pairs);
		}
		break;
	case LESS:
		for (auto it = iter_table.begin(); it != iter_table.end(); it++)
		{
			uint32_t iter_addr = it - iter_table.begin();
			attr_t & iter_key_attr = it->at(iter_key_id);
			tindex.get_less(iter_key_attr, iter_addr, match_pairs);
		}
		break;
	case LARGE:
		for (auto it = iter_table.begin(); it != iter_table.end(); it++)
		{
			uint32_t iter_addr = it - iter_table.begin();
			attr_t & iter_key_attr = it->at(iter_key_id);
			tindex.get_large(iter_key_attr, iter_addr, match_pairs);
		}
		break;
	default:
		throw exception_t(UNKNOWN_RELATION, "Unknown relation type");
	}
}

inline void LightTable::map(
	std::vector<uint32_t>& addrs, 
	LightTable * onto_table, 
	std::vector<AddrPair>& match_pairs)
{
	if (onto_table == NULL)
	{
		for (uint32_t addr : addrs)
			match_pairs.emplace_back(addr, 0);
	}
	else
	{
		auto begin = onto_table->begin();
		auto end = onto_table->end();

		for (uint32_t addr : addrs)
			for (auto it = begin; it != end; it++)
				match_pairs.emplace_back(addr, it - begin);
	}

}

inline void LightTable::product(
	LightTable * a, 
	LightTable * b, 
	std::vector<AddrPair>& match_pairs)
{
	for (auto ait = a->begin(); ait != a->end(); ait++)
		for (auto bit = b->begin(); bit != b->end(); bit++)
			match_pairs.emplace_back(ait - a->begin(), bit - b->begin());
}

std::vector<AddrPair> LightTable::product(
	std::vector<AddrPair>& reflexive_pairs, 
	LightTable * b)
{
	std::vector<AddrPair> match_pairs;
	for (auto & pair : reflexive_pairs)
	{
		for (int i = 0; i < b->size(); i++)
		{
			match_pairs.emplace_back(pair.first, i);
		}
	}
	return match_pairs;
}

void LightTable::cross_naive_join(
	LightTable & a, std::string a_keyname, 
	relation_type_t rel_type, 
	LightTable & b, std::string b_keyname,
	std::vector<AddrPair> &match_pairs)
{
	/*
		foreach a in table_a:
			foreach b in table_b:
				if a.a_key rel b.b_key
					match_pairs.push(<a.id, b.id>)
	*/
	int a_key_id = a.mTablefile.get_attr_id(a_keyname.c_str());
	int b_key_id = b.mTablefile.get_attr_id(b_keyname.c_str());

	if (a_key_id < 0)
		throw exception_t(UNKNOWN_ATTR, a_keyname.c_str());
	if (b_key_id < 0)
		throw exception_t(UNKNOWN_ATTR, b_keyname.c_str());

	for (AttrTupleIterator ait = a.begin(); ait != a.end(); ait++)
	{
		for (AttrTupleIterator bit = b.begin(); bit != b.end(); bit++)
		{	
			int a_id = ait - a.begin();
			int b_id = bit - b.begin();

			switch (rel_type)
			{
			case EQ:
				if (ait->at(a_key_id) == bit->at(b_key_id))
					match_pairs.push_back({ a_id, b_id});
				break;
			case NEQ:
				if (!(ait->at(a_key_id) == bit->at(b_key_id)))
					match_pairs.push_back({ a_id, b_id });
				break;
			case LESS:
				if ((ait->at(a_key_id) < bit->at(b_key_id)))
					match_pairs.push_back({ a_id, b_id });
				break;
			case LARGE:
				if ((ait->at(a_key_id) > bit->at(b_key_id)))
					match_pairs.push_back({ a_id, b_id });
				break;
			default:
				throw exception_t(JOIN_UNKNOWN_RELATION_TYPE, "Unknown relation type");
			}
		}
	}
}

void LightTable::merge(
	std::vector<AddrPair> & a,
	merge_type_t merge_type, 
	std::vector<AddrPair> & b,
	std::vector<AddrPair> & c)
{
	if(a.begin() != a.end())
		std::sort(a.begin(), a.end());
	if(b.begin() != b.end())
		std::sort(b.begin(), b.end());

	std::vector<AddrPair>::iterator it;

	c.resize(a.size() + b.size());

	switch (merge_type)
	{
	case AND:
		it = std::set_intersection(a.begin(), a.end(), b.begin(), b.end(), c.begin());
		break;
	case OR:
		it = std::set_union(a.begin(), a.end(), b.begin(), b.end(), c.begin());
		break;
	default:
		throw exception_t(UNSUPPORT_MERGE_TYPE, "Unsupported merge type.");
	}
	
	c.resize(it - c.begin());
}

std::pair<LightTable *, LightTable *> LightTable::merge(
	std::pair<LightTable *, LightTable *> left_comb,
	std::vector<AddrPair>& left, 
	merge_type_t merge_type, 
	std::pair<LightTable *, LightTable *> right_comb,
	std::vector<AddrPair>& right, 
	std::vector<AddrPair>& c,
	std::vector<LightTable*>  & from_tables)
{
	assert(left_comb.first != NULL && left_comb.second != NULL);
	assert(right_comb.first != NULL && right_comb.second != NULL);

	// AB AB (no AB, BA for now) => AB
	bool a_reflex = left_comb.first == left_comb.second;
	bool b_reflex = right_comb.first == right_comb.second;

	if (!a_reflex && !b_reflex)
	{
		if (left_comb.first != right_comb.first)
			throw exception_t(TABLE_COMB_ERROR, "Table combination error, no AB, BA");
		merge(left, merge_type, right, c);
		return std::pair<LightTable *, LightTable *>(left_comb.first, left_comb.second);
	}
	else if(a_reflex && b_reflex)
	{
		// AA, AA => AA
		if (left_comb.first == right_comb.first)
		{
			if (from_tables.size() == 1)
			{
				merge(left, merge_type, right, c);
				return std::pair<LightTable *, LightTable *>(left_comb.first, left_comb.first);
			}
			else if(from_tables.size() == 2)
			{
				std::vector<AddrPair> tmp(left.size());
				if (left.begin() != left.end())
					std::sort(left.begin(), left.end());
				if (right.begin() != right.end())
					std::sort(right.begin(), right.end());

				std::vector<AddrPair>::iterator it;

				tmp.resize(left.size() + right.size());

				switch (merge_type)
				{
				case AND:
					it = std::set_intersection(left.begin(), left.end(), right.begin(), right.end(), tmp.begin());
					break;
				case OR:
					it = std::set_union(left.begin(), left.end(), right.begin(), right.end(), tmp.begin());
					break;
				default:
					break;
				}
				c.resize(it - c.begin());

				LightTable *other = (from_tables[0] == left_comb.first) ? from_tables[1] : from_tables[0];
				for (auto ait = left.begin(); ait != left.end(); ait++)
					for (int i = 0; i < other->size(); i++)
						c.emplace_back(ait->first, i);
				return std::pair<LightTable *, LightTable *>(left_comb.first, other);
			}
		}
		else
		{
			// AA BB => AB
			for (int ai = 0; ai < left.size(); ai++)
				for (int bi = 0; bi < right.size(); bi++)
					c.emplace_back(left[ai].first, right[bi].second);
			return std::pair<LightTable *, LightTable *>( left_comb.first , right_comb.second );
		}
	}
	else if (a_reflex)
	{
		// AA AB => AB
		if(right_comb.second == left_comb.first)
			throw exception_t(TABLE_COMB_ERROR, "Table combination error, no AA, BA");
		if (left.begin() != left.end())
			std::sort(left.begin(), left.end());
		if (right.begin() != right.end())
			std::sort(right.begin(), right.end());
		switch (merge_type)
		{
			case AND:
			{
				int ai = 0, bi = 0;
				while (ai < left.size() && bi < right.size())
				{
					if (left[ai].first == right[bi].first)
					{
						c.emplace_back(left[ai].first, right[bi].second);
						bi++;
					}
					else if(left[ai].first < right[bi].first)
					{
						ai++;
					}
					else
					{
						bi++;
					}
				}
			}
			break;
			case OR:
			{
				// A is unique
				int li = 0, ri = 0;
				while (li < left.size() && ri < right.size())
				{
					if (left[li].first == right[ri].first)
					{
						ri++;
					}
					else if (left[li].first < right[ri].first)
					{
						for (int i = 0; i < right_comb.second->size(); i++)
							c.emplace_back(left[li].first, i);
						li++;
					}
					else
					{
						c.emplace_back(right[ri].first, right[ri].second);
						ri++;
					}
				}

				while (li < left.size())
				{
					for (int i = 0; i < right_comb.second->size(); i++)
						c.emplace_back(left[li].first, i);
					li++;
				}

				while (ri < right.size())
				{
					c.emplace_back(right[ri].first, right[ri].second);
					ri++;
				}
			}
			break;
		default:
			break;
		}
		

		return std::pair<LightTable *, LightTable *>( left_comb.first , right_comb.second );
	}
	else
	{
		// AB AA => AB
		if (!(left_comb.first == right_comb.first))
			throw exception_t(TABLE_COMB_ERROR, "Table combination error, no BA, AA");
		if (left.begin() != left.end())
			std::sort(left.begin(), left.end());
		if (right.begin() != right.end())
			std::sort(right.begin(), right.end());
		switch (merge_type)
		{
		case AND:
		{
			int li = 0, ri = 0;
			while (li < left.size() && ri < right.size())
			{
				if (left[li].first == right[ri].first)
				{
					c.emplace_back(left[li].first, left[li].second);
					li++;
				}
				else if (left[li].first < right[ri].first)
				{
					li++;
				}
				else
				{
					ri++;
				}
			}
		}
		break;
		case OR:
		{
			// Right is unique
			int li = 0, ri = 0;
			while (li < left.size() && ri < right.size())
			{
				if (left[li].first == right[ri].first)
				{
					li++;
				}
				else if (left[li].first < right[ri].first)
				{
					c.emplace_back(right[ri].first, right[ri].second);
					li++;
				}
				else
				{
					for (int i = 0; i < left_comb.second->size(); i++)
						c.emplace_back(right[ri].first, i);
					ri++;
				}
			}

			while (ri < right.size())
			{
				for (int i = 0; i < left_comb.second->size(); i++)
					c.emplace_back(right[ri].first, i);
				ri++;
			}

			while (li < left.size())
			{
				c.emplace_back(left[li].first, left[li].second);
				li++;
			}
		}
		break;
		default:
			break;
		}

		return std::pair<LightTable *, LightTable *>(left_comb.first , left_comb.second);
	}
}
