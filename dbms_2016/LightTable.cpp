#include "LightTable.h"

#include <algorithm>

#define BIT_HAS_HASH 0x1
#define BIT_HAS_TREE 0x2

std::ostream &operator <<(std::ostream &os, const AttrTuple tuple)
{
	for (auto it = tuple.begin(); it != tuple.end(); it++)
		os << *it << "\t";
	return os;
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
void LightTable::join(
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
	case EQ:
		// 1. Hash join (if two, choose small one to iterate)
		// 2. Tree join
		// 3. Naive
		if ((a_stat & BIT_HAS_HASH) || (b_stat & BIT_HAS_HASH))
			join_hash(a, a_keyname, *a_index_file, 
				rel_type, 
				b, b_keyname, *b_index_file, match_pairs);
		else if ((a_stat & BIT_HAS_TREE) && (b_stat & BIT_HAS_TREE))
			join_merge(a, a_keyname, *a_index_file,
				rel_type,
				b, b_keyname, *b_index_file, match_pairs);
		else
			join_naive(a, a_keyname,
				rel_type,
				b, b_keyname,
				match_pairs);
		break;
	case NEQ:
		break;
	case LESS:
		break;
	case LARGE:
		break;
	default:
		break;
	}
}

void LightTable::select(
	LightTable & a, std::vector<std::string>& a_select_attr_names, 
	LightTable & b, std::vector<std::string>& b_select_attr_names, 
	std::vector<AddrPair>& match_pairs)
{
	// In-Tuple id
	std::vector<int> a_select_ids;
	std::vector<int> b_select_ids;

	a.get_selectid_from_names(a_select_attr_names, a_select_ids);
	b.get_selectid_from_names(b_select_attr_names, b_select_ids);

	for (std::vector<AddrPair>::iterator it = match_pairs.begin(); it != match_pairs.end(); it++)
	{
		// Tuple id
		int a_id = it->first;
		int b_id = it->second;
		
		const AttrTuple &a_tuple = a.get_tuple(a_id);
		for (int tuple_element_id : a_select_ids)
		{
			std::cout << a_tuple[tuple_element_id] << "\t";
		}

		const AttrTuple &b_tuple = b.get_tuple(b_id);
		for (int tuple_element_id : b_select_ids)
		{
			std::cout << b_tuple[tuple_element_id] << "\t";
		}
		std::cout << "\n";
	}
}

void LightTable::create(const char * tablename, AttrDesc * descs, int num)
{
	mTablename = tablename;

	std::string tbl_path = mTablename + ".tbl";
	std::string dat_path = mTablename + ".dat";

	mTablefile.open(tbl_path.c_str(), "wb+");
	mTablefile.create(tablename, descs, num);

	init_seq_types(descs, num);

	mDatafile.open(dat_path.c_str(), "w+");
	mDatafile.init(mSeqTypes.data(), num);
}

void LightTable::load(const char * tablename)
{
	mTablename = tablename;

	std::string tbl_path = mTablename + ".tbl";
	std::string dat_path = mTablename + ".dat";
	
	mTablefile.open(tbl_path.c_str(), "rb+");
	mDatafile.open(dat_path.c_str(), "r+");

	mTablefile.read_from();
	init_seq_types(mTablefile.mAttrDescPool.data(), mTablefile.mAttrDescPool.size());

	mDatafile.init(mSeqTypes.data(), mSeqTypes.size());
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
	mTablefile.create_index(desc, type, idx_path.c_str());
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
uint32_t LightTable::find(
	const char * attr_name, 
	attr_t &attr, 
	relation_type_t find_type, 
	std::vector<uint32_t> & match_addrs)
{
	uint32_t match_num = 0;

	// Check if attr has index
	IndexFile *index_file = mTablefile.get_index_file(attr_name);
	if (index_file != NULL)
	{
		match_num = find_with_index(attr_name, attr, find_type, index_file, match_addrs);
	}
	else
	{
		int attr_id = mTablefile.get_attr_id(attr_name);
		if (attr_id < 0)
			throw exception_t(UNKNOWN_ATTR, attr_name);
		
		for (AttrTupleIterator it = begin(); it != end(); it++)
		{
			if (it->at(attr_id) == attr)
			{
				match_num++;
				match_addrs.push_back(it - begin());
			}
		}
	}	
	return match_num;
}

AttrTuple & LightTable::get_tuple(uint32_t index)
{
	return mDatafile.get(index);
}

void LightTable::dump()
{
	mTablefile.dump_info();
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

uint32_t LightTable::find_with_index(
	const char * attr_name, 
	attr_t & attr, 
	relation_type_t find_type, 
	IndexFile *index_file, 
	std::vector<uint32_t>& match_addrs)
{
	uint32_t match_num = 0;

	switch (find_type)
	{
	case EQ:
		match_num = index_file->get(attr, match_addrs);
		break;
	case NEQ:
		assert(false);
		break;
	case LESS: 
		{
			assert(index_file->type() == TREE || index_file->type() == PTREE);
			TreeIndexFile *tindex_file = static_cast<TreeIndexFile*>(index_file);
			match_num = tindex_file->get(attr, LESS, match_addrs);
		}	
		break;
	case LARGE:
		{
			assert(index_file->type() == TREE || index_file->type() == PTREE);
			TreeIndexFile *tindex_file = static_cast<TreeIndexFile*>(index_file);
			match_num = tindex_file->get(attr, LARGE, match_addrs);
		}
		break;
	default:
		throw exception_t(UNKNOWN_RELATION, "Unknown relation type");
	}

	return match_num;
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

void LightTable::join_hash(
	LightTable & a, std::string a_keyname, IndexFile & a_index,
	relation_type_t rel_type, 
	LightTable & b, std::string b_keyname, IndexFile & b_index,
	std::vector<AddrPair> &match_pairs)
{
}

void LightTable::join_merge(
	LightTable & a, std::string a_keyname, IndexFile & a_index,
	relation_type_t rel_type, 
	LightTable & b, std::string b_keyname, IndexFile & b_index,
	std::vector<AddrPair> &match_pairs)
{
}

void LightTable::join_naive(
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
					match_pairs.push_back({a_id, b_id});
				break;
			case NEQ:
				if (!(ait->at(a_key_id) == bit->at(b_key_id)))
					match_pairs.push_back({ a_id, b_id });
				break;
			case LESS:
				if (!(ait->at(a_key_id) < bit->at(b_key_id)))
					match_pairs.push_back({ a_id, b_id });
				break;
			case LARGE:
				if (!(ait->at(a_key_id) > bit->at(b_key_id)))
					match_pairs.push_back({ a_id, b_id });
				break;
			default:
				throw exception_t(JOIN_UNKNOWN_RELATION_TYPE, "Unknown relation type");
			}
		}
	}
}
