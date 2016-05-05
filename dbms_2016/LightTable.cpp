#include "LightTable.h"

#include <algorithm>

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

uint32_t LightTable::find(const char * attr_name, attr_t &attr, FindType find_type, std::vector<uint32_t> & match_addrs)
{
	// Check if attr has index
	IndexFile *index_file = mTablefile.get_index_file(attr_name);
	if (index_file != NULL)
	{
		return find_with_index(attr_name, attr, find_type, index_file, match_addrs);
	}
	else
	{
		return 0;
	}
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
	FindType find_type, 
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
		break;
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
	throw exception_t(INDEX_NOT_FOUND, name);
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
