#include "IndexFile.h"

#include "system.h"

#include <iostream>
#include <algorithm>

using namespace std;

IndexFile::IndexFile(attr_domain_t keydomain, uint32_t keysize, IndexType index_type) : 
	mKeydomain(keydomain), mKeysize(keysize), mType(index_type)
{
}

IndexFile::~IndexFile()
{
}

HashIndexFile::HashIndexFile(attr_domain_t keydomain, uint32_t keysize) : 
	IndexFile(keydomain, keysize, HASH)
{
}

HashIndexFile::~HashIndexFile()
{
}

bool HashIndexFile::set(const attr_t & attr_ref, const uint32_t record_addr)
{
	// Multi_map always insertion success
	mHashIndexTable.insert(pair<attr_t, uint32_t>(attr_ref, record_addr));
	return true;
}

uint32_t HashIndexFile::get(const attr_t &attr_ref, std::vector<uint32_t> &match_addrs)
{
	auto result = mHashIndexTable.equal_range(attr_ref);
	if (result.first == result.second)
		return 0;

	uint32_t cnt = 0;
	for (auto it = result.first; it != result.second; it++) 
	{
		match_addrs.push_back(it->second);
		cnt++;
	}
	return cnt;
}

uint32_t HashIndexFile::get(const attr_t &attr_ref, std::vector<AddrPair> &match_pairs)
{
	auto result = mHashIndexTable.equal_range(attr_ref);
	if (result.first == result.second)
		return 0;

	for (auto it = result.first; it != result.second; it++)
	{
		match_pairs.emplace_back(it->second, it->second);
	}
	return match_pairs.size();
}

uint32_t HashIndexFile::get(const attr_t &attr_ref, const uint32_t fix_addr, std::vector<AddrPair> &match_pairs)
{
	auto result = mHashIndexTable.equal_range(attr_ref);
	if (result.first == result.second)
		return 0;

	for (auto it = result.first; it != result.second; it++)
	{
		match_pairs.emplace_back(fix_addr, it->second);
	}
	return match_pairs.size();
}

uint32_t HashIndexFile::get_not(const attr_t & attr_ref, std::vector<uint32_t>& match_addrs)
{
	for (auto it = mHashIndexTable.begin(); it != mHashIndexTable.end(); it++)
	{
		if (!(it->first == attr_ref))
			match_addrs.push_back(it->second);
	}

	return match_addrs.size();
}

uint32_t HashIndexFile::get_not(const attr_t & attr_ref, std::vector<AddrPair>& match_pairs)
{
	for (auto it = mHashIndexTable.begin(); it != mHashIndexTable.end(); it++)
	{
		if (!(it->first == attr_ref))
			match_pairs.emplace_back(it->second, it->second);
	}

	return match_pairs.size();
}

uint32_t HashIndexFile::get_not(const attr_t & attr_ref, const uint32_t fix_addr, std::vector<AddrPair>& match_pairs)
{
	for (auto it = mHashIndexTable.begin(); it != mHashIndexTable.end(); it++)
	{
		if (!(it->first == attr_ref))
			match_pairs.emplace_back(fix_addr, it->second);
	}

	return match_pairs.size();
}

void HashIndexFile::write_back()
{
	assert(mKeydomain != UNDEFINED_DOMAIN && mFile != NULL);
	fseek(mFile, 0, SEEK_SET);

	for (HashIndexTable::iterator it = mHashIndexTable.begin(); it != mHashIndexTable.end(); it++)
	{
		if (mKeydomain == INTEGER_DOMAIN)
			write_back_pair(it->first.Int(), it->second);
		else if (mKeydomain == VARCHAR_DOMAIN)
			write_back_pair(it->first.Varchar(), it->second);
	}
}

void HashIndexFile::read_from()
{
	assert(mKeydomain != UNDEFINED_DOMAIN && mFile != NULL);
	fseek(mFile, 0, SEEK_SET);

	uint32_t addr;
	if (mKeydomain == INTEGER_DOMAIN)
	{
		int ival;
		while (read_from_pair(&ival, &addr))
		{
			mHashIndexTable.insert(pair<attr_t, uint32_t>(attr_t(ival), addr));
		}
	}
	else if (mKeydomain == VARCHAR_DOMAIN)
	{
		char sval[ATTR_SIZE_MAX + 1];
		memset(sval, 0, ATTR_SIZE_MAX + 1);

		while (read_from_pair(sval, &addr))
		{
			mHashIndexTable.insert(pair<attr_t, uint32_t>(attr_t(sval), addr));
			memset(sval, 0, ATTR_SIZE_MAX + 1);
		}
	}
}

void HashIndexFile::dump()
{
	for (HashIndexTable::iterator it = mHashIndexTable.begin(); it != mHashIndexTable.end(); it++)
	{
		cout << it->first << " -> " << it->second << endl;
	}
}

PrimaryIndexFile::PrimaryIndexFile(attr_domain_t keydomain, uint32_t keysize) : 
	IndexFile(keydomain, keysize, PHASH)
{
}

PrimaryIndexFile::~PrimaryIndexFile()
{
}

bool PrimaryIndexFile::set(const attr_t & attr_ref, const uint32_t record_addr)
{
	return mPrimaryIndexTable.insert(pair<attr_t, uint32_t>(attr_ref, record_addr)).second;
}

uint32_t PrimaryIndexFile::get(const attr_t & attr_ref, std::vector<uint32_t>& match_addrs)
{
	auto res = mPrimaryIndexTable.find(attr_ref);
	if (res != mPrimaryIndexTable.end())
	{
		match_addrs.push_back(res->second);
		return true;
	}
	return false;
}

uint32_t PrimaryIndexFile::get(const attr_t & attr_ref, std::vector<AddrPair>& match_pairs)
{
	auto res = mPrimaryIndexTable.find(attr_ref);
	if (res != mPrimaryIndexTable.end())
	{
		match_pairs.emplace_back(res->second, res->second);
		return true;
	}
	return false;
}

uint32_t PrimaryIndexFile::get(const attr_t & attr_ref, const uint32_t fix_addr, std::vector<AddrPair>& match_pairs)
{
	auto res = mPrimaryIndexTable.find(attr_ref);
	if (res != mPrimaryIndexTable.end())
	{
		match_pairs.emplace_back(fix_addr, res->second);
		return 1;
	}
	return 0;
}

uint32_t PrimaryIndexFile::get_not(const attr_t & attr_ref, std::vector<uint32_t>& match_addrs)
{
	for (auto it = mPrimaryIndexTable.begin(); it != mPrimaryIndexTable.end(); it++)
	{
		if (!(it->first == attr_ref))
			match_addrs.push_back(it->second);
	}

	return match_addrs.size();
}

uint32_t PrimaryIndexFile::get_not(const attr_t & attr_ref, std::vector<AddrPair>& match_pairs)
{
	for (auto it = mPrimaryIndexTable.begin(); it != mPrimaryIndexTable.end(); it++)
	{
		if (!(it->first == attr_ref))
			match_pairs.emplace_back(it->second, it->second);
	}

	return match_pairs.size();
}

uint32_t PrimaryIndexFile::get_not(const attr_t & attr_ref, const uint32_t fix_addr, std::vector<AddrPair>& match_pairs)
{
	for (auto it = mPrimaryIndexTable.begin(); it != mPrimaryIndexTable.end(); it++)
	{
		if (!(it->first == attr_ref))
			match_pairs.emplace_back(fix_addr, it->second);
	}

	return match_pairs.size();
}

bool PrimaryIndexFile::get_primary(const attr_t & attr_ref, uint32_t * match_addr)
{
	assert(match_addr != NULL);

	auto res = mPrimaryIndexTable.find(attr_ref);
	if (res != mPrimaryIndexTable.end())
	{
		*match_addr = res->second;
		return true;
	}
	return false;
}

bool PrimaryIndexFile::isExist(const attr_t & attr_ref)
{
	return (mPrimaryIndexTable.find(attr_ref) != mPrimaryIndexTable.end());
}

void PrimaryIndexFile::write_back()
{
	assert(mKeydomain != UNDEFINED_DOMAIN && mFile != NULL);
	fseek(mFile, 0, SEEK_SET);

	for (HashIndexTable::iterator it = mPrimaryIndexTable.begin(); it != mPrimaryIndexTable.end(); it++)
	{
		if (mKeydomain == INTEGER_DOMAIN)
			write_back_pair(it->first.Int(), it->second);
		else if (mKeydomain == VARCHAR_DOMAIN)
			write_back_pair(it->first.Varchar(), it->second);
	}
}

void PrimaryIndexFile::read_from()
{
	assert(mKeydomain != UNDEFINED_DOMAIN && mFile != NULL);
	fseek(mFile, 0, SEEK_SET);

	uint32_t addr;
	if (mKeydomain == INTEGER_DOMAIN)
	{
		int ival;
		while (read_from_pair(&ival, &addr))
		{
			mPrimaryIndexTable.insert(pair<attr_t, uint32_t>(attr_t(ival), addr));
		}
	}
	else if (mKeydomain == VARCHAR_DOMAIN)
	{
		char sval[ATTR_SIZE_MAX + 1];
		memset(sval, 0, ATTR_SIZE_MAX + 1);
		while (read_from_pair(sval, &addr))
		{
			mPrimaryIndexTable.insert(pair<attr_t, uint32_t>(attr_t(sval), addr));
			memset(sval, 0, ATTR_SIZE_MAX + 1);
		}
	}
}

void PrimaryIndexFile::dump()
{
	for (HashIndexTable::iterator it = mPrimaryIndexTable.begin(); it != mPrimaryIndexTable.end(); it++)
	{
		cout << it->first << " -> " << it->second << endl;
	}
}

TreeIndexFile::TreeIndexFile(attr_domain_t keydomain, uint32_t keysize) : IndexFile(keydomain, keysize, TREE)
{
}

TreeIndexFile::~TreeIndexFile()
{
}

bool TreeIndexFile::set(const attr_t & attr_ref, const uint32_t record_addr)
{
	mTreeIndexTable.insert(pair<attr_t, uint32_t>(attr_ref, record_addr));
	return true;
}

uint32_t TreeIndexFile::get(const attr_t & attr_ref, std::vector<uint32_t>& match_addrs)
{
	auto range = mTreeIndexTable.equal_range(attr_ref);
	TreeIndexTable::iterator begin = range.first;
	TreeIndexTable::iterator end = range.second;

	for (auto it = begin; it != end; it++)
		match_addrs.emplace_back(it->second);
	
	return match_addrs.size();
}

uint32_t TreeIndexFile::get(const attr_t & attr_ref, std::vector<AddrPair>& match_pairs)
{
	auto range = mTreeIndexTable.equal_range(attr_ref);
	TreeIndexTable::iterator begin = range.first;
	TreeIndexTable::iterator end = range.second;

	for (auto it = begin; it != end; it++)
		match_pairs.emplace_back(it->second, it->second);

	return match_pairs.size();
}

uint32_t TreeIndexFile::get(const attr_t & attr_ref, const relation_type_t rel_type, std::vector<uint32_t>& match_addrs)
{
	switch (rel_type)
	{
	case EQ:
		return get(attr_ref, match_addrs);
	case NEQ:
		return get_not(attr_ref, match_addrs);
	case LESS:
		return get_less(attr_ref, match_addrs);
	case LARGE:
		return get_large(attr_ref, match_addrs);
	default:
		throw exception_t(UNKNOWN_RELATION_TYPE, "Unkown relation type");
	}
}

uint32_t TreeIndexFile::get(const attr_t & attr_ref, const uint32_t fix_addr, std::vector<AddrPair>& match_pairs)
{
	auto range = mTreeIndexTable.equal_range(attr_ref);
	TreeIndexTable::iterator begin = range.first;
	TreeIndexTable::iterator end = range.second;

	for (auto it = begin; it != end; it++)
		match_pairs.emplace_back(fix_addr, it->second);

	return match_pairs.size();
}

uint32_t TreeIndexFile::get_not(const attr_t & attr_ref, std::vector<uint32_t>& match_addrs)
{
	auto range = mTreeIndexTable.equal_range(attr_ref);
	TreeIndexTable::iterator begin = range.first;
	TreeIndexTable::iterator end = range.second;
	
	for (auto it = mTreeIndexTable.begin(); it != begin; it++)
		match_addrs.emplace_back(it->second);
	for (auto it = end; it != mTreeIndexTable.end(); it++)
		match_addrs.emplace_back(it->second);

	return match_addrs.size();
}

uint32_t TreeIndexFile::get_not(const attr_t & attr_ref, std::vector<AddrPair> & match_pairs)
{
	auto range = mTreeIndexTable.equal_range(attr_ref);
	TreeIndexTable::iterator begin = range.first;
	TreeIndexTable::iterator end = range.second;

	for (auto it = mTreeIndexTable.begin(); it != begin; it++)
		match_pairs.emplace_back(it->second, it->second);
	for (auto it = end; it != mTreeIndexTable.end(); it++)
		match_pairs.emplace_back(it->second, it->second);

	return match_pairs.size();
}

uint32_t TreeIndexFile::get_not(const attr_t & attr_ref, const uint32_t fix_addr, std::vector<AddrPair>& match_pairs)
{
	auto range = mTreeIndexTable.equal_range(attr_ref);
	
	TreeIndexTable::iterator begin = range.first;
	TreeIndexTable::iterator end = range.second;
	
	for (auto it = mTreeIndexTable.begin(); it != begin; it++)
		match_pairs.emplace_back(fix_addr, it->second);
	
	for (auto it = end; it != mTreeIndexTable.end(); it++)
		match_pairs.emplace_back(fix_addr, it->second);
	
	return match_pairs.size();
}

uint32_t TreeIndexFile::get_less(const attr_t & attr_ref, std::vector<uint32_t>& match_addrs)
{
	TreeIndexTable::iterator begin = mTreeIndexTable.begin();
	TreeIndexTable::iterator end = mTreeIndexTable.lower_bound(attr_ref);
	for (auto it = begin; it != end; it++)
		match_addrs.emplace_back(it->second);

	return match_addrs.size();
}

uint32_t TreeIndexFile::get_less(const attr_t & attr_ref, std::vector<AddrPair>& match_pairs)
{
	TreeIndexTable::iterator begin = mTreeIndexTable.begin();
	TreeIndexTable::iterator end = mTreeIndexTable.lower_bound(attr_ref);
	for (auto it = begin; it != end; it++)
		match_pairs.emplace_back(it->second, it->second);

	return match_pairs.size();
}

uint32_t TreeIndexFile::get_less(const attr_t & attr_ref, const uint32_t fix_addr, std::vector<AddrPair>& match_pairs)
{
	TreeIndexTable::iterator begin = mTreeIndexTable.begin();
	TreeIndexTable::iterator end = mTreeIndexTable.lower_bound(attr_ref);
	for (auto it = begin; it != end; it++)
		match_pairs.emplace_back(fix_addr, it->second);

	return match_pairs.size();
}

uint32_t TreeIndexFile::get_large(const attr_t & attr_ref, std::vector<uint32_t>& match_addrs)
{
	TreeIndexTable::iterator begin = mTreeIndexTable.upper_bound(attr_ref);
	TreeIndexTable::iterator end = mTreeIndexTable.end();
	for (auto it = begin; it != end; it++)
		match_addrs.emplace_back(it->second);

	return match_addrs.size();
}

uint32_t TreeIndexFile::get_large(const attr_t & attr_ref, std::vector<AddrPair>& match_pairs)
{
	TreeIndexTable::iterator begin = mTreeIndexTable.upper_bound(attr_ref);
	TreeIndexTable::iterator end = mTreeIndexTable.end();
	for (auto it = begin; it != end; it++)
		match_pairs.emplace_back(it->second, it->second);

	return match_pairs.size();
}

uint32_t TreeIndexFile::get_large(const attr_t & attr_ref, const uint32_t fix_addr, std::vector<AddrPair>& match_pairs)
{
	TreeIndexTable::iterator begin = mTreeIndexTable.upper_bound(attr_ref);
	TreeIndexTable::iterator end = mTreeIndexTable.end();
	for (auto it = begin; it != end; it++)
		match_pairs.emplace_back(fix_addr, it->second);

	return match_pairs.size();
}

void TreeIndexFile::write_back()
{
	assert(mKeydomain != UNDEFINED_DOMAIN && mFile != NULL);
	fseek(mFile, 0, SEEK_SET);

	for (TreeIndexTable::iterator it = mTreeIndexTable.begin(); it != mTreeIndexTable.end(); it++)
	{
		if (mKeydomain == INTEGER_DOMAIN)
			write_back_pair(it->first.Int(), it->second);
		else if (mKeydomain == VARCHAR_DOMAIN)
			write_back_pair(it->first.Varchar(), it->second);
	}
}

void TreeIndexFile::read_from()
{
	assert(mKeydomain != UNDEFINED_DOMAIN && mFile != NULL);
	fseek(mFile, 0, SEEK_SET);

	uint32_t addr;
	if (mKeydomain == INTEGER_DOMAIN)
	{
		int ival;
		while (read_from_pair(&ival, &addr))
		{
			mTreeIndexTable.insert(pair<attr_t, uint32_t>(attr_t(ival), addr));
		}
	}
	else if (mKeydomain == VARCHAR_DOMAIN)
	{
		char sval[ATTR_SIZE_MAX + 1];
		memset(sval, 0, ATTR_SIZE_MAX + 1);
		while (read_from_pair(sval, &addr))
		{
			mTreeIndexTable.insert(pair<attr_t, uint32_t>(attr_t(sval), addr));
			memset(sval, 0, ATTR_SIZE_MAX + 1);
		}
	}
}

void TreeIndexFile::dump()
{
	for (TreeIndexTable::iterator it = mTreeIndexTable.begin(); it != mTreeIndexTable.end(); it++)
	{
		cout << it->first << " -> " << it->second << endl;
	}
}

void TreeIndexFile::merge_eq(
	const TreeIndexFile & a,
	const TreeIndexFile & b, 
	std::vector<AddrPair>& match_pairs)
{
	auto ait = a.mTreeIndexTable.begin();
	auto bit = b.mTreeIndexTable.begin();
	auto a_end = a.mTreeIndexTable.end();
	auto b_end = b.mTreeIndexTable.end();

	while (ait != a_end && bit != b_end)
	{
		if (ait->first == bit->first)
		{
			auto temp_bit = bit;
			while (temp_bit != b_end && ait->first == temp_bit->first)
			{
				match_pairs.emplace_back(ait->second, temp_bit->second);
				temp_bit++;
			}
			ait++;
		}
		else if (ait->first < bit->first)
			ait++;
		else
			bit++;
	}	
}

void TreeIndexFile::merge_neq(const TreeIndexFile & a, const TreeIndexFile & b, std::vector<AddrPair>& match_pairs)
{
	for (auto it = a.mTreeIndexTable.begin(); it != a.mTreeIndexTable.end(); it++)
	{
		auto eq_range = b.mTreeIndexTable.equal_range(it->first);
		for (auto nit = b.mTreeIndexTable.begin(); nit != eq_range.first; nit++)
			match_pairs.emplace_back(it->second, nit->second);
		for (auto nit = eq_range.second; nit != b.mTreeIndexTable.end(); nit++)
			match_pairs.emplace_back(it->second, nit->second);
	}
}

void TreeIndexFile::merge_less(const TreeIndexFile & a, const TreeIndexFile & b, std::vector<AddrPair>& match_pairs)
{
#ifdef _OLD
	for (auto ait = a.mTreeIndexTable.begin(); ait != a.mTreeIndexTable.end(); ait++)
	{
		auto lowerbound = b.mTreeIndexTable.lower_bound(ait->first);
		for (auto bit = b.mTreeIndexTable.begin(); bit != lowerbound; bit++)
			match_pairs.emplace_back(ait->second, bit->second);
	}
#else
	auto ait = a.mTreeIndexTable.begin();
	auto bit = b.mTreeIndexTable.begin();
	auto a_end = a.mTreeIndexTable.end();
	auto b_end = b.mTreeIndexTable.end();

	while (ait != a_end && bit != b_end)
	{
		if (ait->first == bit->first)
		{
			bit++;
		}
		else if (ait->first < bit->first)
		{
			auto temp_bit = bit;
			while (temp_bit != b_end && ait->first < temp_bit->first)
			{
				match_pairs.emplace_back(ait->second, temp_bit->second);
				temp_bit++;
			}
			ait++;
		}
		else
			bit++;
	}
#endif
}

void TreeIndexFile::merge_large(const TreeIndexFile & a, const TreeIndexFile & b, std::vector<AddrPair>& match_pairs)
{
#ifdef _OLD
	for (auto ait = a.mTreeIndexTable.begin(); ait != a.mTreeIndexTable.end(); ait++)
	{
		auto upperbound = b.mTreeIndexTable.upper_bound(ait->first);
		for (auto bit = upperbound; bit != b.mTreeIndexTable.end(); bit++)
			match_pairs.emplace_back(ait->second, bit->second);
	}
#else

	auto ait = a.mTreeIndexTable.begin();
	auto bit = b.mTreeIndexTable.begin();
	auto a_end = a.mTreeIndexTable.end();
	auto b_end = b.mTreeIndexTable.end();

	while (ait != a_end && bit != b_end)
	{
		if (ait->first == bit->first)
		{
			auto temp_bit = b.mTreeIndexTable.begin();
			while (temp_bit != bit && ait->first > temp_bit->first)
			{
				match_pairs.emplace_back(ait->second, temp_bit->second);
				temp_bit++;
			}
			ait++;
		}
		else if (ait->first < bit->first)
		{
			ait++;
		}
		else
			bit++;
	}
#endif
}

void IndexFile::write_back_pair(const void *src, uint32_t addr)
{
	fwrite(src, mKeysize, 1, mFile);
	fwrite(&addr, sizeof(uint32_t), 1, mFile);
}

void IndexFile::write_back_pair(int ival, uint32_t addr)
{
	write_back_pair(&ival, addr);
}

bool IndexFile::read_from_pair(void *dst, uint32_t * addr_dst)
{
	if (fread(dst, mKeysize, 1, mFile) == 0)
		return false;
	if (fread(addr_dst, sizeof(uint32_t), 1, mFile) == 0)
		return false;
	return true;
}
