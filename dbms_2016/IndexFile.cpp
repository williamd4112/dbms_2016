#include "IndexFile.h"

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
	return mHashIndexTable.insert(pair<attr_t, uint32_t>(attr_ref, record_addr))->second;
}

bool HashIndexFile::get(const attr_t &attr_ref, std::vector<uint32_t> &match_addrs)
{
	auto result = mHashIndexTable.equal_range(attr_ref);
	if (result.first == result.second)
		return false;
	
	for (auto it = result.first; it != result.second; it++)
		match_addrs.push_back(it->second);
	return true;
}

void HashIndexFile::write_back()
{
	assert(mKeydomain != UNDEFINED_DOMAIN && mFile != NULL);
	fseek(mFile, 0, SEEK_SET);

	for (HashIndexTable::iterator it = mHashIndexTable.begin(); it != mHashIndexTable.end(); it++)
	{
		if (mKeydomain == INTEGER_DOMAIN)
			fprintf(mFile, "%d\t%u\n", it->first.Int(), it->second);
		else if (mKeydomain == VARCHAR_DOMAIN)
			fprintf(mFile, "%s\t%u\n", it->first.Varchar(), it->second);
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
		while (fscanf(mFile, "%d%u", &ival, &addr) == 2)
		{
			mHashIndexTable.insert(pair<attr_t, uint32_t>(attr_t(ival), addr));
		}
	}
	else if (mKeydomain == VARCHAR_DOMAIN)
	{
		char sval[ATTR_SIZE_MAX + 1];
		while (fscanf(mFile, "%s%u", sval, &addr) == 2)
		{
			mHashIndexTable.insert(pair<attr_t, uint32_t>(attr_t(sval), addr));
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

bool PrimaryIndexFile::get(const attr_t & attr_ref, std::vector<uint32_t>& match_addrs)
{
	auto res = mPrimaryIndexTable.find(attr_ref);
	if (res != mPrimaryIndexTable.end())
	{
		match_addrs.push_back(res->second);
		return true;
	}
	return false;
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

void PrimaryIndexFile::write_back()
{
	assert(mKeydomain != UNDEFINED_DOMAIN && mFile != NULL);
	fseek(mFile, 0, SEEK_SET);

	for (HashIndexTable::iterator it = mPrimaryIndexTable.begin(); it != mPrimaryIndexTable.end(); it++)
	{
		if (mKeydomain == INTEGER_DOMAIN)
			fprintf(mFile, "%d\t%u\n", it->first.Int(), it->second);
		else if (mKeydomain == VARCHAR_DOMAIN)
			fprintf(mFile, "%s\t%u\n", it->first.Varchar(), it->second);
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
		while (fscanf(mFile, "%d%u", &ival, &addr) == 2)
		{
			mPrimaryIndexTable.insert(pair<attr_t, uint32_t>(attr_t(ival), addr));
		}
	}
	else if (mKeydomain == VARCHAR_DOMAIN)
	{
		char sval[ATTR_SIZE_MAX + 1];
		while (fscanf(mFile, "%s%u", sval, &addr) == 2)
		{
			mPrimaryIndexTable.insert(pair<attr_t, uint32_t>(attr_t(sval), addr));
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
	return mTreeIndexTable.insert(pair<attr_t, uint32_t>(attr_ref, record_addr))->second;
}

bool TreeIndexFile::get(const attr_t & attr_ref, std::vector<uint32_t>& match_addrs)
{
	auto result = mTreeIndexTable.equal_range(attr_ref);
	if (result.first == result.second)
		return false;

	for (auto it = result.first; it != result.second; it++)
		match_addrs.push_back(it->second);
	return true;
}

bool TreeIndexFile::get(const attr_t & attr_lower, const attr_t & attr_upper, std::vector<uint32_t>& match_addrs)
{
	TreeIndexTable::iterator begin = mTreeIndexTable.lower_bound(attr_lower);
	TreeIndexTable::iterator end = mTreeIndexTable.upper_bound(attr_upper);
	if (begin == end)
		return false;

	for (auto it = begin; it != end; it++)
		match_addrs.push_back(it->second);
	return true;
}

void TreeIndexFile::write_back()
{
	assert(mKeydomain != UNDEFINED_DOMAIN && mFile != NULL);
	fseek(mFile, 0, SEEK_SET);

	for (TreeIndexTable::iterator it = mTreeIndexTable.begin(); it != mTreeIndexTable.end(); it++)
	{
		if (mKeydomain == INTEGER_DOMAIN)
			fprintf(mFile, "%d\t%u\n", it->first.Int(), it->second);
		else if (mKeydomain == VARCHAR_DOMAIN)
			fprintf(mFile, "%s\t%u\n", it->first.Varchar(), it->second);
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
		while (fscanf(mFile, "%d%u", &ival, &addr) == 2)
		{
			mTreeIndexTable.insert(pair<attr_t, uint32_t>(attr_t(ival), addr));
		}
	}
	else if (mKeydomain == VARCHAR_DOMAIN)
	{
		char sval[ATTR_SIZE_MAX + 1];
		while (fscanf(mFile, "%s%u", sval, &addr) == 2)
		{
			mTreeIndexTable.insert(pair<attr_t, uint32_t>(attr_t(sval), addr));
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
