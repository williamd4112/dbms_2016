#pragma once

#include "DiskFile.h"
#include "database_type.h"
#include <cassert>
#include <iostream>

enum IndexExceptionType
{
	INDEX_NOT_FOUND
};

enum IndexType
{
	HASH = 0, 
	TREE = 1, 
	PHASH = 2, 
	PTREE = 3
};

struct IndexException
{
	IndexExceptionType type;
	std::string msg;

	IndexException(){}
	IndexException(IndexExceptionType _type, std::string _msg) : type(_type), msg(_msg){}
};

typedef std::unordered_multimap<attr_t, uint32_t, attr_t_hash> HashIndexTable;
typedef std::multimap<attr_t, uint32_t> TreeIndexTable;
typedef std::unordered_map<attr_t, uint32_t, attr_t_hash> PrimaryIndexTable;

class IndexFile
	: public DiskFile
{
public:
	IndexFile(attr_domain_t keydomain, uint32_t keysize, IndexType index_type);
	~IndexFile();

	virtual bool set(const attr_t &attr_ref, const uint32_t record_addr) = 0;
	virtual uint32_t get(const attr_t &attr_ref, std::vector<uint32_t> &match_addrs) = 0;

	const IndexType type() const { return mType; }
protected:
	IndexType mType;
	attr_domain_t mKeydomain;
	uint32_t mKeysize;

};

class HashIndexFile
	: public IndexFile
{
public:
	HashIndexFile(attr_domain_t keydomain, uint32_t keysize);
	~HashIndexFile();

	bool set(const attr_t &attr_ref, const uint32_t record_addr);
	uint32_t get(const attr_t &attr_ref, std::vector<uint32_t> &match_addrs);

	void write_back();
	void read_from();

	void dump();
private:
	HashIndexTable mHashIndexTable;
};

class TreeIndexFile
	: public IndexFile
{
public:
	TreeIndexFile(attr_domain_t keydomain, uint32_t keysize);
	~TreeIndexFile();

	bool set(const attr_t &attr_ref, const uint32_t record_addr);
	uint32_t get(const attr_t &attr_ref, std::vector<uint32_t> &match_addrs);
	uint32_t get(const attr_t &attr_lower, const attr_t &attr_upper, std::vector<uint32_t> &match_addrs);
	uint32_t get(const attr_t &attr_pivot, FindType find_type, std::vector<uint32_t> &match_addrs);

	void write_back();
	void read_from();

	void dump();
private:
	TreeIndexTable mTreeIndexTable;
};

class PrimaryIndexFile
	: public IndexFile
{
public:
	PrimaryIndexFile(attr_domain_t keydomain, uint32_t keysize);
	~PrimaryIndexFile();

	bool set(const attr_t &attr_ref, const uint32_t record_addr);
	uint32_t get(const attr_t &attr_ref, std::vector<uint32_t> &match_addrs);
	bool get_primary(const attr_t &attr_ref, uint32_t *match_addr);
	bool isExist(const attr_t &attr_ref);

	void write_back();
	void read_from();

	void dump();
private:
	PrimaryIndexTable mPrimaryIndexTable;
};

