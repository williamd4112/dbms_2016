#pragma once

#include "DiskFile.h"
#include "database_type.h"
#include <cassert>
#include <iostream>

#define INDEX_UNKOWN_RELATION_TYPE 0x1

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
	virtual uint32_t get(const attr_t &attr_ref, std::vector<uint32_t> &match_addrs) = 0; // Filter
	virtual uint32_t get(const attr_t &attr_ref, std::vector<AddrPair> &match_pairs) = 0; // Reflexive
	virtual uint32_t get(const attr_t &attr_ref, const uint32_t fix_addr, std::vector<AddrPair> &match_pairs) = 0; // Cross filter

	virtual uint32_t get_not(const attr_t &attr_ref, std::vector<uint32_t> &match_addrs) = 0;
	virtual uint32_t get_not(const attr_t &attr_ref, std::vector<AddrPair> &match_pairs) = 0; // Reflexive
	virtual uint32_t get_not(const attr_t &attr_ref, const uint32_t fix_addr, std::vector<AddrPair> &match_pairs) = 0;

	const IndexType type() const { return mType; }
	void write_back_pair(const void *src, uint32_t addr);
	void write_back_pair(int ival, uint32_t addr);
	bool read_from_pair(void *dst, uint32_t * addr_dst);
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
	uint32_t get(const attr_t &attr_ref, std::vector<AddrPair> &match_pairs);
	uint32_t get(const attr_t &attr_ref, const uint32_t fix_addr, std::vector<AddrPair> &match_pairs);

	uint32_t get_not(const attr_t &attr_ref, std::vector<uint32_t> &match_addrs);
	uint32_t get_not(const attr_t &attr_ref, std::vector<AddrPair> &match_pairs);
	uint32_t get_not(const attr_t &attr_ref, const uint32_t fix_addr, std::vector<AddrPair> &match_pairs);

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
	uint32_t get(const attr_t &attr_ref, std::vector<AddrPair> &match_pairs);
	uint32_t get(const attr_t &attr_ref, const relation_type_t rel_type, std::vector<uint32_t> &match_addrs);
	uint32_t get(const attr_t &attr_ref, const uint32_t fix_addr, std::vector<AddrPair> &match_pairs);

	uint32_t get_not(const attr_t &attr_ref, std::vector<uint32_t> &match_addrs);
	uint32_t get_not(const attr_t &attr_ref, std::vector<AddrPair> &match_pairs);
	uint32_t get_not(const attr_t &attr_ref, const uint32_t fix_addr, std::vector<AddrPair> &match_pairs);

	uint32_t get_less(const attr_t &attr_ref, std::vector<uint32_t> &match_addrs);
	uint32_t get_less(const attr_t &attr_ref, std::vector<AddrPair> &match_pairs);
	uint32_t get_less(const attr_t &attr_ref, const uint32_t fix_addr, std::vector<AddrPair> &match_pairs);

	uint32_t get_large(const attr_t &attr_ref, std::vector<uint32_t> &match_addrs);
	uint32_t get_large(const attr_t &attr_ref, const uint32_t fix_addr, std::vector<AddrPair> &match_pairs);
	uint32_t get_large(const attr_t &attr_ref, std::vector<AddrPair> &match_pairs);

	void write_back();
	void read_from();

	void dump();

	static void merge_eq(const TreeIndexFile &a, const TreeIndexFile &b, std::vector<AddrPair> &match_pairs);
	static void merge_neq(const TreeIndexFile &a, const TreeIndexFile &b, std::vector<AddrPair> &match_pairs);
	static void merge_less(const TreeIndexFile &a, const TreeIndexFile &b, std::vector<AddrPair> &match_pairs);
	static void merge_large(const TreeIndexFile &a, const TreeIndexFile &b, std::vector<AddrPair> &match_pairs);
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
	uint32_t get(const attr_t &attr_ref, std::vector<AddrPair> &match_pairs);
	uint32_t get(const attr_t &attr_ref, const uint32_t fix_addr, std::vector<AddrPair> &match_pairs);

	uint32_t get_not(const attr_t &attr_ref, std::vector<uint32_t> &match_addrs);
	uint32_t get_not(const attr_t &attr_ref, std::vector<AddrPair> &match_pairs);
	uint32_t get_not(const attr_t &attr_ref, const uint32_t fix_addr, std::vector<AddrPair> &match_pairs);

	bool get_primary(const attr_t &attr_ref, uint32_t *match_addr);
	bool isExist(const attr_t &attr_ref);

	void write_back();
	void read_from();

	void dump();
private:
	PrimaryIndexTable mPrimaryIndexTable;
};

