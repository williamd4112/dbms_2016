#pragma once

#include <stdint.h>
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <vector>
#include <stack>
#include <iostream>
#include <cassert>

#define ATTR_NUM_MAX 5
#define ATTR_NAME_MAX 45
#define TABLE_NAME_MAX ATTR_NAME_MAX
#define ATTR_SIZE_MAX ATTR_NAME_MAX

#define ATTR_TYPE_UNDEFINED 0x0
#define ATTR_TYPE_INTEGER 0x1
#define ATTR_TYPE_VARCHAR 0x2

// Star type for QueryExecution aggregation
#define ATTR_TYPE_STAR 0x4 

#define ATTR_CONSTRAINT_NO 0x0
#define ATTR_CONSTRAINT_PRIMARY_KEY 0x1

#define ROW_SIZE_MAX (ATTR_NUM_MAX * ATTR_SIZE_MAX)

#define INDEX_FILENAME_MAX 80
#define INDEX_RECORD_SIZE_MAX 255
#define UNKNOWN_RELATION_TYPE 0x81

extern const char *kAttrTypeNames[];

template <class T, class HashType>
using HashSet = std::unordered_set<T, HashType>;

template <class K, class V>
using HashMap = std::unordered_map<K, V>;

struct c_unique {
	int current;
	c_unique();
	int operator()();
};

enum attr_exception_t
{
	ATTR_UNSUPPORTED_OP
};

enum unary_op_type_t
{
	POS, NEG
};

enum relation_type_t
{
	EQ, NEQ, LESS, LARGE
};

enum merge_type_t
{
	AND, OR
};

enum attr_domain_t
{
	INTEGER_DOMAIN,
	VARCHAR_DOMAIN,
	UNDEFINED_DOMAIN
};

unary_op_type_t operator -(const unary_op_type_t u);

unary_op_type_t operator *(const unary_op_type_t a, const unary_op_type_t b);

struct attr_t
{
	union attr_value_t
	{
		int integer;
		char varchar[ATTR_SIZE_MAX];
		attr_value_t(int _val) : integer(_val) {}
		attr_value_t(const char *_str) : varchar{ 0 }
		{
#ifdef _STRDUP_VER_
			varchar = _strdup(_str); 
#else
			strncpy(varchar, _str, ATTR_SIZE_MAX);
#endif
		}

		attr_value_t() : varchar{0} {}
		~attr_value_t() {}

		attr_value_t& operator=(int _val)
		{
			integer = _val;
			return *this;
		}

		attr_value_t& operator=(const char *_val)
		{
#ifdef _STRDUP_VER_
			varchar = _strdup(_str);
#else
			strncpy(varchar, _val, ATTR_SIZE_MAX);
#endif
			return *this;
		}
	};

public:
	attr_t(int _val) : value(_val), domain(INTEGER_DOMAIN) {}

	attr_t(const char *_str) : value(_str), domain(VARCHAR_DOMAIN) {}

	attr_t(const attr_t& _attr) : domain(_attr.domain)
	{
		if (_attr.domain == INTEGER_DOMAIN) value = _attr.value.integer;
		else if (_attr.domain == VARCHAR_DOMAIN) value = _attr.value.varchar;
	}

	attr_t() : domain(UNDEFINED_DOMAIN) {}

	~attr_t() {}

	inline size_t size()
	{
		return (domain == INTEGER_DOMAIN) ? sizeof(int) :
			(domain == VARCHAR_DOMAIN) ? strlen(value.varchar) :
			0;
	}

	inline void init_as(attr_domain_t _domain)
	{
		domain = _domain;
	}

	inline attr_domain_t Domain() const { return domain; }
	inline int Int() const { return value.integer; }
	inline const char *Varchar() const { return value.varchar; }

	attr_t &operator=(const attr_t& _attr)
	{
		domain = _attr.domain;
		if (domain == INTEGER_DOMAIN) value = _attr.value.integer;
		else if (_attr.domain == VARCHAR_DOMAIN) value = _attr.value.varchar;
		return *this;
	}

	attr_t &operator=(int _val) { domain = INTEGER_DOMAIN; value = _val; return (*this); }
	attr_t &operator=(const char *_val)
	{
#ifdef _STRDUP_VER_
		if (domain == VARCHAR_DOMAIN)
			delete value.varchar;
#endif
		domain = VARCHAR_DOMAIN;
		value = _val;
		return (*this);
	}

	friend std::ostream& operator <<(std::ostream& os, const attr_t &attr)
	{
		switch (attr.domain)
		{
		case INTEGER_DOMAIN:
			return os << attr.value.integer;
		case VARCHAR_DOMAIN:
			return os << ((attr.value.varchar[0] != '\0') ? attr.value.varchar : "NULL");
		default:
			return os << "null";
		}
	}

	friend bool operator <(const attr_t &a, const attr_t &b)
	{
		assert(a.Domain() == b.Domain());
		if (a.Domain() == INTEGER_DOMAIN) return a.Int() < b.Int();
		else if (a.Domain() == VARCHAR_DOMAIN) return strncmp(a.Varchar(), b.Varchar(), ATTR_NUM_MAX) < 0;
		else return false;
	}

	friend bool operator >(const attr_t &a, const attr_t &b)
	{
		assert(a.Domain() == b.Domain());
		if (a.Domain() == INTEGER_DOMAIN) return a.Int() > b.Int();
		else if (a.Domain() == VARCHAR_DOMAIN) return strncmp(a.Varchar(), b.Varchar(), ATTR_NUM_MAX) > 0;
		else return false;
	}

	inline friend bool operator==(const attr_t &a, const attr_t &b)
	{
		if (a.Domain() != b.Domain()) return false;
		switch (a.domain)
		{
		case INTEGER_DOMAIN:
			return a.value.integer == b.value.integer;
		case VARCHAR_DOMAIN:
			return strncmp(a.value.varchar, b.value.varchar, ATTR_SIZE_MAX) == 0;
		default: // NULL TYPE
			return true;
		}
	}

	inline friend bool operator!=(const attr_t &a, const attr_t &b)
	{
		return !(a == b);
	}

	inline attr_t operator -()
	{
		if (domain != INTEGER_DOMAIN)
			throw ATTR_UNSUPPORTED_OP;
		return attr_t(-value.integer);
	}
	
	inline attr_t operator *(const unary_op_type_t op)
	{
		if (domain != INTEGER_DOMAIN)
			throw ATTR_UNSUPPORTED_OP;
		return (op == POS) ? attr_t(value.integer) : attr_t(-value.integer);
	}
private:
	attr_domain_t domain;
	attr_value_t value;
};

struct attr_t_hash {
	size_t operator() (const attr_t &attr) const {
		assert(attr.Domain() != UNDEFINED_DOMAIN);
		if (attr.Domain() == INTEGER_DOMAIN)
			return std::hash<int>{}(attr.Int());
		else
			return std::hash<std::string>{}(attr.Varchar());
	}
};

typedef std::vector<attr_t> AttrTuple;
typedef std::vector<AttrTuple>::iterator AttrTupleIterator;
typedef std::pair<uint32_t, uint32_t> AddrPair;

struct addr_pair_hash {
	size_t operator() (const AddrPair &addr_pair) const {
		return std::hash<uint32_t>{}(addr_pair.first) ^ std::hash<uint32_t>{}(addr_pair.second);
	}
};


