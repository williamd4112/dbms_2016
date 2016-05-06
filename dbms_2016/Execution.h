#pragma once

#include "database_type.h"
#include "database_util.h"

#include "DatabaseFile.h"
#include "SQLParser.h"
#include "RecordTable.h"

#define NO_ERROR 0x0
#define TABLE_NO_FOUND 0x1
#define TABLE_DUPLICATE 0x2
#define ALIAS_DUPLICATE 0x3

template <uint32_t PAGESIZE = PAGESIZE_8K>
class From;

template <uint32_t PAGESIZE = PAGESIZE_8K>
class Select
{
public:
	Select(std::vector<sql::Expr*> & select_list);
	Select(std::vector<sql::AggregationFunction*> & aggreation_list);
	~Select();
private:
	struct SelectEntry
	{
		RecordTable<PAGESIZE> *table_ptr;
		table_attr_desc_t *attr_desc;
		std::string attr_name;
		std::string attr_alias;
	};

	std::vector<SelectEntry> mSelectEntries;
};

template <uint32_t PAGESIZE = PAGESIZE_8K>
class Where
{
public:
	Where(From<PAGESIZE> &from, sql::Expr * where_clause);
	~Where();

private:
	std::vector<uint32_t> mSpecAddrs;
};

template <uint32_t PAGESIZE = PAGESIZE_8K>
class From
{
	typedef std::pair<std::string, int> NameTableEntry;

	friend class Where<PAGESIZE>;
	friend class Select<PAGESIZE>;
public:
	From(DatabaseFile<PAGESIZE> &db, std::vector<sql::TableRef*>* from_clause);
	~From();
private:
	DatabaseFile<PAGESIZE> &mDbf;

	std::vector<RecordTable<PAGESIZE> *> mSpecTables;
	std::unordered_map<std::string, int> mNameTable;
};

template<uint32_t PAGESIZE>
inline From<PAGESIZE>::From(DatabaseFile<PAGESIZE>& db, std::vector<sql::TableRef*>* from_clause)
	: mDbf(db)
{
	// Pre allocate
	mSpecTables.resize(from_clause->size());

	// Parsing FROM clause
	for (int i = 0; i < from_clause->size(); i++)
	{
		// Parse table name from clause, and try to get table from database
		sql::TableRef *ref = from_clause->at(i);
		mSpecTables[i] = mDbf.get_table(ref->name);
		if (mSpecTables[i] == NULL)
			throw exception_t(TABLE_NO_FOUND, ref->name);

		// if duplicated table exist, insertion failed
		auto result = mNameTable.insert(NameTableEntry(ref->name, i));
		if (!result.second)
			throw exception_t(TABLE_DUPLICATE, ref->name);

		// also, if there is alias, put alias at reference map
		if (ref->alias != NULL)
		{
			result = mNameTable.insert(NameTableEntry(ref->alias, i));
			if (!result.second)
				throw exception_t(ALIAS_DUPLICATE, ref->alias);
		}
	}
}

template<uint32_t PAGESIZE>
inline From<PAGESIZE>::~From()
{
}

template<uint32_t PAGESIZE>
inline Where<PAGESIZE>::Where(From<PAGESIZE>& from, sql::Expr * where_clause)
{
}

template<uint32_t PAGESIZE>
inline Where<PAGESIZE>::~Where()
{
}
