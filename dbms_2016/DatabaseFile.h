#pragma once

#include "DiskFile.h"
#include "RecordTable.h"
#include "system.h"
#include <iostream>
#include <unordered_map>

/*
	DatabaseFile

	a file stores all existed table name in disk.
	after loaded into memory, build a hash table to look up <name, table *> quickly
*/
template <unsigned int PAGESIZE>
class DatabaseFile
	: public DiskFile
{
#define insert_to_table(name, table) mTableHashTable.insert(std::pair<std::string, RecordTable<PAGESIZE> *>((name), (table)))
	typedef std::unordered_map<std::string, RecordTable<PAGESIZE> *> TableHashTable;
public:
	DatabaseFile();
	~DatabaseFile();

	bool create_table(const char *, table_attr_desc_t *, unsigned int, unsigned int);
	bool create_table(const char *, table_attr_desc_t *, unsigned int);
	bool create_table(const char *, std::vector<sql::ColumnDefinition*>&);
	RecordTable<PAGESIZE> *get_table(const char *);

	void write_back();
	void read_from();

private:
	TableHashTable mTableHashTable;

	RecordTable<PAGESIZE> *allocate_table();
	
};

template<unsigned int PAGESIZE>
inline DatabaseFile<PAGESIZE>::DatabaseFile()
{
}

template<unsigned int PAGESIZE>
inline DatabaseFile<PAGESIZE>::~DatabaseFile()
{
	for (TableHashTable::iterator it = mTableHashTable.begin(); it != mTableHashTable.end(); it++)
	{
		it->second->save_table();
		it->second->save_freemap();
		it->second->save_record();
		delete it->second;
	}
}

template<unsigned int PAGESIZE>
inline bool DatabaseFile<PAGESIZE>::create_table(const char *tablename, table_attr_desc_t *pDescs, unsigned int attrNum, unsigned int pkIndex)
{
	RecordTable<PAGESIZE> *rt = allocate_table();
	rt->create(tablename, attrNum, pDescs, pkIndex);
	auto result = insert_to_table(tablename, rt);
	
	if (!result.second)
	{
		fprintf(stderr, "DatabaseFile::create_table(): duplicated table, %s\n",tablename);
	}

	return result.second;
}

template<unsigned int PAGESIZE>
inline bool DatabaseFile<PAGESIZE>::create_table(const char *, table_attr_desc_t *, unsigned int)
{
	RecordTable<PAGESIZE> *rt = allocate_table();
	rt->create(tablename, attrNum, pDescs);
	auto result = insert_to_table(tablename, rt);

	if (!result.second)
	{
		fprintf(stderr, "DatabaseFile::create_table(): duplicated table, %s\n", tablename);
	}

	return result.second;
}

template<unsigned int PAGESIZE>
inline bool DatabaseFile<PAGESIZE>::create_table(const char *tablename, std::vector<sql::ColumnDefinition*>& col_defs)
{
	RecordTable<PAGESIZE> *rt = allocate_table();
	auto result = insert_to_table(tablename, rt);

	if (!result.second)
	{
		fprintf(stderr, "DatabaseFile::create_table(): duplicated table, %s\n", tablename);
	}
	rt->create(tablename, col_defs);
	return result.second;
}

template<unsigned int PAGESIZE>
inline RecordTable<PAGESIZE> * DatabaseFile<PAGESIZE>::get_table(const char *name)
{
	TableHashTable::iterator it = mTableHashTable.find(name);
	if (it != mTableHashTable.end())
	{
		return it->second;
	}
	return NULL;
}

template<unsigned int PAGESIZE>
inline void DatabaseFile<PAGESIZE>::write_back()
{
	fseek(mFile, 0, SEEK_SET);
	for (TableHashTable::iterator it = mTableHashTable.begin(); it != mTableHashTable.end(); it++)
	{
		fprintf(mFile, "%s\n", it->first.c_str());
	}
}

template<unsigned int PAGESIZE>
inline void DatabaseFile<PAGESIZE>::read_from()
{
	char buff[TABLE_NAME_MAX + 1];
	while (fscanf(mFile, "%s", buff) == 1)
	{
		RecordTable<PAGESIZE> *pTable = allocate_table();
		pTable->load(buff);
		
		auto result = insert_to_table(buff, pTable);
		if (!result.second)
		{
			fprintf(stderr, "DatabaseFile::read_from(): corrupted database file, duplicated table name inside\n");
			fatal_error();
		}
	}
}

template<unsigned int PAGESIZE>
inline RecordTable<PAGESIZE>* DatabaseFile<PAGESIZE>::allocate_table()
{
	RecordTable<PAGESIZE> *pTable = new RecordTable<PAGESIZE>;
	if (pTable == NULL)
	{
		fprintf(stderr, "DatabaseFile::read_from(): out of memmory.\n");
		fatal_error();
	}
	return pTable;
}

