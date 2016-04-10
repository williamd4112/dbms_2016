#pragma once

#include "RecordFile.h"
#include "BitmapPageFreeMapFile.h"
#include "TableFile.h"

template <unsigned int PAGESIZE>
class RecordTable
{
public:
	RecordTable();
	~RecordTable();

	inline void load(const char *);
	inline void create(const char *, unsigned int attrNum, table_attr_desc_t *);
	inline void insert(void *);
	inline void select(const char **, unsigned int);

	void save_table();
	void save_record();
	void save_freemap();
private:
	TableFile mTableFile;
	RecordFile<PAGESIZE> mRecordFile;
	BitmapPageFreeMapFile<PAGESIZE> mFreemapFile;

	inline void open_all(const char *, const char *);
};

template<unsigned int PAGESIZE>
inline RecordTable<PAGESIZE>::RecordTable()
{
}

template<unsigned int PAGESIZE>
inline RecordTable<PAGESIZE>::~RecordTable()
{

}

template<unsigned int PAGESIZE>
inline void RecordTable<PAGESIZE>::load(const char *tablename)
{
	open_all(tablename, "rb+");
}

template<unsigned int PAGESIZE>
inline void RecordTable<PAGESIZE>::create(const char *tablename, unsigned int attrNum, table_attr_desc_t *pDescs)
{
	open_all(tablename, "wb+");
	mTableFile.init(tablename, attrNum, pDescs);
	mRecordFile.init(mTableFile.get_row_size());
}

template<unsigned int PAGESIZE>
inline void RecordTable<PAGESIZE>::insert(void * src)
{
	try
	{
		unsigned int free_page_id = mFreemapFile.get_free_page();
		
		unsigned char result;

		// TODO: make use the addr in index
		unsigned int addr = mRecordFile.put_record(free_page_id, src, &result);
		
#ifndef NDEBUG
		if (result & BIT_SUCCESS)
		{
			printf("RecordTable::insert(): insert success.\n");
		}
		else
		{
			printf("RecordTable::insert(): insert fail.\n");
		}
#endif

		if (result & BIT_PUT_FULL)
		{
			mFreemapFile.set_page_full(free_page_id);
		}
	}
	catch (int e)
	{
		fprintf(stderr, "RecordTable::insert(): no enough space.\n");
	}
	
}

template<unsigned int PAGESIZE>
inline void RecordTable<PAGESIZE>::select(const char **pColDescs, unsigned int colNum)
{
	unsigned int maxPageID = mFreemapFile.get_max_page_id();
	
	for (int i = 0; i <= maxPageID; i++)
	{

	}
}

template<unsigned int PAGESIZE>
inline void RecordTable<PAGESIZE>::save_table()
{
	mTableFile.write_back();
}

template<unsigned int PAGESIZE>
inline void RecordTable<PAGESIZE>::save_record()
{
	mRecordFile.write_back();
}

template<unsigned int PAGESIZE>
inline void RecordTable<PAGESIZE>::save_freemap()
{
	mFreemapFile.write_back();
}

template<unsigned int PAGESIZE>
inline void RecordTable<PAGESIZE>::open_all(const char *tablename, const char *mode)
{
	std::string tablename_str(tablename);
	std::string tbl_path = tablename_str + ".tbl";
	std::string dat_path = tablename_str + ".dat";
	std::string fmp_path = tablename_str + ".fmp";

	mTableFile.open(tbl_path.c_str(), mode);
	mTableFile.read_from();

	mRecordFile.open(dat_path.c_str(), mode);
	mRecordFile.read_from();

	mFreemapFile.open(fmp_path.c_str(), mode);
	mFreemapFile.read_from();
}
