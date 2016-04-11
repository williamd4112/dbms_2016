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
	inline void create(const char *, unsigned int attrNum, table_attr_desc_t *, int);
	inline void insert(void *);
	inline bool select_show(const char **, unsigned int);
	inline bool select_copy(const char **, unsigned int, void *, size_t);

	void save_table();
	void save_record();
	void save_freemap();
private:
	TableFile mTableFile;
	RecordFile<PAGESIZE> mRecordFile;
	BitmapPageFreeMapFile<PAGESIZE> mFreemapFile;

	inline void open_all(const char *, const char *);
	inline void print_record(table_attr_desc_t **, unsigned int, const unsigned char *);
	inline bool check_duplicated(const void *src);
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
#ifndef NDEBUG
	printf("Table loaded:\n");
	mTableFile.dump_info();
#endif

	mRecordFile.init(mTableFile.get_row_size());
}

template<unsigned int PAGESIZE>
inline void RecordTable<PAGESIZE>::create(const char *tablename, unsigned int attrNum, table_attr_desc_t *pDescs, int primaryKeyIndex)
{
	open_all(tablename, "wb+");
	mTableFile.init(tablename, attrNum, pDescs, primaryKeyIndex);
	mRecordFile.init(mTableFile.get_row_size());
}

template<unsigned int PAGESIZE>
inline void RecordTable<PAGESIZE>::insert(void * src)
{
	if (check_duplicated(src))
	{
		printf("RecordTable::insert(): insert failed, duplicate record\n");
		return;
	}

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

/*
	select
	
	pass column names, column number, print the selected column in table

	return false, failed
			true, ok
*/
template<unsigned int PAGESIZE>
inline bool RecordTable<PAGESIZE>::select_show(const char **pColDescs, unsigned int colNum)
{
	unsigned int maxPageID = mFreemapFile.get_max_page_id();
	
	table_attr_desc_t **pDescs = new table_attr_desc_t*[colNum];
	if (!mTableFile.get_attr_descs(pColDescs, colNum, pDescs))
		return false;

	for (int i = 0; i < colNum; i++)
	{
		printf("%s\t",pColDescs[i]);
	}
	putchar('\n');

	for (int i = 0; i <= maxPageID; i++)
	{
		const DataPage<PAGESIZE> *page = mRecordFile.get_data_page(i);
		for (int j = 0; j < page->get_row_count(); j++)
		{
			print_record(pDescs, colNum, page->get_data_row(j));
		}
	}
	delete [] pDescs;

	return true;
}

template<unsigned int PAGESIZE>
inline bool RecordTable<PAGESIZE>::select_copy(const char **pColDescs, unsigned int colNum, void *dst, size_t dstSize)
{
	//unsigned int maxPageID = mFreemapFile.get_max_page_id();

	//table_attr_desc_t **pDescs = new table_attr_desc_t*[colNum];
	//if (!mTableFile.get_attr_descs(pColDescs, colNum, pDescs))
	//	return false;

	//for (int i = 0; i <= maxPageID; i++)
	//{
	//	const DataPage<PAGESIZE> *page = mRecordFile.get_data_page(i);
	//	for (int j = 0; j < page->get_row_count(); j++)
	//	{
	//	
	//	}
	//}
	//delete[] pDescs;

	return true;
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

template<unsigned int PAGESIZE>
inline void RecordTable<PAGESIZE>::print_record(table_attr_desc_t **pDesc, unsigned int descNum, const unsigned char *src)
{
	int int_val;
	char varchar_val[ATTR_SIZE_MAX];
	for (int i = 0; i < descNum; i++)
	{
		if (pDesc[i]->type == ATTR_TYPE_INTEGER)
		{
			memcpy(&int_val, &src[pDesc[i]->offset], pDesc[i]->size);
			printf("%d\t",int_val);
		}
		else if (pDesc[i]->type == ATTR_TYPE_VARCHAR)
		{
			memcpy(varchar_val, &src[pDesc[i]->offset], pDesc[i]->size);
			printf("%s\t",varchar_val);
		}
		else
		{
			printf("NULL");
		}
	}
	putchar('\n');
}

template<unsigned int PAGESIZE>
inline bool RecordTable<PAGESIZE>::check_duplicated(const void * src)
{
	// Check Duplicate
	int pkIndex = mTableFile.get_table_header().primaryKeyIndex;
	bool isDuplicate;
	if (pkIndex < 0)
	{
		// When no primary key
		mRecordFile.find_record(src, mFreemapFile.get_max_page_id(), &isDuplicate);
	}
	else
	{
		// Get field value from srcc
		const table_attr_desc_t *desc = mTableFile.get_attr_desc(pkIndex);
		const unsigned char *col_src = (const unsigned char *)src;
		mRecordFile.find_record_with_col(col_src + desc->offset, 
			mFreemapFile.get_max_page_id(), 
			desc->offset, 
			desc->size, 
			&isDuplicate);
		
	}
	return isDuplicate;
}
