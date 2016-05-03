#pragma once

#include "RecordFile.h"
#include "BitmapPageFreeMapFile.h"
#include "TableFile.h"
#include "system.h"

#define FAST_ITERATOR_ERROR_COL -1

enum RecordTableException 
{
	NO_EXCEPTION,
	BAD_ADDR,
	DUPLICATED_RECORD
};



template <unsigned int PAGESIZE>
class RecordTable
{
public:
	/*
		fast_iterator

		do not insert anything within one iteration
	*/
	struct fast_iterator
	{
	public:
		fast_iterator(RecordTable*);
		~fast_iterator();
		
		unsigned char *next();
		unsigned char *next(unsigned int *pAddr);
	private:
		RecordTable *table;

		unsigned int page_id;
		unsigned int row_id;
		DataPage<PAGESIZE> *cur_page;
	};

	RecordTable();
	~RecordTable();

	inline void load(const char *);
	inline void create(const char *, unsigned int attrNum, table_attr_desc_t *, int);
	inline void create(const char *, unsigned int attrNum, table_attr_desc_t *);
	inline void create(const char *, std::vector<sql::ColumnDefinition*>&);

	inline bool insert(void *);
	inline bool select_show(const char **, unsigned int);
	
	BitmapPageFreeMapFile<PAGESIZE> &freemap();
	RecordFile<PAGESIZE> &records();
	TableFile &tablefile() { return mTableFile; }

	inline unsigned int get_row_size();
	inline const char *get_name();
	inline RecordTableException get_error();
	inline int get_int(unsigned int, unsigned int col_offset);
	inline const char *get_varchar(unsigned int, unsigned int col_offset, char *dst);
	inline unsigned char *get_row(unsigned int);
	inline void print_record(table_attr_desc_t **, unsigned int, const unsigned char *);
	
	void save_table();
	void save_record();
	void save_freemap();

	static const char *get_error_msg(RecordTableException e);
	const char *get_error_msg();

	void dump_info();
	void dump_content();
private:
	TableFile mTableFile;
	RecordFile<PAGESIZE> mRecordFile;
	BitmapPageFreeMapFile<PAGESIZE> mFreemapFile;

	/* Record last error exception, access only by get_error()*/
	RecordTableException mError;

	inline void open_all(const char *, const char *);
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

	// TODO: Load index if has
}

/*
	create

	@Deprecated: no passing primary key in new version
*/
template<unsigned int PAGESIZE>
inline void RecordTable<PAGESIZE>::create(const char *tablename, unsigned int attrNum, table_attr_desc_t *pDescs, int primaryKeyIndex)
{
	open_all(tablename, "wb+");
	mTableFile.init(tablename, attrNum, pDescs, primaryKeyIndex);
	mRecordFile.init(mTableFile.get_row_size());
}

template<unsigned int PAGESIZE>
inline void RecordTable<PAGESIZE>::create(const char *tablename, unsigned int attrNum, table_attr_desc_t *pDescs)
{
	open_all(tablename, "wb+");
	mTableFile.init(tablename, attrNum, pDescs);
	mRecordFile.init(mTableFile.get_row_size());
}

template<unsigned int PAGESIZE>
inline void RecordTable<PAGESIZE>::create(const char *tablename, std::vector<sql::ColumnDefinition*>& col_defs)
{
	open_all(tablename, "wb+");
	mTableFile.init(tablename, col_defs);
	mRecordFile.init(mTableFile.get_row_size());
}

/*
	insert

	src is a pointer to insertion data
	first, check if there is a duplicated record (for now, use exhaustive search)
	
*/
template<unsigned int PAGESIZE>
inline bool RecordTable<PAGESIZE>::insert(void * src)
{
	if (check_duplicated(src))
	{
		mError = DUPLICATED_RECORD;
		return false;
	}

	bool success = true;
	try
	{
		unsigned int free_page_id = mFreemapFile.get_free_page();
		
		unsigned char result;
		unsigned char mode = PAGEBUFFER_WRITE;
		
		if (!mFreemapFile.is_present(free_page_id))
			mode |= PAGEBUFFER_CREATE;

		// TODO: make use the addr in index
		unsigned int addr = mRecordFile.put_record(free_page_id, src, &result, mode);
		
		/// TODO: handling error better
#ifdef _DEBUG_INSERT
		if (result & BIT_SUCCESS)
		{

			printf("RecordTable::insert(): insert success.\n");
		}
		else
		{

			printf("RecordTable::insert(): insert fail.\n");
		}
#endif
		if(!(result & BIT_SUCCESS))
			success = false;

		mFreemapFile.set_page_present(free_page_id);

		// Mark current max page as full, so that next time calling get_free_page_id we can advance page id
		if (result & BIT_PUT_FULL)
		{
			mFreemapFile.set_page_full(free_page_id);
		}
	}
	catch (int e)
	{
		Error("RecordTable::insert(): no enough space.\n");
		success = false;
	}
	return success;
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
inline BitmapPageFreeMapFile<PAGESIZE>& RecordTable<PAGESIZE>::freemap()
{
	return mFreemapFile;
}

template<unsigned int PAGESIZE>
inline RecordFile<PAGESIZE>& RecordTable<PAGESIZE>::records() 
{
	return mRecordFile;
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
inline const char *RecordTable<PAGESIZE>::get_error_msg(RecordTableException e)
{
	switch (e)
	{
	case BAD_ADDR:
		return "Bad adddress";
	case DUPLICATED_RECORD:
		return "Duplicated record";
	default:
		return "No error";
		break;
	}
}

template<unsigned int PAGESIZE>
inline const char * RecordTable<PAGESIZE>::get_error_msg()
{
	return get_error_msg(get_error());
}

template<unsigned int PAGESIZE>
inline void RecordTable<PAGESIZE>::dump_info()
{
	mTableFile.dump_info();
}

template<unsigned int PAGESIZE>
inline void RecordTable<PAGESIZE>::dump_content()
{
	RecordTable<PAGESIZE>::fast_iterator it(this);
	
	table_attr_desc_t **pPDescs = new table_attr_desc_t*[mTableFile.get_table_header().attrNum];
	for (int i = 0; i < mTableFile.get_table_header().attrNum; i++)
	{
		pPDescs[i] = mTableFile.get_attr_desc(i);
	}

	unsigned char *record;
	while ((record = it.next()) != NULL)
	{
		print_record(pPDescs, mTableFile.get_table_header().attrNum, record);
	}
	
	delete [] pPDescs;
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
inline unsigned int RecordTable<PAGESIZE>::get_row_size()
{
	return mTableFile.get_row_size();
}

template<unsigned int PAGESIZE>
inline const char * RecordTable<PAGESIZE>::get_name()
{
	return mTableFile.get_table_header().name;
}

template<unsigned int PAGESIZE>
inline RecordTableException RecordTable<PAGESIZE>::get_error()
{
	RecordTableException e = mError;
	mError = NO_EXCEPTION;
	return e;
}

template<unsigned int PAGESIZE>
inline int RecordTable<PAGESIZE>::get_int(unsigned int addr, unsigned int col_offset)
{
	const DataPage<PAGESIZE> *page = mRecordFile.get_data_page(get_page_id(addr));
	if (page == NULL)
	{
		throw BAD_ADDR;
	}

	int ival;
	if (!page->read_int_at(get_page_offset(addr), col_offset, &ival))
		throw BAD_ADDR;
	return ival;
}

template<unsigned int PAGESIZE>
inline const char * RecordTable<PAGESIZE>::get_varchar(unsigned int addr, unsigned int col_offset, char *dst)
{
	const DataPage<PAGESIZE> *page = mRecordFile.get_data_page(get_page_id(addr));
	if (page == NULL)
		throw BAD_ADDR;

	if (!page->read_int_at(get_page_offset(addr), col_offset, &dst))
		throw BAD_ADDR;
	return dst;
}

template<unsigned int PAGESIZE>
inline unsigned char * RecordTable<PAGESIZE>::get_row(unsigned int addr)
{
	return mRecordFile.get_record()
}

template<unsigned int PAGESIZE>
inline void RecordTable<PAGESIZE>::print_record(table_attr_desc_t **pDesc, unsigned int descNum, const unsigned char *src)
{
	int int_val;
	char varchar_val[ATTR_SIZE_MAX + 1];
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
			if (strlen(varchar_val) <= 0)
				printf("NULL\t");
			else
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
		// Get field value from src
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

template<unsigned int PAGESIZE>
inline RecordTable<PAGESIZE>::fast_iterator::fast_iterator(RecordTable *pTable)
	: table(pTable), page_id(0), row_id(0)
{
	assert(table != NULL);
	cur_page = table->records().get_data_page(page_id);
}

template<unsigned int PAGESIZE>
inline RecordTable<PAGESIZE>::fast_iterator::~fast_iterator()
{
}

template<unsigned int PAGESIZE>
inline unsigned char * RecordTable<PAGESIZE>::fast_iterator::next()
{
	unsigned int dummy;
	return next(&dummy);
}

template<unsigned int PAGESIZE>
inline unsigned char * RecordTable<PAGESIZE>::fast_iterator::next(unsigned int * pAddr)
{
	do
	{
		assert(cur_page != NULL);

		if (row_id < cur_page->get_row_count())
		{
			*pAddr = get_page_addr(page_id, row_id);
			return cur_page->get_data_row(row_id++);
		}
		else
		{
			// No row remaining
			page_id++;
			row_id = 0;
			cur_page = table->records().get_data_page(page_id);
		}
	} while (page_id <= table->freemap().get_max_page_id());

	return NULL;
}
