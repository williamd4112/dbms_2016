#pragma once

#include "FileUtil.h"

#include <iostream>
#include <cstdlib>
#include <cstdio>
#include <queue>
#include <cassert>

#define PAGESIZE_8K 8192
#define ROW_USED 0xff
#define ROW_FREE 0x00

template <size_t PAGESIZE>
/*
	Page
	This class is used to manipulate data page. 
	In-Memory:
		. mRowsize:
	In-Disk:
		. mData:
			. | ---- | ---- | ----------------- |
				used	cnt		   data
*/
class DataPage
{
#define get_row_addr(row_id) mDataSegBegin + mRowsize * (row_id)
#define get_col_addr(row_id, col_offset) get_row_addr((row_id)) + col_offset
public:
	DataPage(size_t rowsize);
	DataPage();
	~DataPage();
	
	inline void init(size_t rowsize);

	inline int write_row(void *src);
	inline bool write_int_at(int row_id, size_t row_offset, int val);
	inline bool write_varchar_at(int row_id, size_t row_offset, const char *val, size_t len);
	
	inline bool read_row(int row_id, void *dst);
	inline bool read_int_at(int row_id, size_t row_offset, void *dst);
	inline bool read_varchar_at(int row_id, size_t row_offset, void *dst, size_t len);

	unsigned char *get_data_row(unsigned int row_id) const;

	inline void write_back(FILE *file, size_t offset);
	inline void read_at(FILE *file, size_t offset);
	
	inline int find_row(const void *src);
	inline int find_col(const void *src, unsigned int col_offset, unsigned int col_size);
	inline int find_col_int(int src, unsigned int col_offset);
	inline int find_col_varchar(char * src, unsigned int col_offset, unsigned int col_size);

	inline bool isUsed(int row_id);
	inline bool isFull();
	inline void clear();
	inline unsigned int get_row_count() const;

	void dump_info();
private:
	/* Size of one row */
	size_t mRowsize;
	
	/* Max number of rows */
	unsigned int mMaxRowCount;

	/* Start of data segment */
	unsigned char *mDataSegBegin;
	
	/* Start of row_count */
	unsigned short *mpRowCount;

#ifdef _DELETION
	/* Free List of page */
	std::queue<int> mFreeRowQueue;
#endif

	/* Raw data */
	unsigned char mData[PAGESIZE];

	inline int find_free_row();
};

template<size_t PAGESIZE>
DataPage<PAGESIZE>::DataPage(size_t rowsize)
	: mRowsize(rowsize),
	mMaxRowCount(PAGESIZE / (rowsize + sizeof(unsigned char))),
	mpRowCount(NULL)
{
	mpRowCount = (unsigned short*)(mData + mMaxRowCount * sizeof(unsigned char));
	mDataSegBegin = ((unsigned char*)mpRowCount) + sizeof(unsigned short);
	memset(mData, 0, PAGESIZE);

	*mpRowCount = 0;

#ifdef _DELETION
	mFreeRowList.resize(mMaxRowCount);
#endif
}

template<size_t PAGESIZE>
inline DataPage<PAGESIZE>::DataPage()
	: mRowsize(0),
	mMaxRowCount(0),
	mpRowCount(NULL)
{

}

template<size_t PAGESIZE>
DataPage<PAGESIZE>::~DataPage()
{
}

template<size_t PAGESIZE>
inline void DataPage<PAGESIZE>::init(size_t rowsize)
{
	mRowsize = rowsize;
	mMaxRowCount = PAGESIZE / (rowsize + sizeof(unsigned char));
	mpRowCount = (unsigned short*)(mData + mMaxRowCount * sizeof(unsigned char));
	mDataSegBegin = ((unsigned char*)mpRowCount) + sizeof(unsigned short);
	memset(mData, 0, PAGESIZE);
	*mpRowCount = 0;
}

template<size_t PAGESIZE>
inline int DataPage<PAGESIZE>::write_row(void * src)
{
	int free_id = find_free_row();
	if (free_id < 0)
		return -1;

	memcpy(mDataSegBegin + mRowsize * free_id, src, mRowsize);
	mData[free_id] = ROW_USED;
	*mpRowCount = *mpRowCount + 1;

	return free_id;
}

template<size_t PAGESIZE>
inline bool DataPage<PAGESIZE>::write_int_at(int row_id, size_t row_offset, int val)
{
	if (row_id >= mMaxRowCount || row_offset >= mRowsize)
		return false;
	memcpy(mDataSegBegin + rowsize * row_id + row_offset, &val, sizeof(int));
	return true;
}

template<size_t PAGESIZE>
inline bool DataPage<PAGESIZE>::write_varchar_at(int row_id, size_t row_offset, const char * val, size_t len)
{
	if (row_id >= mMaxRowCount || row_offset >= mRowsize)
		return false;
	memcpy(mDataSegBegin + rowsize * row_id + row_offset, val, len * sizeof(char));
	return true;
}

template<size_t PAGESIZE>
inline bool DataPage<PAGESIZE>::read_row(int row_id, void * dst)
{
	if (row_id >= mMaxRowCount)
		return false;
	if (!isUsed(row_id))
		return false;
	memcpy(dst, mDataSegBegin + row_id * mRowsize, mRowsize);

	return true;
}

template<size_t PAGESIZE>
inline bool DataPage<PAGESIZE>::read_int_at(int row_id, size_t row_offset, void * dst)
{
	if (row_id >= mMaxRowCount || row_offset >= mRowsize || !isUsed(row_id))
		return false;
	memcpy(dst, mDataSegBegin + rowsize * row_id + row_offset, sizeof(int));

	return true;
}

template<size_t PAGESIZE>
inline bool DataPage<PAGESIZE>::read_varchar_at(int row_id, size_t row_offset, void * dst, size_t len)
{
	if (row_id >= mMaxRowCount || row_offset >= mRowsize || isUsed(row_id))
		return false;
	memcpy(dst, mDataSegBegin + rowsize * row_id + row_offset, len * sizeof(char));

	return true;
}

template<size_t PAGESIZE>
inline unsigned char * DataPage<PAGESIZE>::get_data_row(unsigned int row_id) const
{
	assert(row_id >= 0 && row_id < *mpRowCount);
	return get_row_addr(row_id);
}

template<size_t PAGESIZE>
inline void DataPage<PAGESIZE>::write_back(FILE * file, size_t offset)
{
	FileUtil::write_back(file, offset, mData, PAGESIZE);
}

template<size_t PAGESIZE>
inline void DataPage<PAGESIZE>::read_at(FILE * file, size_t offset)
{
	FileUtil::read_at(file, offset, mData, PAGESIZE);

#ifdef _DELETION
	// Construct free queue
	for (int i = 0; i < mMaxRowCount; i++);
	{
		if (mData[i] == ROW_USED)
		{
			*mRowCount++;
			mFreeRowQueue.push(i);
		}
	}
#endif

}

/*
	find_row

	Pass a row data copy to match the disk row data.

	return - Row ID (PageOffset) - success
		   - -1 - fail (PageOffset must < 32 bit, don't worry about ambiguious)
*/
template<size_t PAGESIZE>
inline int DataPage<PAGESIZE>::find_row(const void * src)
{
	for (int i = 0; i < *mpRowCount; i++)
	{
		if (isUsed(i))
		{
			if (memcmp(src, get_row_addr(i), mRowsize) == 0)
				return i;
		}
	}
	return -1;
}

template<size_t PAGESIZE>
inline int DataPage<PAGESIZE>::find_col(const void * col_src, unsigned int col_offset, unsigned int col_size)
{
	for (int i = 0; i < *mpRowCount; i++)
	{
		if (isUsed(i))
		{
			if (memcmp(col_src, get_col_addr(i, col_offset), col_size) == 0)
				return i;
		}
	}
	return -1;
}

template<size_t PAGESIZE>
inline int DataPage<PAGESIZE>::find_col_int(int src, unsigned int col_offset)
{
	return find_col(&src, col_offset, sizeof(int));
}

template<size_t PAGESIZE>
inline int DataPage<PAGESIZE>::find_col_varchar(char * src, unsigned int col_offset, unsigned int col_size)
{
	return find_col(src, col_offset, col_size);
}

template<size_t PAGESIZE>
inline bool DataPage<PAGESIZE>::isUsed(int row_id)
{
	if (row_id < 0 || row_id >= mMaxRowCount)
		return true;
	return mData[row_id] == ROW_USED;
}

template<size_t PAGESIZE>
inline bool DataPage<PAGESIZE>::isFull()
{
	return (*mpRowCount) >= mMaxRowCount;
}

template<size_t PAGESIZE>
inline void DataPage<PAGESIZE>::clear()
{
	memset(mData, 0, PAGESIZE);
	*mpRowCount = 0;
}

template<size_t PAGESIZE>
inline unsigned int DataPage<PAGESIZE>::get_row_count() const
{
	return *mpRowCount;
}

template<size_t PAGESIZE>
void DataPage<PAGESIZE>::dump_info()
{
	std::cout << "Max # of rows: " << mMaxRowCount << std::endl;
	std::cout << "Rowsize: " << mRowsize << std::endl;
	std::cout << "# Row: " << *mpRowCount << std::endl;

}

template<size_t PAGESIZE>
inline int DataPage<PAGESIZE>::find_free_row()
{
	if (*mpRowCount < mMaxRowCount)
	{
		if(mData[*mpRowCount] == ROW_FREE)
			return *mpRowCount;
	}
#ifdef _DELETION
	else if (!mFreeRowQueue.empty())
	{
		int id = mFreeRowQueue.top(); mFreeRowQueue.pop();
		return id;
	}
#endif
	return -1;
}
