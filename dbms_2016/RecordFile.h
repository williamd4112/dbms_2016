#pragma once

#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <assert.h>

#include "DataPage.h"
#include "DiskFile.h"
#include "Bit.h"

#define BIT_INVALID 0x80000000
#define BIT_LOW_PAGEID 13
#define BIT_HIGH_PAGEOFFSET 12
#define BIT_MASK_PAGEOFFSET 0x1FFF

#define BIT_USING 0x1
#define BIT_DIRTY 0x2

#define BIT_SUCCESS 0x1
#define BIT_PUT_FULL 0x2

#define PAGEBUFFER_WRITE 0x1
#define PAGEBUFFER_READ 0x2

#define get_page_id(addr) (get_val_uint32((addr), BIT_LOW_PAGEID, 31))
#define get_page_offset(addr) (get_val_uint32((addr), 0, BIT_HIGH_PAGEOFFSET))
#define get_page_addr(id, offset) ((id) << BIT_LOW_PAGEID) | ((offset) & BIT_MASK_PAGEOFFSET)


/*
	RecordFile

	Handling record insertion at disk page and record retrival at disk page.

	Each record has its own address of record file.
	Since MAX_PAGE_SIZE = 8K, there are at most 4G / 8K = 50K page.

	Page Row offset = 0 ~ 8191 (13 bit)[0] ~ [12]
	Page offset = 0 ~ 524287 (19 bit)[13] ~ [31]

	Once a record write into data file, its location never change again.
	So, index strucuture can use Page Number + Page Offset to determine the absolute address of a record

*/
template <size_t PAGESIZE, 
	unsigned int BUFFER_NUM_ROW = 32, unsigned int BUFFER_NUM_COL = 1, unsigned int BUFFER_SLOT_NUM = BUFFER_NUM_ROW * BUFFER_NUM_COL>
class RecordFile
	: public DiskFile
{
#define get_page(pid, mode) mPageBuffer.get((pid), (mode))
public:
	/*
		PageBuffer

		Record file won't write back to disk at each write request. Frequent disk IO definitely slow down the system.
		When RecordFile handling a put_record, it just write to page buffer in memory. Writeback until the buffer is full.

		NOTE: RecordFile doesn't allocate a page buffer in its stack, it allocate in heap.

		As for page allocation in buffer. Here is some options.
		1. Locality - slot selected by modulo the page number
		2. Linear Hash - slot selected by complex hash function

		When collision, if the old page is dirty, writeback the page to disk, if not, only replace it.

		For simplicity, we use locality now. 
		So, more rows, more memory usage, more performance.
	*/
	struct PageBuffer
	{
#define file_offset(pid) (pid) * PAGESIZE
#define flush_slot(bid) mPages[(bid)].write_back(*mpFile, mBufferPageID[(bid)] * PAGESIZE); \
		mPages[(bid)].clear(); \
		mBufferSlotInfo[(bid)] = 0x0; \
		mBufferPageID[(bid)] = 0

#define is_dirty(bid) mBufferSlotInfo[(bid)] & (BIT_USING | BIT_DIRTY)

	public:
		PageBuffer();
		~PageBuffer();

		inline void init(FILE **, size_t rowsize);
		inline DataPage<PAGESIZE> *get(unsigned int, unsigned char);
		inline void flush();
	private:
		FILE **mpFile;
		DataPage<PAGESIZE> *mPages;
		unsigned char mBufferSlotInfo[BUFFER_SLOT_NUM];
		unsigned int mBufferPageID[BUFFER_SLOT_NUM];

		inline void load(unsigned int, unsigned int);
	};

	RecordFile(size_t rowsize);
	RecordFile();
	~RecordFile();
	
	inline unsigned int put_record(unsigned int ,void *, unsigned char *);
	inline bool get_record(unsigned int, void *);
	inline DataPage<PAGESIZE> *get_data_page(unsigned int);
	inline unsigned int find_record(const void *, unsigned int, bool *);
	inline unsigned int find_record_with_col(const void *, unsigned int, unsigned int, unsigned int, bool *);

	inline void init(size_t rowsize);
	inline void write_back();
	inline void read_from();
private:
	size_t rowsize;
	PageBuffer mPageBuffer;

};

template<size_t PAGESIZE, unsigned int BUFFER_NUM_ROW, unsigned int BUFFER_NUM_COL, unsigned int BUFFER_SLOT_NUM>
inline RecordFile<PAGESIZE, BUFFER_NUM_ROW, BUFFER_NUM_COL, BUFFER_SLOT_NUM>::
RecordFile(size_t rowsize)
	: DiskFile(), rowsize(rowsize)
{
	mPageBuffer.init(&mFile, rowsize);
}

template<size_t PAGESIZE, unsigned int BUFFER_NUM_ROW, unsigned int BUFFER_NUM_COL, unsigned int BUFFER_SLOT_NUM>
inline RecordFile<PAGESIZE, BUFFER_NUM_ROW, BUFFER_NUM_COL, BUFFER_SLOT_NUM>::
RecordFile()
	: DiskFile()
{
}

template<size_t PAGESIZE, unsigned int BUFFER_NUM_ROW, unsigned int BUFFER_NUM_COL, unsigned int BUFFER_SLOT_NUM>
inline RecordFile<PAGESIZE, BUFFER_NUM_ROW, BUFFER_NUM_COL, BUFFER_SLOT_NUM>::
~RecordFile()
{

}


/*
	put_record

	Pass page_id, write a row into the specific page.

	Usually, page_id is determined by Table (by looking up the free map file)

	result is used to check if success

	return file address in record file

*/
template<size_t PAGESIZE, unsigned int BUFFER_NUM_ROW, unsigned int BUFFER_NUM_COL, unsigned int BUFFER_SLOT_NUM>
inline unsigned int RecordFile<PAGESIZE, BUFFER_NUM_ROW, BUFFER_NUM_COL, BUFFER_SLOT_NUM>::
put_record(unsigned int page_id, void *src, unsigned char *result)
{
	// Get page from cache
	DataPage<PAGESIZE> *page = get_page(page_id, PAGEBUFFER_WRITE);
	
	assert(page != NULL);
	assert(result != NULL);

	*result = 0x0;

	// Put record
	int page_offset = page->write_row(src);
	if (page_offset >= 0)
	{
		page_id = get_page_addr(page_id, page_offset);
		*result |= BIT_SUCCESS;
	}

	if (page->isFull())
		*result |= BIT_PUT_FULL;

	return page_id;
}

/*
	get_record

	Pass a file address, return a record in the disk page.
	
	return
	false - no such record
	true - found
*/
template<size_t PAGESIZE, unsigned int BUFFER_NUM_ROW, unsigned int BUFFER_NUM_COL, unsigned int BUFFER_SLOT_NUM>
inline bool RecordFile<PAGESIZE, BUFFER_NUM_ROW, BUFFER_NUM_COL, BUFFER_SLOT_NUM>::
get_record(unsigned int file_addr, void *dst)
{
	unsigned int page_id = get_page_id(file_addr);
	unsigned int page_offset = get_page_offset(file_addr);

	DataPage<PAGESIZE> *page = get_page(page_id, PAGEBUFFER_READ);

	return page->read_row(page_offset, dst);
}

template<size_t PAGESIZE, unsigned int BUFFER_NUM_ROW, unsigned int BUFFER_NUM_COL, unsigned int BUFFER_SLOT_NUM>
inline DataPage<PAGESIZE>* RecordFile<PAGESIZE, BUFFER_NUM_ROW, BUFFER_NUM_COL, BUFFER_SLOT_NUM>::get_data_page(unsigned int page_id)
{
	return get_page(page_id, PAGEBUFFER_READ);
}

template<size_t PAGESIZE, unsigned int BUFFER_NUM_ROW, unsigned int BUFFER_NUM_COL, unsigned int BUFFER_SLOT_NUM>
inline unsigned int RecordFile<PAGESIZE, BUFFER_NUM_ROW, BUFFER_NUM_COL, BUFFER_SLOT_NUM>::
find_record(const void *src, unsigned int max_page, bool *result)
{
	// WARNING: exhaustive searching here
	// AVOID DIRECT CALLING find_record, use index to find record instead
	for (int i = 0; i <= max_page; i++)
	{
		int row_id;
		DataPage<PAGESIZE_8K> *page = get_page(i, PAGEBUFFER_READ);
		if ((row_id = page->find_row(src)) >= 0)
		{
			*result = true;
			return get_page_addr(i, row_id);
		}
	}
	*result = false;
	return 0xffffffff;
}

template<size_t PAGESIZE, unsigned int BUFFER_NUM_ROW, unsigned int BUFFER_NUM_COL, unsigned int BUFFER_SLOT_NUM>
inline unsigned int RecordFile<PAGESIZE, BUFFER_NUM_ROW, BUFFER_NUM_COL, BUFFER_SLOT_NUM>::
find_record_with_col(const void *src, unsigned int max_page, unsigned int col_offset, unsigned int col_size, bool *result)
{
	for (int i = 0; i <= max_page; i++)
	{
		int row_id;
		DataPage<PAGESIZE_8K> *page = get_page(i, PAGEBUFFER_READ);
		if ((row_id = page->find_col(src, col_offset, col_size)) >= 0)
		{
			*result = true;
			return get_page_addr(i, row_id);
		}
	}
	*result = false;
	return 0xffffffff;
}

template<size_t PAGESIZE, unsigned int BUFFER_NUM_ROW, unsigned int BUFFER_NUM_COL, unsigned int BUFFER_SLOT_NUM>
inline void RecordFile<PAGESIZE, BUFFER_NUM_ROW, BUFFER_NUM_COL, BUFFER_SLOT_NUM>::init(size_t rowsize)
{
	assert(rowsize > 0);
	this->rowsize = rowsize;
	mPageBuffer.init(&mFile, rowsize);
}

/*
	write_back

	Flush the page buffer
*/
template<size_t PAGESIZE, unsigned int BUFFER_NUM_ROW, unsigned int BUFFER_NUM_COL, unsigned int BUFFER_SLOT_NUM>
inline void RecordFile<PAGESIZE, BUFFER_NUM_ROW, BUFFER_NUM_COL, BUFFER_SLOT_NUM>::
write_back()
{
	mPageBuffer.flush();
}

template<size_t PAGESIZE, unsigned int BUFFER_NUM_ROW, unsigned int BUFFER_NUM_COL, unsigned int BUFFER_SLOT_NUM>
inline void RecordFile<PAGESIZE, BUFFER_NUM_ROW, BUFFER_NUM_COL, BUFFER_SLOT_NUM>::
read_from()
{
	// DO NOTHING
}

template<size_t PAGESIZE, unsigned int BUFFER_NUM_ROW, unsigned int BUFFER_NUM_COL, unsigned int BUFFER_SLOT_NUM>
inline RecordFile<PAGESIZE, BUFFER_NUM_ROW, BUFFER_NUM_COL, BUFFER_SLOT_NUM>::
PageBuffer::PageBuffer() : 
	mBufferSlotInfo {0}, mBufferPageID {0}
{

}

template<size_t PAGESIZE, unsigned int BUFFER_NUM_ROW, unsigned int BUFFER_NUM_COL, unsigned int BUFFER_SLOT_NUM>
inline RecordFile<PAGESIZE, BUFFER_NUM_ROW, BUFFER_NUM_COL, BUFFER_SLOT_NUM>::
PageBuffer::~PageBuffer()
{
	delete[] mPages;
}

template<size_t PAGESIZE, unsigned int BUFFER_NUM_ROW, unsigned int BUFFER_NUM_COL, unsigned int BUFFER_SLOT_NUM>
inline void RecordFile<PAGESIZE, BUFFER_NUM_ROW, BUFFER_NUM_COL, BUFFER_SLOT_NUM>::PageBuffer::
init(FILE **pFile, size_t rowsize)
{
	mpFile = pFile;
	mPages = new DataPage<PAGESIZE>[BUFFER_SLOT_NUM];
	for (int i = 0; i < BUFFER_SLOT_NUM; i++)
		mPages[i].init(rowsize);
}

template<size_t PAGESIZE, unsigned int BUFFER_NUM_ROW, unsigned int BUFFER_NUM_COL, unsigned int BUFFER_SLOT_NUM>
inline DataPage<PAGESIZE>* RecordFile<PAGESIZE, BUFFER_NUM_ROW, BUFFER_NUM_COL, BUFFER_SLOT_NUM>::
PageBuffer::get(unsigned int page_id, unsigned char mode)
{
	unsigned int buffer_id = page_id % BUFFER_SLOT_NUM;

	// Cache miss 
	// 1. Page ID inconsistent
	// 2. Page not using
	if ((mBufferPageID[buffer_id] != page_id || 
		!(mBufferSlotInfo[buffer_id] & BIT_USING)))
	{
		load(buffer_id, page_id);
	}
	
	// Make the buffer dirty
	if (mode & PAGEBUFFER_WRITE)
		mBufferSlotInfo[buffer_id] |= BIT_DIRTY;

	assert(&mPages[buffer_id] != NULL);

	return &mPages[buffer_id];
}

template<size_t PAGESIZE, unsigned int BUFFER_NUM_ROW, unsigned int BUFFER_NUM_COL, unsigned int BUFFER_SLOT_NUM>
inline void RecordFile<PAGESIZE, BUFFER_NUM_ROW, BUFFER_NUM_COL, BUFFER_SLOT_NUM>::
PageBuffer::flush()
{
	for (int i = 0; i < BUFFER_SLOT_NUM; i++)
		if (is_dirty(i)) 
		{
			flush_slot(i);
		}
}

template<size_t PAGESIZE, unsigned int BUFFER_NUM_ROW, unsigned int BUFFER_NUM_COL, unsigned int BUFFER_SLOT_NUM>
inline void RecordFile<PAGESIZE, BUFFER_NUM_ROW, BUFFER_NUM_COL, BUFFER_SLOT_NUM>::
PageBuffer::load(unsigned int buffer_id, unsigned int page_id)
{
	// Check if need flush
	if (is_dirty(buffer_id))
	{
		flush_slot(buffer_id);
	}

	// Set using bit
	mBufferSlotInfo[buffer_id] |= BIT_USING;
	mBufferPageID[buffer_id] = page_id;

	// Bring page in
	// NOTE: assume rowsize not change
	mPages[buffer_id].read_at(*mpFile, file_offset(page_id));
}
