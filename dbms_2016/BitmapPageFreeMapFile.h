#pragma once

#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <assert.h>

#include "Bit.h"
#include "FileUtil.h"
#include "DiskFile.h"

#define PAGEFREEMAPFILE_NO_FREE_PAGE -1
#define PAGEFREEMAPFILE_CLOSE_ERROR -2

/*
	PageFreeMapFile
	
	. Store which page is occupied
	. Fast retrival of a free page for page allocation
	. Slow when first full

	Related class:
	. HeapPageFreeMapFile

	. Data Segment
		. Number of pages (int)
		. Page full bit (# Max pages / 8) byte
		
*/

template <size_t PAGESIZE, 
	size_t FILESIZE = 4294967295,
	unsigned int MAXNUMPAGE = FILESIZE / PAGESIZE>
class BitmapPageFreeMapFile
	: public DiskFile
{
public:
	struct DiskPart
	{
		unsigned int mCurMaxPage;
		Bitmap<MAXNUMPAGE> mBitmap;

		DiskPart();
		~DiskPart();
	};

	BitmapPageFreeMapFile();
	~BitmapPageFreeMapFile();

	inline unsigned int get_free_page();
	inline unsigned int get_max_page_id();
	inline void set_page_full(unsigned int);
	inline void dump_info();

	inline void write_back();
	inline void read_from();

private:
	/* Data read/write from disk */
	DiskPart mDiskPart;
};

template<size_t PAGESIZE, size_t FILESIZE, unsigned int MAXNUMPAGE>
inline BitmapPageFreeMapFile<PAGESIZE, FILESIZE, MAXNUMPAGE>::BitmapPageFreeMapFile()
	: DiskFile()
{
	
}

template<size_t PAGESIZE, size_t FILESIZE, unsigned int MAXNUMPAGE>
inline BitmapPageFreeMapFile<PAGESIZE, FILESIZE, MAXNUMPAGE>::~BitmapPageFreeMapFile()
{

}

/*
	get_free_page
	
	Start scanning from last available position, end when scanning last page.
	(for now, we don't need to consider fragmentation)
*/
template<size_t PAGESIZE, size_t FILESIZE, unsigned int MAXNUMPAGE>
inline unsigned int BitmapPageFreeMapFile<PAGESIZE, FILESIZE, MAXNUMPAGE>::get_free_page()
{
	unsigned int maxPageIndex = mDiskPart.mCurMaxPage;

#ifndef _DELETION
	while (maxPageIndex < MAXNUMPAGE)
	{
		if (!mDiskPart.mBitmap.Test(maxPageIndex))
		{
			mDiskPart.mCurMaxPage = maxPageIndex;
			return maxPageIndex;
		}
		else
			maxPageIndex++;
	}
#else
	do {
		maxPageIndex %= MAXNUMPAGE;
		if (!mDiskPart.mBitmap.Test(maxPageIndex)) {
			mDiskPart.mCurMaxPage = maxPageIndex;
			return maxPageIndex;
		}
		else
			maxPageIndex++;
	} while (maxPageIndex != mDiskPart.mCurMaxPage);
#endif

	throw PAGEFREEMAPFILE_NO_FREE_PAGE;
}

template<size_t PAGESIZE, size_t FILESIZE, unsigned int MAXNUMPAGE>
inline unsigned int BitmapPageFreeMapFile<PAGESIZE, FILESIZE, MAXNUMPAGE>::get_max_page_id()
{
	return mDiskPart.mCurMaxPage;
}

/*
	set_page_full
	
	Called by other object which manipulate the page file. 
*/
template<size_t PAGESIZE, size_t FILESIZE, unsigned int MAXNUMPAGE>
inline void BitmapPageFreeMapFile<PAGESIZE, FILESIZE, MAXNUMPAGE>::set_page_full(unsigned int pid)
{
	assert(pid >= 0 && pid < MAXNUMPAGE);
	mDiskPart.mBitmap.Set(pid);
}


template<size_t PAGESIZE, size_t FILESIZE, unsigned int MAXNUMPAGE>
inline void BitmapPageFreeMapFile<PAGESIZE, FILESIZE, MAXNUMPAGE>::dump_info()
{
	std::cout << "===PageFreeMapFile Begin===" << std::endl;
	std::cout << "Max # Page: " << MAXNUMPAGE << std::endl
		<< "Current Max Page: " << mDiskPart.mCurMaxPage << std::endl
		<< "Page Status:" << std::endl;
	//mDiskPart.mBitmap.dump_info();
	std::cout << "===PageFreeMapFile End===" << std::endl;
}

template<size_t PAGESIZE, size_t FILESIZE, unsigned int MAXNUMPAGE>
inline void BitmapPageFreeMapFile<PAGESIZE, FILESIZE, MAXNUMPAGE>::write_back()
{
	FileUtil::write_back(mFile, 0, &mDiskPart, sizeof(DiskPart));
}

template<size_t PAGESIZE, size_t FILESIZE, unsigned int MAXNUMPAGE>
inline void BitmapPageFreeMapFile<PAGESIZE, FILESIZE, MAXNUMPAGE>::read_from()
{
	FileUtil::read_at(mFile, 0, &mDiskPart, sizeof(DiskPart));
}

template<size_t PAGESIZE, size_t FILESIZE, unsigned int MAXNUMPAGE>
inline BitmapPageFreeMapFile<PAGESIZE, FILESIZE, MAXNUMPAGE>::DiskPart::DiskPart() 
	: mCurMaxPage(0)
{
}

template<size_t PAGESIZE, size_t FILESIZE, unsigned int MAXNUMPAGE>
inline BitmapPageFreeMapFile<PAGESIZE, FILESIZE, MAXNUMPAGE>::DiskPart::~DiskPart()
{
}
