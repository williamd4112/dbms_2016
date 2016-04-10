#pragma once

#include <cstdio>
#include <cstdlib>
#include <iostream>

/*
	Record
	Only store at read/write buffer, so there is no many of copy in memory.
	So, we expect this class more readable even though losing some efficiency.

	This class is design for abstracting read/write operation on Buffer object.

	For example, when we SELECT * FROM table
	we may bring amount of record from disk to memory, and store them at read buffer.

	When we insert, we put data at write buffer, and write data into RecordFile 's buffer.
*/
class Record
{
public:
	Record(unsigned char *pool, size_t size);
	~Record();

	inline void write(void *src);
	inline void write_int(int val, size_t offset);
	inline void write_varchar(const char *val, size_t len, size_t offset);
	inline void read(void *dst);
	inline int read_int(size_t offset);
	inline const char *read_varchar(size_t offset);
private:
	/* Free space size */
	size_t mSize;

	/* Assign by Buffer object */
	unsigned char *mData;
};

