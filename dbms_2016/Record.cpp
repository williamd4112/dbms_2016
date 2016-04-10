#include "Record.h"

Record::Record(unsigned char * pool, size_t size)
{
}

Record::~Record()
{
}

inline void Record::write(void * src)
{
}

inline void Record::write_int(int val, size_t offset)
{
}

inline void Record::write_varchar(const char * val, size_t len, size_t offset)
{
}

inline void Record::read(void * dst)
{
}

inline int Record::read_int(size_t offset)
{
	return 0;
}

inline const char * Record::read_varchar(size_t offset)
{
	return nullptr;
}
