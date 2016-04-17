#include "DataPage.h"

int get_int_from_record(const unsigned char *src, int offset)
{
	int val;
	memcpy(&val, src + offset, sizeof(int));
	return val;
}
