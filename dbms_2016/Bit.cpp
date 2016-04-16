#include "Bit.h"

void printm(void * src, unsigned int size)
{
	const unsigned char *csrc = (const unsigned char *)src;
	for (int j = 0; j < size; j++)
	{
		printf("%x ", csrc[j]);
	}
}
