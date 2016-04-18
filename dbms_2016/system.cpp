#pragma once

#include <cstdlib>
#include <cstdio>
#include "system.h"

#ifdef _QUIET
bool quiet = true;
#else
bool quiet = false;
#endif

void fatal_error()
{
	// TODO: write a proper exception handling routine for this
	system("pause");
	exit(-1);
}

void sub_error(const char *msg)
{
	printf("Warning: %s\n", msg);
}
