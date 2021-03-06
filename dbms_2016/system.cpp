#pragma once

#include <cstdlib>
#include <cstdio>
#include <ctime>
#include "system.h"

#ifdef _QUIET
bool quiet = true;
#else
bool quiet = false;
#endif

bool pause_at_exit = true;
bool interactive = false;
bool report = false;

void fatal_error()
{
	// TODO: write a proper exception handling routine for this
	printf("Runtime fatal error.\n");
	system("pause");
	exit(-1);
}

void sub_error(const char *msg)
{
	printf("Warning: %s\n", msg);
}

void profile(Routine routine)
{
	clock_t begin, end;
	double time_spent;

	begin = clock();
	routine();
	end = clock();
	time_spent = (double)(end - begin) / CLOCKS_PER_SEC;
	printf("Time elapsed: %lf\n", time_spent);
}

exception_t::exception_t(uint32_t _code) : 
	code(_code), msg("No message")
{
}

exception_t::exception_t(uint32_t _code, const char * _msg) :
	code(_code), msg(_msg)
{
}

exception_t::~exception_t()
{
}
