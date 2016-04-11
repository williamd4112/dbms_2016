#pragma once

#include <cstdlib>
#include <cstdio>

void fatal_error()
{
	// TODO: write a proper exception handling routine for this
	system("pause");
	exit(-1);
}