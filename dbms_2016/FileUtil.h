#pragma once

#include <cstdio>
#include <iostream>

#include <sys\stat.h>

namespace FileUtil
{
	void write_back(FILE *file, size_t offset, void *src, size_t src_size);
	void read_at(FILE *file, size_t offset, void *dst, size_t dst_size);
	bool exist(const char *);
}
