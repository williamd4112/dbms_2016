#include "FileUtil.h"

#include <assert.h>

void FileUtil::write_back(FILE * file, size_t offset, void * src, size_t src_size)
{
	assert(file != NULL);
	fseek(file, offset, SEEK_SET);
	fwrite(src, src_size, 1, file);
}

void FileUtil::read_at(FILE * file, size_t offset, void * dst, size_t dst_size)
{
	assert(file != NULL);
	fseek(file, offset, SEEK_SET);
	fread(dst, dst_size, 1, file);
}

bool FileUtil::exist(const char *filename)
{
	struct stat buffer;
	return stat(filename, &buffer) == 0;
}

