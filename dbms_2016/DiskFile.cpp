#include "DiskFile.h"



DiskFile::DiskFile() : mFile(NULL)
{
}


DiskFile::~DiskFile()
{
	if (close() == EOF)
		throw DISKFILE_ERROR_CLOSE;
}

bool DiskFile::open(const char *filepath, const char *mode)
{
	mFile = fopen(filepath, mode);
	return mFile != NULL;
}

inline bool DiskFile::close()
{
	return fclose(mFile) == 0;
}
