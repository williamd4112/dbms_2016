#include "DiskFile.h"



DiskFile::DiskFile() : mFile(NULL)
{
}


DiskFile::~DiskFile()
{
	if (mFile != NULL && close() == EOF)
		throw DISKFILE_ERROR_CLOSE;
}

bool DiskFile::open(const char *filepath, const char *mode)
{
	mFile = fopen(filepath, mode);
	mFilepath = filepath;
	return mFile != NULL;
}

std::string DiskFile::get_filepath()
{
	return mFilepath;
}

inline bool DiskFile::close()
{
	return fclose(mFile) == 0;
}
