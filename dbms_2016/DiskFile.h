#pragma once

#include <cstdio>

#define DISKFILE_ERROR_CLOSE -2

class DiskFile
{
public:
	DiskFile();
	~DiskFile();

	bool open(const char *, const char *);

	virtual void write_back() = 0;
	virtual void read_from() = 0;

protected:
	FILE *mFile;

private:
	inline bool close();
};

