#pragma once

#include <cstdio>
#include <iostream>

#define DISKFILE_ERROR_CLOSE -2

class DiskFile
{
public:
	DiskFile();
	~DiskFile();

	bool open(const char *, const char *);

	std::string get_filepath();

	virtual void write_back() = 0;
	virtual void read_from() = 0;

protected:
	std::string mFilepath;
	FILE *mFile;

private:
	inline bool close();
};

