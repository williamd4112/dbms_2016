#pragma once

#include "database_table_type.h"
#include "database_type.h"
#include "database_util.h"
#include "DiskFile.h"
#include "LightTable.h"

#define UNKNOWN_TABLE 0x55
#define DATABASE_LITE_DUPLICATE_TABLE 0x56

class DatabaseLiteFile
	: public DiskFile
{
public:
	DatabaseLiteFile();
	~DatabaseLiteFile();

	LightTable & get_table(std::string tablename);
	void set_table(std::string tablename, std::vector<table_attr_desc_t> &attr_descs);
	inline bool isExist(std::string tablename);

	inline void write_back();
	inline void read_from();
private:
	HashMap<std::string, LightTable*> mTables;
};

