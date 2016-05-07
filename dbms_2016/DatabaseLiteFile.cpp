#include "DatabaseLiteFile.h"



DatabaseLiteFile::DatabaseLiteFile()
{
}


DatabaseLiteFile::~DatabaseLiteFile()
{
}

LightTable & DatabaseLiteFile::get_table(std::string tablename)
{
	auto table = mTables.find(tablename);
	if (table == mTables.end())
		throw exception_t(UNKNOWN_TABLE, std::string(std::string("Unknown table: ") + tablename).c_str());
	return *(table->second);
}

void DatabaseLiteFile::set_table(std::string tablename, std::vector<table_attr_desc_t> &attr_descs)
{
	LightTable *pTable = new LightTable;
	pTable->create(tablename.c_str(), attr_descs.data(), attr_descs.size());
	
	auto res = mTables.insert({ tablename, pTable });
	if (!res.second)
	{
		delete pTable;
		throw exception_t(DATABASE_LITE_DUPLICATE_TABLE, std::string(std::string("Duplicated table : ") + tablename).c_str());
	}
	pTable->dump();
}

inline bool DatabaseLiteFile::isExist(std::string tablename)
{
	return mTables.find(tablename) == mTables.end();
}

inline void DatabaseLiteFile::write_back()
{
	fseek(mFile, 0, SEEK_SET);
	for (auto it = mTables.begin(); it != mTables.end(); it++)
	{
		fprintf(mFile, "%s\n", it->first.c_str()); // Store tablename
		it->second->save(); // Write back table instance
	}
}

inline void DatabaseLiteFile::read_from()
{
	fseek(mFile, 0, SEEK_SET);
	char name_buff[TABLE_NAME_MAX + 1];
	while (fscanf(mFile, "%s", name_buff) > 0) // Load tablename
	{
		LightTable *pTable = new LightTable();
		pTable->load(name_buff); // Load table instance

		auto res = mTables.insert({ name_buff, pTable});
		if (!res.second)
		{
			Warning("Duplicated table %s, ignore", name_buff);
		}
	}
}
