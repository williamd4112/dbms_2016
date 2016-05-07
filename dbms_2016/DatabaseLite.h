#pragma once

#include "database_type.h"
#include "database_util.h"
#include "LightTable.h"
#include "SQLParser.h"
#include "DatabaseLiteFile.h"

/*
	DatabaseLite

	working with LightTable, SequenceFile

*/
class DatabaseLite
{
public:
	DatabaseLite(const char *dbs_filepath);
	~DatabaseLite();

	void exec(std::string & command);
	void load(std::string dbs_filepath);
	void save();
private:
	DatabaseLiteFile mDbf;

	void exec_create(sql::SQLStatement *stmt);
	void exec_insert(sql::SQLStatement *stmt);
	void exec_select(sql::SQLStatement *stmt);
};

