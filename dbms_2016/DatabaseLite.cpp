#include "DatabaseLite.h"

#define UNKOWN_STMT_TYPE 0x1
#define UNEXPECTED_ERROR 0x2

DatabaseLite::DatabaseLite(const char *dbs_filepath)
{
	bool exist = FileUtil::exist(dbs_filepath);
	mDbf.open(dbs_filepath, exist ? "r+" : "w+");

	if (exist)
		mDbf.read_from();
}


DatabaseLite::~DatabaseLite()
{

}

void DatabaseLite::exec(std::string & command)
{
	sql::SQLParserResult *parser = sql::SQLParser::parseSQLString(command);
	try
	{
		if (parser->isValid)
		{
			for (sql::SQLStatement *stmt : parser->statements)
			{
				switch (stmt->type())
				{
				case sql::kStmtSelect:
					exec_select(stmt);
					break;
				case sql::kStmtCreate:
					exec_create(stmt);
					break;
				case sql::kStmtInsert:
					exec_insert(stmt);
					break;
				default:
					throw exception_t(UNKOWN_STMT_TYPE, "Unknown stmt type");
				}
			}
		}
		else
		{
			Error("Parser error, %s\n", parser->errorMsg);
		}
	}
	catch (exception_t e)
	{
		switch (e.code)
		{
		default:
			Error("Error code: %d, %s\n", e.code, e.msg.c_str());
			break;
		}
	}
}

void DatabaseLite::load(std::string dbs_filepath)
{
	mDbf.open(dbs_filepath.c_str(), "r+");
	mDbf.read_from(); // Load tablenames, table instance into memory
}

void DatabaseLite::save()
{
	mDbf.write_back();
}

void DatabaseLite::exec_create(sql::SQLStatement * stmt)
{
	sql::CreateStatement *create_stmt = static_cast<sql::CreateStatement*>(stmt);

	std::vector<AttrDesc> attr_descs(create_stmt->columns->size());
	if (create_stmt->columns == NULL)
		throw exception_t(UNEXPECTED_ERROR, "Unexpected error: Create statement has null column def");

	const auto col_defs = create_stmt->columns;
	uint32_t offset = 0;
	for (int i = 0; i < attr_descs.size(); i++)
	{
		const sql::ColumnDefinition *col_def = col_defs->at(i);
		strncpy(attr_descs[i].name, col_def->name, ATTR_NAME_MAX);
		attr_descs[i].offset = offset;
		attr_descs[i].size = col_def->length;
		attr_descs[i].type = db::type_coldef_to_attr(col_def->type);
		attr_descs[i].constraint = (col_def->IsPK) ? ATTR_CONSTRAINT_PRIMARY_KEY : ATTR_CONSTRAINT_NO;

		offset += attr_descs[i].size;
	}

	mDbf.set_table(create_stmt->tableName, attr_descs);
}

void DatabaseLite::exec_insert(sql::SQLStatement * stmt)
{
	sql::InsertStatement* in_st = (sql::InsertStatement*)stmt;

	if (in_st->values == NULL)
		throw exception_t(UNEXPECTED_ERROR, "Insert statemnt has null value list.");

	LightTable & table_ref = mDbf.get_table(in_st->tableName);
	
	const auto & values = *(in_st->values);
	AttrTuple tuple(values.size());

	if (in_st->columns != NULL)
	{
		// Columun mapping
		const auto & col_refs = *(in_st->columns);
		for (int i = 0; i < col_refs.size(); i++)
		{
			int col_id = table_ref.get_attr_id(col_refs[i]);

			if (values[col_id]->type == sql::kExprLiteralInt)
				tuple[col_id] = values[i]->ival;
			else if (values[i]->type == sql::kExprLiteralString)
				tuple[col_id] = values[i]->name;
			else
				throw exception_t(UNEXPECTED_ERROR, "Unknown expr value type");
		}
	}
	else
	{
		// Order mapping
		for (int i = 0; i < values.size(); i++)
		{
			if (values[i]->type == sql::kExprLiteralInt)
				tuple[i] = values[i]->ival;
			else if (values[i]->type == sql::kExprLiteralString)
				tuple[i] = values[i]->name;
			else
				throw exception_t(UNEXPECTED_ERROR, "Unknown expr value type");
		}
	}
	table_ref.insert(tuple);
}

void DatabaseLite::exec_select(sql::SQLStatement * stmt)
{
	if (stmt == NULL)
		throw exception_t(UNEXPECTED_ERROR, "Select statement conversion error, null statement.");

	sql::SelectStatement & select_stmt = static_cast<sql::SelectStatement&>(*stmt);

	if (select_stmt.hasWhere())
	{

	}
	else
	{

	}
}
