#pragma once

#include "DatabaseFile.h"
#include "SQLParser.h"
#include "FileUtil.h"
#include "QueryExecution.h"
#include <unordered_map>
#include <stack>

static const char *PROMPT_PREFIX = "Database$: ";

enum DatabaseErrorType
{
	DUPLICATE_TABLE,
	INSERT_COLUMN_NOT_FOUND,
	SYNTAX_ERROR
};

template <unsigned int PAGESIZE>
class Database
{
public:
#define Error printf
#define Msg printf
#define ClearBuffer() memset(mExtractionBuffer, 0, ROW_SIZE_MAX)

	Database(const char *);
	~Database();

	inline void execute(std::string&);
	inline void shutdown();
private:
	DatabaseFile<PAGESIZE> mDatabaseFile;
	unsigned char mExtractionBuffer[ROW_SIZE_MAX];
	
	void error_handler(DatabaseErrorType);
	void exec_error_handler(QueryException &e);
	bool extract_values(sql::InsertStatement*, RecordTable<PAGESIZE> *);
	bool extract_value(const sql::Expr *, const table_attr_desc_t*);
};

template<unsigned int PAGESIZE>
inline Database<PAGESIZE>::Database(const char *dbs_filepath)
{
	bool exist = FileUtil::exist(dbs_filepath);
	mDatabaseFile.open(dbs_filepath, exist ? "r+" : "w+");

	if (exist)
		mDatabaseFile.read_from();
}

template<unsigned int PAGESIZE>
inline Database<PAGESIZE>::~Database()
{
}

template<unsigned int PAGESIZE>
inline void Database<PAGESIZE>::execute(std::string &query)
{
	Msg("%s execute: %s\n", PROMPT_PREFIX, query.c_str());
	sql::SQLParserResult *parser = sql::SQLParser::parseSQLString(query);

	if (parser->isValid)
	{
		for (sql::SQLStatement *stmt : parser->statements)
		{
			switch (stmt->type())
			{
			case sql::kStmtSelect:
			{
				try
				{
					QueryExecution<PAGESIZE> exec(mDatabaseFile);
					exec.execute(stmt);
				}
				catch (QueryException e)
				{
					exec_error_handler(e);
				}
				break;
			}
			case sql::kStmtInsert:
			{
				try
				{
					sql::InsertStatement* in_st = (sql::InsertStatement*)stmt;
					RecordTable<PAGESIZE> *table = mDatabaseFile.get_table(in_st->tableName);
					extract_values(in_st, table);
					if (table->insert(mExtractionBuffer))
						Msg("%s insertion success.\n", PROMPT_PREFIX);
					else
						Msg("%s insertion failed.\n", PROMPT_PREFIX);
					//table->dump_content();
				}
				catch (DatabaseErrorType e)
				{
					error_handler(e);
				}
				break;
			}
			case sql::kStmtCreate:
			{
				sql::CreateStatement* cr_st = (sql::CreateStatement*)stmt;
				bool result = mDatabaseFile.create_table(cr_st->tableName, *cr_st->columns);
				if (result)
				{
					Msg("%s table %s creation successful.\n", PROMPT_PREFIX, cr_st->tableName);
					RecordTable<PAGESIZE> *table = mDatabaseFile.get_table(cr_st->tableName);
					table->dump_info();
				}
				else
				{
					Error("%s table %s creation failed.\n", PROMPT_PREFIX, cr_st->tableName);
				}
				break;
			}
			default:
				Error("%s unsupported statement.\n", PROMPT_PREFIX);
				break;
			}
		}
	}
	else
	{
		Error("%s Execution error, syntax error.\n", PROMPT_PREFIX);
	}
}

template<unsigned int PAGESIZE>
inline void Database<PAGESIZE>::shutdown()
{
	mDatabaseFile.write_back();
}

template<unsigned int PAGESIZE>
inline void Database<PAGESIZE>::error_handler(DatabaseErrorType e)
{
	switch (e)
	{
	case SYNTAX_ERROR:
		Error("%s Execution error, syntax error.\n", PROMPT_PREFIX);
		break;
	case INSERT_COLUMN_NOT_FOUND:
		Error("%s Insertion error, column not found.\n", PROMPT_PREFIX);
		break;
	default:
		Error("%s Unhandled error, error code: %d\n", PROMPT_PREFIX, e);
		break;
	}
}

template<unsigned int PAGESIZE>
inline void Database<PAGESIZE>::exec_error_handler(QueryException & e)
{
	switch (e.type)
	{
	case FROM_NO_SUCH_TABLE:
		Error("%s %s not found.\n", PROMPT_PREFIX, e.msg.c_str());
		break;
	case FROM_DUPLICATE_TABLE:
		Error("%s %s duplicated table.\n", PROMPT_PREFIX, e.msg.c_str());
		break;
	case FROM_DUPLICATE_ALIAS:
		Error("%s %s duplicated alias.\n", PROMPT_PREFIX, e.msg.c_str());
		break;
	case WHERE_COLUMN_AMBIGUOUS:
		Error("%s %s ambiguous column.\n", PROMPT_PREFIX, e.msg.c_str());
		break;
	case WHERE_TABLEREF_ERROR:
		Error("%s %s table ref error.\n", PROMPT_PREFIX, e.msg.c_str());
		break;
	case SELECT_COLUMN_AMBIGUOUS:
		Error("%s %s ambiguous column.\n", PROMPT_PREFIX, e.msg.c_str());
		break;
	case SELECT_COLUMN_UNDEFINED:
		Error("%s %s column undefined.\n", PROMPT_PREFIX, e.msg.c_str());
		break;
	case SELECT_TABLE_UNDEFINED:
		Error("%s %s table undefined.\n", PROMPT_PREFIX, e.msg.c_str());
		break;
	case EXPR_SYNTAX_ERROR:
		Error("%s %s\n", PROMPT_PREFIX, e.msg.c_str());
		break;
	default:
		Error("%s unhandled error: %d\n", PROMPT_PREFIX, e.type);
		break;
	}
}

template<unsigned int PAGESIZE>
inline bool Database<PAGESIZE>::extract_values(sql::InsertStatement *in_st, RecordTable<PAGESIZE> *table)
{
	ClearBuffer();

	assert(in_st->values != NULL);

	const std::vector<sql::Expr*> *values = in_st->values;
	if (in_st->columns == NULL)
	{
		const std::vector<sql::Expr*> *values = in_st->values;
		for (int i = 0; i < values->size(); i++)
		{
			const sql::Expr *expr = values->at(i);
			const table_attr_desc_t *pDesc = table->tablefile().get_attr_desc(i);
			if (!extract_value(expr, pDesc))
				return false;
		}
	}
	else
	{
		if (in_st->columns->size() != in_st->values->size())
		{
			Error("%s Insertion statement error: column names number should be same as values'.\n", PROMPT_PREFIX);
			return false;
		}

		const int size = in_st->columns->size();
		for (int i = 0; i < size; i++)
		{
			const sql::Expr *expr = values->at(i);
			const table_attr_desc_t *pDesc = table->tablefile().get_attr_desc(in_st->columns->at(i));
			if (!extract_value(expr, pDesc))
				return false;
		}
	}
}

template<unsigned int PAGESIZE>
inline bool Database<PAGESIZE>::extract_value(const sql::Expr *expr, const table_attr_desc_t *pDesc)
{
	if (pDesc == NULL)
		throw INSERT_COLUMN_NOT_FOUND;
	if (expr->type == sql::kExprLiteralInt)
		memcpy(mExtractionBuffer + pDesc->offset, &expr->ival, pDesc->size);
	else if (expr->type == sql::kExprLiteralString)
		memcpy(mExtractionBuffer + pDesc->offset, expr->name, strlen(expr->name));
	else
	{
		Error("%s Insertion statement error: syntax error\n", PROMPT_PREFIX);
		return false;
	}
	return true;
}

