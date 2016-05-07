#pragma once

#include "system.h"
#include "DatabaseFile.h"
#include "SQLParser.h"
#include "FileUtil.h"
#include "database_util.h"
#include "QueryExecution.h"
#include "Execution.h"
#include <unordered_map>
#include <stack>

#define PROMPT_PREFIX "Database$: "

enum DatabaseErrorType
{
	DUPLICATE_TABLE,
	INSERT_COLUMN_NOT_FOUND,
	INSERT_COLUMN_SIZE_EXECEED,
	UNDEFINED_TABLE,
	SYNTAX_ERROR
};

template <unsigned int PAGESIZE>
class Database
{
public:

#define ClearBuffer() memset(mExtractionBuffer, 0, ROW_SIZE_MAX)

	Database(const char *);
	~Database();

	inline void execute(std::string&);
	
	// TODO : For testing
	inline void create_index(const char *tablename, const char *attr_name, const char *index_name, IndexType index_type);
	inline void find(const char *tablename, const char *attr_name, int begin, int end);
	inline void shutdown();
private:
	DatabaseFile<PAGESIZE> mDatabaseFile;
	unsigned char mExtractionBuffer[ROW_SIZE_MAX];
	
	void error_handler(DatabaseErrorType);
	void exec_error_handler(QueryException &e);
	void db_error_handler(db::DatabaseUtilException &e);
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
	sql::SQLParserResult *parser = sql::SQLParser::parseSQLString(query);
	if (parser->isValid)
	{
		for (sql::SQLStatement *stmt : parser->statements)
		{
			switch (stmt->type())
			{
			case sql::kStmtSelect:
			{
				//try
				//{
				//	printf("-----------------------------------------------------------\n");
				//	QueryExecution<PAGESIZE> exec(mDatabaseFile);
				//	exec.execute(stmt);
				//}
				//catch (QueryException e)
				//{
				//	exec_error_handler(e);
				//}
				//catch (db::DatabaseUtilException e)
				//{
				//	db_error_handler(e);
				//}
				SelectStatement *select_stmt = (SelectStatement*)stmt;
				From<PAGESIZE_8K> from(mDatabaseFile, select_stmt->fromTable);
				break;
			}
			case sql::kStmtInsert:
			{
				try
				{
					sql::InsertStatement* in_st = (sql::InsertStatement*)stmt;
					RecordTable<PAGESIZE> *table = mDatabaseFile.get_table(in_st->tableName);
					if (table == NULL)
						throw UNDEFINED_TABLE;
					extract_values(in_st, table);

					if (table->insert(mExtractionBuffer))
					{
						Msg("%s insertion success.\n", PROMPT_PREFIX);
					}
					else
						Error("%s insertion failed: %s.\n", PROMPT_PREFIX, table->get_error_msg());
				}
				catch (DatabaseErrorType e)
				{
					error_handler(e);
				}
				catch (RecordTableException e)
				{
					Error("%s %s\n", PROMPT_PREFIX, RecordTable<PAGESIZE>::get_error_msg(e));
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
		Error("%s Parsing error: %s\n", PROMPT_PREFIX, parser->errorMsg);
	}
}

template<unsigned int PAGESIZE>
inline void Database<PAGESIZE>::create_index(
	const char * tablename, 
	const char * attr_name, 
	const char * index_name, 
	IndexType index_type)
{
	RecordTable<PAGESIZE> *pTable = mDatabaseFile.get_table(tablename);
	if (pTable != NULL)
	{
		pTable->create_index(attr_name, index_name, index_type);
		pTable->dump_info();
	}
	else
	{
		Error("%s failed to set_table index %s\n", PROMPT_PREFIX, index_name);
	}
}

template<unsigned int PAGESIZE>
inline void Database<PAGESIZE>::find(const char * tablename, const char *attr_name, int begin, int end)
{
	RecordTable<PAGESIZE> *pTable = mDatabaseFile.get_table(tablename);
	if (pTable != NULL)
	{
	
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
	case INSERT_COLUMN_SIZE_EXECEED:
		Error("%s Insertion error, column too large.\n", PROMPT_PREFIX);
		break;
	case UNDEFINED_TABLE:
		Error("%s Insertion error, table not found.\n", PROMPT_PREFIX);
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
	case WHERE_COLUMN_UNDEFINED:
		Error("%s %s undefined column.\n", PROMPT_PREFIX, e.msg.c_str());
		break;
	case WHERE_TABLEREF_ERROR:
		Error("%s %s table ref error.\n", PROMPT_PREFIX, e.msg.c_str());
		break;
	case WHERE_TYPE_MISMATCH:
		Error("%s type mismatched.\n", PROMPT_PREFIX, e.msg.c_str());
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
		Error("%s Syntax error: %s\n", PROMPT_PREFIX, e.msg.c_str());
		break;
	default:
		Error("%s error: %d, %s\n", PROMPT_PREFIX, e.type, e.msg.c_str());
		break;
	}
}

template<unsigned int PAGESIZE>
inline void Database<PAGESIZE>::db_error_handler(db::DatabaseUtilException & e)
{
	switch (e)
	{
	case db::TYPE_CAST_ERROR:
		Error("%s Type casting error when parsing record.\n", PROMPT_PREFIX);
		break;
	default:
		Error("%s unhandled error type: %d\n", PROMPT_PREFIX, e);
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
	{
		if (strlen(expr->name) >= pDesc->size) // Since we need to consider terminate character, so use <=
			throw INSERT_COLUMN_SIZE_EXECEED;
		memcpy(mExtractionBuffer + pDesc->offset, expr->name, strlen(expr->name));
	}
	else
	{
		Error("%s Insertion statement error: syntax error\n", PROMPT_PREFIX);
		return false;
	}
	return true;
}

