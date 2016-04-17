#pragma once

#include "SQLParser.h"
#include "RecordTable.h"
#include "DatabaseFile.h"
#include <vector>
#include <stack>
#include <unordered_map>
#include <iostream>
#include <cstdio>

enum QueryExceptionType
{
	FROM_NO_SUCH_TABLE,
	FROM_DUPLICATE_TABLE,
	FROM_DUPLICATE_ALIAS,
	
	WHERE_COLUMN_AMBIGUOUS,
	WHERE_TABLEREF_ERROR,

	SELECT_COLUMN_AMBIGUOUS,
	SELECT_COLUMN_UNDEFINED,
	SELECT_TABLE_UNDEFINED,

	NO_FROM_CLAUSE,
	TYPE_MISMATCH,
	EXPR_SYNTAX_ERROR,
	UNDEFINED_EXPR,
	UNDEFINED_TOKEN_TYPE,
	UNDEFINED_ATTR_TYPE,
	ACCESS_VIOLATION
};

struct QueryException
{
	QueryExceptionType type;
	std::string msg;

	QueryException(QueryExceptionType _type, const char *_msg);
	QueryException(QueryExceptionType _type);
	~QueryException();
};

template <unsigned int PAGESIZE>
class QueryExecution
{
public:
	typedef std::pair<std::string, int> TableMapEntry;
	typedef std::unordered_map<std::string, int> TableMap;
	typedef RecordTable<PAGESIZE> Table;

	QueryExecution(DatabaseFile<PAGESIZE> &dbf);
	~QueryExecution();

	inline void execute(sql::SQLStatement *stmt);
private:
	struct StmtToken
	{
		enum Type { TOKEN_INT, TOKEN_STRING, TOKEN_BOOL, TOKEN_NULL};
		union Value 
		{
			int ival;
			bool bval;
			char *sval;

			Value(int _ival) : ival(_ival) {}
			Value(bool _bval) : bval(_bval) {}
			Value(char *_sval) : sval(_sval) {}
			Value() {}
			~Value() {}
		};

		StmtToken(int _ival) : value(_ival), type(TOKEN_INT) {}
		StmtToken(bool _bval) : value(_bval), type(TOKEN_BOOL) {}
		StmtToken(char *_sval) : value(_sval), type(TOKEN_STRING) {}
		StmtToken() : type(TOKEN_NULL) {}
		~StmtToken() 
		{ 
			// DON 'T DELETE SVAL, TO AVOIDING A BUNCH OF MEMALLOC
			// BUT REMEBER, DON' T MODIFY EXPR WHEN USING TOKEN
		}

		inline int get_int() 
		{
			assert(type == TOKEN_INT);
			return value.ival;
		}

		inline bool get_bool() 
		{
			assert(type == TOKEN_BOOL);
			return value.bval;
		}

		inline const char *get_string() 
		{
			assert(type == TOKEN_STRING);
			return value.sval;
		}

		StmtToken operator =(const StmtToken &t)
		{
			switch (t.type)
			{
			case TOKEN_INT:
				return StmtToken(t.get_int());
			case TOKEN_STRING:
				return StmtToken(t.get_string());
			case TOKEN_BOOL:
				return StmtToken(t.get_bool());
			case TOKEN_NULL:
				return StmtToken();
			default:
				throw QueryException(UNDEFINED_TOKEN_TYPE);
			}
			return StmtToken();
		}

		friend bool operator ==(StmtToken &t1, StmtToken &t2) 
		{
			if (t1.type != t2.type)
				throw QueryException(TYPE_MISMATCH);
			switch (t1.type)
			{
			case TOKEN_INT:
				return t1.get_int() == t2.get_int();
			case TOKEN_STRING:
				return strcmp(t1.get_string(), t2.get_string()) == 0;
			case TOKEN_BOOL:
				return t1.get_bool() == t2.get_bool();
			case TOKEN_NULL:
				return true; // Since t1.type == t2.type
			default:
				assert(false); // NOT REACHED
				break;
			}
		}

		friend bool operator <(StmtToken &t1, StmtToken &t2)
		{
			if(t1.type != TOKEN_INT || t2.type != TOKEN_INT)
				throw QueryException(TYPE_MISMATCH);
			return t1.get_int() < t2.get_int();
		}

		friend bool operator >(StmtToken &t1, StmtToken &t2)
		{
			if (t1.type != TOKEN_INT || t2.type != TOKEN_INT)
				throw QueryException(TYPE_MISMATCH);
			return t1.get_int() > t2.get_int();
		}

		Type type;
	private:
		Value value;
	};

	/* Attached database */
	DatabaseFile<PAGESIZE> &mDbf;

	/* Number of table selected */
	unsigned int mTableNum;

	/* Table reference */
	TableMap mTableMap;

	/* Table Pointers pool*/
	std::vector<Table *> mpTables;

	/* Final output address set */
	std::vector<unsigned int> mRecordAddrs;

	inline void execute_from(std::vector<sql::TableRef*> *from_clause);
	inline void execute_where(sql::Expr *where_clause);
	inline void execute_where_traverse_table(
		sql::Expr *where_clause,
		unsigned char **pRecords,
		std::vector<unsigned int> &addrs,
		unsigned int depth);
	inline bool execute_where_eval(
		sql::Expr *where_clause,
		unsigned char **pRecords);
	inline bool execute_where_parse_expr(
		sql::Expr *expr,
		unsigned char **pRecords,
		std::stack<StmtToken>& parse_stack);
	inline bool execute_where_parse_coldef(
		sql::Expr *expr,
		unsigned char **pRecords,
		std::stack<StmtToken>& parse_stack);
	inline bool execute_where_parse_operator(
		sql::Expr *expr,
		unsigned char **pRecords,
		std::stack<StmtToken>& parse_stack);
	inline bool execute_select(std::vector<sql::Expr *>&);
	inline std::pair<table_attr_desc_t *, unsigned int> match_col(sql::Expr& expr);
	inline void extract_token(const table_attr_desc_t *pAttrDesc, unsigned char *pRecord, std::stack<StmtToken> &parse_stack);
	inline int extract_int(unsigned char *pRecord, unsigned int offset);
	inline char *extract_varchar(unsigned char *pRecord, unsigned int offset);
	inline void print_record(const table_attr_desc_t *pDesc, const unsigned char *pRecord);
};

template<unsigned int PAGESIZE>
inline QueryExecution<PAGESIZE>::QueryExecution(DatabaseFile<PAGESIZE> &dbf)
	: mDbf(dbf)
{
}

template<unsigned int PAGESIZE>
inline QueryExecution<PAGESIZE>::~QueryExecution()
{
}

template<unsigned int PAGESIZE>
inline void QueryExecution<PAGESIZE>::execute(sql::SQLStatement * stmt)
{
	switch (stmt->type())
	{
	case sql::kStmtSelect:
	{
		// From clause
		sql::SelectStatement *select_stmt = (sql::SelectStatement*)stmt;
		execute_from(select_stmt->fromTable);

		// Where clause
		if(select_stmt->whereClause != NULL)
			execute_where(select_stmt->whereClause);

		// Select clause
		if (select_stmt->selectList != NULL)
			execute_select(*select_stmt->selectList);
	}
	break;
	default:
		break;
	}
}

template<unsigned int PAGESIZE>
inline void QueryExecution<PAGESIZE>::execute_from(std::vector<sql::TableRef*>* from_clause)
{
	mTableNum = 0;
	mpTables.resize(from_clause->size());
	for (int i = 0; i < from_clause->size(); i++)
	{
		sql::TableRef *ref = from_clause->at(i);
		mpTables[i] = mDbf.get_table(ref->name);
		if(mpTables[i] == NULL)
			throw QueryException(FROM_NO_SUCH_TABLE, ref->name);

		auto result = mTableMap.insert(TableMapEntry(ref->name, i));

		if (!result.second)
			throw QueryException(FROM_DUPLICATE_TABLE, ref->name);

		mTableNum++;

		if (ref->alias != NULL)
		{
			result = mTableMap.insert(TableMapEntry(ref->alias, i));
			if (!result.second)
				throw QueryException(FROM_DUPLICATE_ALIAS, ref->alias);
		}
	}

#ifdef _DEBUG_SELECT
	for (TableMap::iterator it = mTableMap.begin(); it != mTableMap.end(); it++)
	{
		printf("%s -> {%s\t%d}\n",it->first.c_str(), mpTables[it->second]->get_name(), it->second);
	}
#endif
}

template<unsigned int PAGESIZE>
inline void QueryExecution<PAGESIZE>::execute_where(sql::Expr * where_clause)
{
	if (mTableNum <= 0)
		throw QueryException(NO_FROM_CLAUSE);


	// Allocate table record 's pointer
	unsigned char **pRecords = new unsigned char*[mTableNum];
	
	std::vector<unsigned int> addrs;
	addrs.resize(mTableNum);

	execute_where_traverse_table(where_clause, pRecords, addrs, mTableNum - 1);
	delete [] pRecords;

#ifdef _DEBUG_SELECT
	for (int i = 0; i < mRecordAddrs.size(); i += mTableNum)
	{
		printf("{");
		for (int j = 0; j < mTableNum; j++)
		{
			printf("(");
			const table_attr_desc_t *pDescs = mpTables[j]->tablefile().get_attr_descs();
			for (int k = 0; k < mpTables[j]->tablefile().get_table_header().attrNum; k++)
			{
				if (k != 0) putchar('\t');
				const unsigned char *record = mpTables[j]->records().get_record(mRecordAddrs[i + j]);
				if (record == NULL)
					throw ACCESS_VIOLATION;
				print_record(&pDescs[k], record);
			}
			printf(")");
		}
		printf("}\n");
	}
#endif
}

template<unsigned int PAGESIZE>
inline void QueryExecution<PAGESIZE>::execute_where_traverse_table(
	sql::Expr * where_clause,
	unsigned char ** pRecords,
	std::vector<unsigned int> &addrs,
	unsigned int depth)
{
	unsigned int page_addr;
	Table::fast_iterator it(mpTables[depth]);
	while ((pRecords[depth] = it.next(&page_addr)) != NULL)
	{
		addrs[depth] = page_addr;
		if (depth == 0)
		{
			// Invoke where-checking
			bool b = execute_where_eval(where_clause, pRecords);
			if (b)
			{
				for (int i = 0; i < addrs.size(); i++)
					mRecordAddrs.push_back(addrs[i]);
			}
		}
		else
		{
			execute_where_traverse_table(where_clause, pRecords, addrs, depth - 1);
		}
	}
}

template<unsigned int PAGESIZE>
inline bool QueryExecution<PAGESIZE>::execute_where_eval(
	sql::Expr * where_clause, 
	unsigned char ** pRecords)
{
	std::stack<StmtToken> st;
	return execute_where_parse_expr(where_clause, pRecords, st);
}

template<unsigned int PAGESIZE>
inline bool QueryExecution<PAGESIZE>::execute_where_parse_expr(
	sql::Expr * expr, 
	unsigned char ** pRecords,
	std::stack<StmtToken>& parse_stack)
{
	bool result = false;
	switch (expr->type)
	{
	case sql::kExprLiteralInt:
		parse_stack.push(StmtToken((int)expr->ival));
		break;
	case sql::kExprLiteralString:
		parse_stack.push(StmtToken(expr->name));
		break;
	case sql::kExprColumnRef:
		execute_where_parse_coldef(expr, pRecords, parse_stack);
		break;
	case sql::kExprOperator:
		result = execute_where_parse_operator(expr, pRecords, parse_stack);
		break;
	default:
		throw QueryException(UNDEFINED_EXPR);
	}
	return result;
}

template<unsigned int PAGESIZE>
inline bool QueryExecution<PAGESIZE>::execute_where_parse_coldef(
	sql::Expr * expr, 
	unsigned char ** pRecords, 
	std::stack<StmtToken>& parse_stack)
{
	/// TODO: Caching to spped up
	// Table 0 's record in pRecords[0]
	// Table 1 's record in pRecords[1]
	// ...
	if (expr->hasTable())
	{
		// Try tablename, alias to find table id so that access table pointer
		auto result = mTableMap.find(expr->table);
		unsigned int tid = result->second;
		
		Table *pTable = mpTables[tid];
		if(pTable == NULL)
			throw QueryException(WHERE_TABLEREF_ERROR);

		table_attr_desc_t *pAttrDesc = NULL;
		if (result != mTableMap.end())
		{
			pAttrDesc = pTable->tablefile().get_attr_desc(expr->name);
			extract_token(pAttrDesc, pRecords[tid], parse_stack);
		}
	}
	else
	{
		// Linear search in tables
		unsigned int ref_cnt = 0;
		for (unsigned int i = 0; i < mTableNum; i++)
		{
			Table *pTable = mpTables[i];
			if (pTable == NULL)
				throw QueryException(WHERE_TABLEREF_ERROR);

			table_attr_desc_t *pAttrDesc = pTable->tablefile().get_attr_desc(expr->name);
			if (pAttrDesc != NULL)
			{
				if (ref_cnt > 0)
					throw QueryException(WHERE_COLUMN_AMBIGUOUS, expr->name);
				extract_token(pAttrDesc, pRecords[i], parse_stack);
				ref_cnt++;
			}
		}
	}


	return false;
}

template<unsigned int PAGESIZE>
inline bool QueryExecution<PAGESIZE>::execute_where_parse_operator(
	sql::Expr *expr, 
	unsigned char ** pRecords,
	std::stack<StmtToken>& parse_stack)
{
	execute_where_parse_expr(expr->expr, pRecords, parse_stack);
	execute_where_parse_expr(expr->expr2, pRecords, parse_stack);

	if (parse_stack.size() < 2)
		throw QueryException(EXPR_SYNTAX_ERROR);

	StmtToken &t2 = parse_stack.top(); parse_stack.pop();
	StmtToken &t1 = parse_stack.top(); parse_stack.pop();

	if (t1.type != t2.type)
		throw QueryException(TYPE_MISMATCH);

	bool result = false;
	switch (expr->op_type)
	{
	case sql::Expr::SIMPLE_OP:
	{
		switch (expr->op_char)
		{
		case '=':
			result = (t1 == t2);
			break;
		case '<':
			result = (t1 < t2);
			break;
		case '>':
			result = (t1 > t2);
			break;
		default:
			throw QueryException(EXPR_SYNTAX_ERROR);
		}
	}
	break;
	case sql::Expr::NOT_EQUALS:
		result = !(t1 == t2);
		break;
	case sql::Expr::AND:
		result = (t1.get_bool() && t2.get_bool());
		break;
	case sql::Expr::OR:
		result = (t1.get_bool() || t2.get_bool());
		break;
	default:
		throw QueryException(EXPR_SYNTAX_ERROR);
	}
	parse_stack.push(StmtToken(result));
	return result;
}

template<unsigned int PAGESIZE>
inline bool QueryExecution<PAGESIZE>::execute_select(std::vector<sql::Expr*>& select_clause)
{
	std::vector<std::tuple<unsigned int, table_attr_desc_t *, std::string>> descs;
	for (sql::Expr *expr : select_clause)
	{
		assert(expr != NULL);
		if (expr->type == sql::kExprStar)
		{
			/// TODO : To support star
		}
		else 
		{
			auto desc = match_col(*expr);
			assert(desc.second < mTableNum);
			descs.push_back(std::tuple<unsigned int, table_attr_desc_t *, std::string>
				(desc.second, desc.first, expr->getName()));
		}
	}

	for (int i = 0; i < descs.size(); i++)
	{
		printf("%s\t", std::get<2>(descs[i]).c_str());
	}
	putchar('\n');

	for (int i = 0; i < mRecordAddrs.size(); i += mTableNum)
	{
		for (int j = 0; j < descs.size(); j++)
		{
			unsigned int tid = std::get<0>(descs[j]);
			unsigned int addr = mRecordAddrs[i + tid];
			unsigned char *record = mpTables[tid]->records().get_record(addr);
			table_attr_desc_t *desc = std::get<1>(descs[j]);
			print_record(desc, record);
			printf("\t");
		}
		printf("\n");
	}

	return false;
}

template<unsigned int PAGESIZE>
inline std::pair<table_attr_desc_t *, unsigned int> QueryExecution<PAGESIZE>::match_col(sql::Expr & expr)
{
	unsigned int tid = 0;
	table_attr_desc_t *desc = NULL;
	if (expr.hasTable())
	{
		auto result = mTableMap.find(expr.table);
		if (result != mTableMap.end())
		{
			tid = result->second;
			desc = mpTables[tid]->tablefile().get_attr_desc(expr.name);
			if (desc == NULL)
				throw QueryException(SELECT_COLUMN_UNDEFINED, expr.name);
		}
		else
			throw QueryException(SELECT_TABLE_UNDEFINED, expr.table);
	}
	else 
	{
		int ref_cnt = 0;
		for (unsigned int i = 0; i < mTableNum; i++)
		{
			desc = mpTables[tid]->tablefile().get_attr_desc(expr.name);
			if (desc != NULL)
			{
				tid = i;
				ref_cnt++;
				if (ref_cnt > 1)
					throw QueryException(SELECT_COLUMN_AMBIGUOUS, expr.name);
			}
		}

		if (ref_cnt == 0)
			throw QueryException(SELECT_COLUMN_UNDEFINED, expr.name);
	}
	return std::pair<table_attr_desc_t *, unsigned int>(desc, tid);
}

template<unsigned int PAGESIZE>
inline void QueryExecution<PAGESIZE>::extract_token(
	const table_attr_desc_t * pAttrDesc, 
	unsigned char * pRecord, 
	std::stack<StmtToken>& parse_stack)
{
	if (pAttrDesc->type == ATTR_TYPE_INTEGER)
	{
		parse_stack.push(StmtToken(extract_int(pRecord, pAttrDesc->offset)));
	}
	else if (pAttrDesc->type == ATTR_TYPE_VARCHAR)
	{
		parse_stack.push(StmtToken(extract_varchar(pRecord, pAttrDesc->offset)));
	}
	else
	{
		printf("WARNING: Use NULL Type as Table schema.\n");
		parse_stack.push(StmtToken());
	}
}

template<unsigned int PAGESIZE>
inline int QueryExecution<PAGESIZE>::extract_int(unsigned char * pRecord, unsigned int offset)
{
	return *(int*)(pRecord + offset);
}

template<unsigned int PAGESIZE>
inline char * QueryExecution<PAGESIZE>::extract_varchar(unsigned char * pRecord, unsigned int offset)
{
	return (char*)pRecord + offset;
}

template<unsigned int PAGESIZE>
inline void QueryExecution<PAGESIZE>::print_record(const table_attr_desc_t * pDesc, const unsigned char * pRecord)
{
	int ival;
	char sval[ATTR_SIZE_MAX];
	switch (pDesc->type)
	{
	case ATTR_TYPE_INTEGER:
		memcpy(&ival, pRecord + pDesc->offset, pDesc->size);
		printf("%d",ival);
		break;
	case ATTR_TYPE_VARCHAR:
		memcpy(sval, pRecord + pDesc->offset, pDesc->size);
		printf("%s", sval);
		break;
	case ATTR_TYPE_UNDEFINED:
		printf("NULL");
		break;
	default:
		throw UNDEFINED_ATTR_TYPE;
	}
}
