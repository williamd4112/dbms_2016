#pragma once

#include "SQLParser.h"
#include "RecordTable.h"
#include "DatabaseFile.h"
#include "database_util.h"
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
	WHERE_RANGE_UNDEFINED,

	SELECT_COLUMN_AMBIGUOUS,
	SELECT_COLUMN_UNDEFINED,
	SELECT_TABLE_UNDEFINED,
	SELECT_RANGE_UNDEFINED,

	NO_FROM_CLAUSE,
	TYPE_MISMATCH,
	EXPR_SYNTAX_ERROR,
	UNDEFINED_EXPR,
	UNDEFINED_TOKEN_TYPE,
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
#define RANGE_FROM 0x1
#define RANGE_WHERE 0x2
public:
	enum SelectEntryType
	{
		COLUMN,
		COUNT,
		SUM
	};

	typedef std::pair<std::string, int> TableMapEntry;
	typedef std::unordered_map<std::string, int> TableMap;
	typedef RecordTable<PAGESIZE> Table;
	typedef std::tuple<unsigned int, table_attr_desc_t *, std::string, SelectEntryType> SelectEntry;

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

	/* Range qualifier */
	unsigned char mRange;

	/* Table reference */
	TableMap mTableMap;

	/* Table Pointers pool*/
	std::vector<Table *> mpTables;

	/* Record Pointer buffer */
	std::vector<unsigned char *> pRecords;

	/* Parsing stack for statment */
	std::stack<StmtToken> mParseStack;

	/* Filtered address set */
	std::vector<unsigned int> mFilteredRecordAddrs;
	
	/* Select Entries (specify what column be selected) */
	std::vector<SelectEntry> mSelectEntries;

	/* Aggregation counter */
	std::vector<long long> mAggregationCounter;

	inline void execute_from(std::vector<sql::TableRef*> *from_clause);
	inline void execute_where(sql::Expr *where_clause);

	inline void execute_where_traverse_table(
		sql::Expr *where_clause,
		std::vector<unsigned int> &addrs,
		unsigned int depth);

	inline void execute_select_list(
		std::vector<sql::Expr *>&);

	inline void execute_aggregation_list(
		std::vector<sql::AggregationFunction*>&);

	inline void parse_select_list(
		std::vector<sql::Expr*>& select_list);

	inline void parse_aggregation_list(
		std::vector<sql::AggregationFunction*> aggregation_list);

	inline void execute_select_print_all(
		std::vector<unsigned int> &pageAddrs,
		unsigned int depth);

	inline void execute_select_aggregate_all(
		std::vector<unsigned int> &pageAddrs,
		unsigned int depth);

	inline void execute_select_aggregate_entries(
		std::vector<unsigned int> pageAddrs,
		unsigned int baseoffset);

	inline std::pair<table_attr_desc_t *, unsigned int> match_col(
		const sql::Expr& expr);

	inline std::pair<table_attr_desc_t *, unsigned int> match_col(
		const char *table, 
		const char *name);

	inline void parse_select_entry(
		sql::Expr &expr,
		SelectEntryType type);

	inline bool parse_eval(
		sql::Expr *where_clause);

	inline bool parse_expr(
		sql::Expr *expr);

	inline void parse_token(
		const table_attr_desc_t *pAttrDesc, 
		unsigned char *pRecord);

	inline bool parse_coldef(
		sql::Expr *expr);

	inline bool parse_operator(
		sql::Expr *expr);

	inline const char *gen_select_column_name(
		char *buff, 
		const char *table, 
		const char *col, 
		SelectEntryType type);

	inline void print_select_entries(
		std::vector<SelectEntry> &selectEntries);

	inline void print_aggregation_counter();

	inline void print_select_column_with_entries(
		std::vector<unsigned int> pageAddrs,
		unsigned int baseoffset);
};

template<unsigned int PAGESIZE>
inline QueryExecution<PAGESIZE>::QueryExecution(DatabaseFile<PAGESIZE> &dbf)
	: mDbf(dbf), mRange(RANGE_FROM)
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
		if (select_stmt->whereClause != NULL)
		{
			execute_where(select_stmt->whereClause);
			mRange = RANGE_WHERE;
		}

		// Select clause
		if (select_stmt->hasAggregation() && select_stmt->hasSelect())
			throw QueryException(EXPR_SYNTAX_ERROR, "Aggregation cannot combine with column.");
		else if (select_stmt->hasAggregation())
			execute_aggregation_list(*select_stmt->aggregation_list);
		else if (select_stmt->hasSelect())
			execute_select_list(*select_stmt->selectList);
		else
			throw QueryException(EXPR_SYNTAX_ERROR, "Select nothing");
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
}

template<unsigned int PAGESIZE>
inline void QueryExecution<PAGESIZE>::execute_where(sql::Expr * where_clause)
{
	if (mTableNum <= 0)
		throw QueryException(WHERE_RANGE_UNDEFINED);

	// Allocate table record 's pointer
	std::vector<unsigned int> addrs;
	addrs.resize(mTableNum);

	execute_where_traverse_table(where_clause, addrs, mTableNum - 1);
}

template<unsigned int PAGESIZE>
inline void QueryExecution<PAGESIZE>::execute_where_traverse_table(
	sql::Expr * where_clause,
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
			bool b = parse_eval(where_clause);
			if (b)
			{
				for (int i = 0; i < addrs.size(); i++)
					mFilteredRecordAddrs.push_back(addrs[i]);
			}
		}
		else
		{
			execute_where_traverse_table(where_clause, addrs, depth - 1);
		}
	}
}

template<unsigned int PAGESIZE>
inline bool QueryExecution<PAGESIZE>::parse_eval(
	sql::Expr * where_clause)
{
	while (!mParseStack.empty())
		mParseStack.pop();
	return parse_expr(where_clause);
}

template<unsigned int PAGESIZE>
inline bool QueryExecution<PAGESIZE>::parse_expr(
	sql::Expr * expr)
{
	bool result = false;
	switch (expr->type)
	{
	case sql::kExprLiteralInt:
		mParseStack.push(StmtToken((int)expr->ival));
		break;
	case sql::kExprLiteralString:
		mParseStack.push(StmtToken(expr->name));
		break;
	case sql::kExprColumnRef:
		parse_coldef(expr);
		break;
	case sql::kExprOperator:
		result = parse_operator(expr);
		break;
	default:
		throw QueryException(UNDEFINED_EXPR);
	}
	return result;
}

template<unsigned int PAGESIZE>
inline bool QueryExecution<PAGESIZE>::parse_coldef(
	sql::Expr * expr)
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
			parse_token(pAttrDesc, pRecords[tid]);
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
				parse_token(pAttrDesc, pRecords[i]);
				ref_cnt++;
			}
		}
	}


	return false;
}

template<unsigned int PAGESIZE>
inline bool QueryExecution<PAGESIZE>::parse_operator(
	sql::Expr *expr)
{
	parse_expr(expr->expr);
	parse_expr(expr->expr2);

	if (mParseStack.size() < 2)
		throw QueryException(EXPR_SYNTAX_ERROR);

	StmtToken &t2 = mParseStack.top(); mParseStack.pop();
	StmtToken &t1 = mParseStack.top(); mParseStack.pop();

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
	mParseStack.push(StmtToken(result));
	return result;
}

template<unsigned int PAGESIZE>
inline void QueryExecution<PAGESIZE>::execute_select_list(
	std::vector<sql::Expr*>& select_list)
{
	parse_select_list(select_list);
	print_select_entries(mSelectEntries);

	switch (mRange)
	{
	case RANGE_WHERE:
		for (int i = 0; i < mFilteredRecordAddrs.size(); i += mTableNum)
		{
			print_select_column_with_entries(mFilteredRecordAddrs, i);
		}
		break;
	case RANGE_FROM:
	{
		std::vector<unsigned int> addrs(mTableNum, 0);
		execute_select_print_all(addrs, mTableNum - 1);
	}
	break;
	default:
		throw QueryException(SELECT_RANGE_UNDEFINED, " requires WHERE or FROM.");
	}
}

template<unsigned int PAGESIZE>
inline void QueryExecution<PAGESIZE>::execute_aggregation_list(
	std::vector<sql::AggregationFunction*>& aggreationList)
{
	parse_aggregation_list(aggreationList);
	mAggregationCounter.resize(mSelectEntries.size(), 0);
	
	switch (mRange)
	{
	case RANGE_WHERE:
		for (int i = 0; i < mFilteredRecordAddrs.size(); i += mTableNum)
		{
			execute_select_aggregate_entries(mFilteredRecordAddrs, i);
		}
		break;
	case RANGE_FROM:
		{
			std::vector<unsigned int> addrs(mTableNum, 0);
			execute_select_aggregate_all(addrs, mTableNum - 1);
		}
		break;
	default:
		throw QueryException(SELECT_RANGE_UNDEFINED, " requires WHERE or FROM.");
	}

	print_select_entries(mSelectEntries);
	print_aggregation_counter();
}

template<unsigned int PAGESIZE>
inline void QueryExecution<PAGESIZE>::parse_select_list(
	std::vector<sql::Expr*>& select_list)
{
	for (sql::Expr *expr : select_list)
	{
		parse_select_entry(*expr, COLUMN);
	}
}

template<unsigned int PAGESIZE>
inline void QueryExecution<PAGESIZE>::parse_aggregation_list(
	std::vector<sql::AggregationFunction*> aggregation_list)
{
	for (sql::AggregationFunction *aggregation : aggregation_list)
	{
		switch (aggregation->type)
		{
		case sql::AggregationFunction::kCount:
			parse_select_entry(*(aggregation->attribute), COUNT);
			break;
		case sql::AggregationFunction::kSum:
			parse_select_entry(*(aggregation->attribute), SUM);
			break;
		default:
			throw QueryException(EXPR_SYNTAX_ERROR);
		}
	}
}

template<unsigned int PAGESIZE>
inline void QueryExecution<PAGESIZE>::execute_select_print_all(
	std::vector<unsigned int>& pageAddrs,
	unsigned int depth)
{
	unsigned int page_addr;
	Table::fast_iterator it(mpTables[depth]);
	while (it.next(&page_addr) != NULL)
	{
		pageAddrs[depth] = page_addr;
		if (depth == 0)
		{
			print_select_column_with_entries(pageAddrs, 0);
			printf("\n");
		}
		else
		{
			execute_select_print_all(pageAddrs, depth - 1);
		}
	}
}

template<unsigned int PAGESIZE>
inline void QueryExecution<PAGESIZE>::print_select_column_with_entries(
	std::vector<unsigned int> pageAddrs,
	unsigned int baseoffset)
{
	for (unsigned int i = 0; i < mSelectEntries.size(); i++)
	{
		unsigned int tid = std::get<0>(mSelectEntries[i]);
		unsigned int addr = pageAddrs[baseoffset + tid];
		unsigned char *record = mpTables[tid]->records().get_record(addr);

		table_attr_desc_t *desc = std::get<1>(mSelectEntries[i]);
		db::print_record(desc, record);
		printf("\t");
	}
}

template<unsigned int PAGESIZE>
inline void QueryExecution<PAGESIZE>::execute_select_aggregate_all(
	std::vector<unsigned int>& pageAddrs, 
	unsigned int depth)
{
	unsigned int page_addr;
	Table::fast_iterator it(mpTables[depth]);
	while (it.next(&page_addr) != NULL)
	{
		pageAddrs[depth] = page_addr;
		if (depth == 0)
		{
			execute_select_aggregate_entries(pageAddrs, 0);
		}
		else
		{
			execute_select_aggregate_all(pageAddrs, depth - 1);
		}
	}
}

template<unsigned int PAGESIZE>
inline void QueryExecution<PAGESIZE>::execute_select_aggregate_entries(
	std::vector<unsigned int> pageAddrs, 
	unsigned int baseoffset)
{
	for (unsigned int i = 0; i < mSelectEntries.size(); i++)
	{
		unsigned int tid = std::get<0>(mSelectEntries[i]);
		unsigned int addr = pageAddrs[baseoffset + tid];
		unsigned char *record = mpTables[tid]->records().get_record(addr);
		table_attr_desc_t *desc = std::get<1>(mSelectEntries[i]);

		SelectEntryType type = std::get<3>(mSelectEntries[i]);
		switch (type)
		{
		case COUNT:
			mAggregationCounter[i]++;
			break;
		case SUM:
			mAggregationCounter[i] += db::parse_int(record, *desc);
			break;
		default:
			throw QueryException(EXPR_SYNTAX_ERROR, "Syntax error on aggrgation query.");
			break;
		}
	}
}

template<unsigned int PAGESIZE>
inline std::pair<table_attr_desc_t *, unsigned int> QueryExecution<PAGESIZE>::match_col(const sql::Expr & expr)
{
	return match_col(expr.table, expr.name);
}

template<unsigned int PAGESIZE>
inline std::pair<table_attr_desc_t*, unsigned int> QueryExecution<PAGESIZE>::match_col(const char * table, const char * name)
{
	unsigned int tid = 0;
	table_attr_desc_t *desc = NULL;
	if (table != NULL)
	{
		auto result = mTableMap.find(table);
		if (result != mTableMap.end())
		{
			tid = result->second;
			desc = mpTables[tid]->tablefile().get_attr_desc(name);
			if (desc == NULL)
				throw QueryException(SELECT_COLUMN_UNDEFINED, name);
		}
		else
			throw QueryException(SELECT_TABLE_UNDEFINED, table);
	}
	else
	{
		int ref_cnt = 0;
		for (unsigned int i = 0; i < mTableNum; i++)
		{
			desc = mpTables[tid]->tablefile().get_attr_desc(name);
			if (desc != NULL)
			{
				tid = i;
				ref_cnt++;
				if (ref_cnt > 1)
					throw QueryException(SELECT_COLUMN_AMBIGUOUS, name);
			}
		}

		if (ref_cnt == 0)
			throw QueryException(SELECT_COLUMN_UNDEFINED, name);
	}
	return std::pair<table_attr_desc_t *, unsigned int>(desc, tid);
}

template<unsigned int PAGESIZE>
inline void QueryExecution<PAGESIZE>::parse_select_entry(
	sql::Expr & expr,
	SelectEntryType type)
{
	if (expr.type == sql::kExprStar)
	{
		for (int i = 0; i < mTableNum; i++)
		{
			for (int j = 0; j < mpTables[i]->tablefile().get_table_header().attrNum; j++)
			{
				assert(mpTables[i] != NULL);
				
				table_attr_desc_t *desc = mpTables[i]->tablefile().get_attr_desc(j);
				mSelectEntries.push_back(SelectEntry
					(i, desc, desc->name, type));
			}
		}
	}
	else if (expr.type == sql::kExprColumnRef)
	{
		auto desc = match_col(expr);
		assert(desc.second < mTableNum);

		mSelectEntries.push_back(SelectEntry
			(desc.second, desc.first, expr.getName(), type));
	}
	else
		throw QueryException(EXPR_SYNTAX_ERROR);
}

template<unsigned int PAGESIZE>
inline void QueryExecution<PAGESIZE>::parse_token(
	const table_attr_desc_t * pAttrDesc, 
	unsigned char * pRecord)
{
	if (pAttrDesc->type == ATTR_TYPE_INTEGER)
	{
		mParseStack.push(StmtToken(db::parse_int(pRecord, *pAttrDesc)));
	}
	else if (pAttrDesc->type == ATTR_TYPE_VARCHAR)
	{
		mParseStack.push(StmtToken(db::parse_varchar(pRecord, *pAttrDesc)));
	}
	else
	{
		Warning("WARNING: Use NULL Type as Table schema.\n");
		mParseStack.push(StmtToken());
	}
}

template<unsigned int PAGESIZE>
inline const char * QueryExecution<PAGESIZE>::gen_select_column_name(
	char * buff, 
	const char * table, 
	const char * col, 
	SelectEntryType type)
{
	assert(buff != NULL);

	switch (type)
	{
	case COLUMN:
		if (table != NULL) sprintf(buff, "%s.%s", table, col);
		else sprintf(buff, "%s", col);
		break;
	case COUNT:
		if (table != NULL) sprintf(buff, "COUNT(%s.%s)", table, col);
		else sprintf(buff, "COUNT(%s)", col);
		break;
	case SUM:
		if (table != NULL) sprintf(buff, "SUM(%s.%s)", table, col);
		else sprintf(buff, "SUM(%s)", col);
		break;
	default:
		throw QueryException(EXPR_SYNTAX_ERROR);
	}
	return buff;
}

template<unsigned int PAGESIZE>
inline void QueryExecution<PAGESIZE>::print_select_entries(std::vector<SelectEntry> &selectEntries)
{
	for (int i = 0; i < selectEntries.size(); i++)
	{
		printf("%s\t", std::get<2>(selectEntries[i]).c_str());
	}
	putchar('\n');
}

template<unsigned int PAGESIZE>
inline void QueryExecution<PAGESIZE>::print_aggregation_counter()
{
	for (int i = 0; i < mAggregationCounter.size(); i++)
	{
		printf("%lld\t", mAggregationCounter[i]);
	}
	putchar('\n');
}
