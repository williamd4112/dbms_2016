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
	typedef std::pair<sql::TableRef *, LightTable*> FromEntry;
	typedef std::pair<int, int> SelectEntry;

	DatabaseLite(const char *dbs_filepath);
	~DatabaseLite();

	void exec(std::string & command);
	void exec_create_index(std::string tablename, std::string attrname, IndexType type);
	void load(std::string dbs_filepath);
	void save();
private:
	DatabaseLiteFile mDbf;

	void exec_create(sql::SQLStatement *stmt);
	void exec_insert(sql::SQLStatement *stmt);
	void exec_select(sql::SQLStatement *stmt);

	void parse_select_clause(
		std::vector<sql::Expr *> * select_clause,
		std::vector<FromEntry> & from_tables,
		std::vector<SelectEntry> & select_cols);

	void parse_from_clause(
		std::vector<sql::TableRef*> *from_clause,
		std::vector<FromEntry> & from_tables);

	void parse_where_clause(
		sql::Expr * where_clause, 
		std::vector<std::pair<sql::TableRef *, LightTable*>> & from_tables,
		std::vector<std::vector<AddrPair>> & where_addr_pairs,
		std::pair<LightTable *, LightTable *> & table_comb);

	LightTable * match_table(sql::Expr * colref, std::vector<std::pair<sql::TableRef *, LightTable*>> & from_tables);
	relation_type_t expr_op_to_rel(sql::Expr *expr_op);
	attr_t expr_to_attr(sql::Expr *expr);
	bool eval_constant_op(sql::Expr *a, sql::Expr *b, sql::Expr *op);
};

