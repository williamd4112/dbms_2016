#include "DatabaseLite.h"

#define UNKOWN_STMT_TYPE 0x1
#define UNEXPECTED_ERROR 0x2
#define AMBIGUOUS_ERROR 0x3

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

void DatabaseLite::exec(std::string & command, bool profile)
{
	sql::SQLParserResult *parser = sql::SQLParser::parseSQLString(command);
	try
	{
		if (parser->isValid)
		{
			clock_t begin, end;
			double time_spent;

			begin = clock();

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

			if (profile)
			{
				end = clock();
				time_spent = (double)(end - begin) / CLOCKS_PER_SEC;
				printf("Time elapsed: %lf\n", time_spent);
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

void DatabaseLite::exec(std::string & command)
{
	exec(command, false);
}

void DatabaseLite::exec_create_index(std::string tablename, std::string attrname, IndexType type)
{
	LightTable & table = mDbf.get_table(tablename);
	table.create_index(attrname.c_str(), type);
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
	AttrTuple tuple(table_ref.tuple_size());
	for (int i = 0; i < tuple.size(); i++)
		tuple[i].init_as((table_ref.get_attr_type(i) == ATTR_TYPE_INTEGER) ? INTEGER_DOMAIN : VARCHAR_DOMAIN);


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

	std::vector<std::pair<sql::TableRef *, LightTable*>> from_tables; // At most two table, use linear search faster
	std::vector<std::vector<AddrPair>> where_addr_pairs;
	std::pair<LightTable *, LightTable *> table_comb;
	std::vector<std::tuple<LightTable *, int, int, DatabaseAggregateType, bool>> select_cols;
	std::vector<std::tuple<LightTable *, int, int, DatabaseAggregateType, bool>> aggre_list; //table, tuple id, col id, type

	std::vector<sql::Expr*> * select_clause = select_stmt.selectList;

	parse_from_clause(select_stmt.fromTable, from_tables);

	if (select_stmt.hasWhere())
	{
		parse_where_clause(select_stmt.whereClause, from_tables, where_addr_pairs, table_comb);
	}
	else
	{
		if (from_tables.size() == 1)
			table_comb.first = table_comb.second = 0;
		else if (from_tables.size() == 2)
			table_comb = { from_tables[0].second, from_tables[1].second};
		else
			throw exception_t(UNEXPECTED_ERROR, "No table selected");
	}
	
	// Select stage
	if (select_stmt.hasAggregation())
	{
		std::vector<sql::AggregationFunction*> *func_list = select_stmt.aggregation_list;
		for (int i = 0; i < func_list->size(); i++)
		{
			sql::Expr *col_ref = select_stmt.aggregation_list->at(i)->attribute;
			DatabaseAggregateType aggre_type = func_list->at(i)->type == sql::AggregationFunction::kCount ? COUNT : SUM;
			parse_select_entry(col_ref, from_tables, table_comb, aggre_type, aggre_list);
		}

		std::vector<int> aggre_counters(aggre_list.size(), 0);

		if (select_stmt.hasWhere())
		{
			for (auto pair : where_addr_pairs.back())
			{
				for (int i = 0; i < aggre_counters.size(); i++)
				{
					exec_select_aggre(pair, aggre_list[i], aggre_counters[i]);
				}
			}

			for (int i = 0; i < aggre_counters.size(); i++)
				std::cout << aggre_counters[i] << "\t";
			std::cout << "\n";
		}
		else
		{
			if (from_tables.size() == 2)
			{
				LightTable *tables[2] = { from_tables[0].second , from_tables[1].second };
				for (int ai = 0; ai < tables[0]->size(); ai++)
				{
					for (int bi = 0; bi < tables[1]->size(); bi++)
					{
						for (int i = 0; i < aggre_list.size(); i++)
						{
							exec_select_aggre({ ai, bi }, aggre_list[i], aggre_counters[i]);
						}
					}
				}
				for (int i = 0; i < aggre_counters.size(); i++)
					std::cout << aggre_counters[i] << "\t";
				std::cout << "\n";
			}
			else if(from_tables.size() == 1)
			{
				LightTable *tables[1] = { from_tables[0].second };
				for (int ai = 0; ai < tables[0]->size(); ai++)
				{
					for (int i = 0; i < aggre_list.size(); i++)
					{
						exec_select_aggre({ ai, ai }, aggre_list[i], aggre_counters[i]);
					}
				}

				for (int i = 0; i < aggre_counters.size(); i++)
					std::cout << aggre_counters[i] << "\t";
				std::cout << "\n";
			}
			else
			{
				throw exception_t(UNEXPECTED_ERROR, "No table selected");
			}
		}
	}
	else
	{
		for (int i = 0; i < select_clause->size(); i++)
		{
			sql::Expr * col_ref = select_clause->at(i);
			parse_select_entry(col_ref, from_tables, table_comb, NO_AGGRE, select_cols);
		}

		if (select_stmt.hasWhere())
		{
			for (auto pair : where_addr_pairs.back())
			{
				for (auto col : select_cols)
				{
					int addr = (std::get<1>(col) == 0) ? pair.first : pair.second;
					int colid = std::get<2>(col);
					LightTable * bind_table = std::get<0>(col);

					auto & tuple = bind_table->get_tuple(addr);
					std::cout << tuple.at(colid) << "\t";
				}
				std::cout << "\n";
			}
		}
		else
		{
			if (from_tables.size() == 2)
			{
				LightTable *tables[2] = { from_tables[0].second , from_tables[1].second };
				for (int ai = 0; ai < tables[0]->size(); ai++)
				{
					for (int bi = 0; bi < tables[1]->size(); bi++)
					{
						for (auto col : select_cols)
						{
							int addr = (std::get<1>(col) == 0) ? ai : bi;
							int colid = std::get<2>(col);
							LightTable * bind_table = std::get<0>(col);

							auto & tuple = bind_table->get_tuple(addr);
							std::cout << tuple.at(colid) << "\t";
						}
						std::cout << "\n";
					}
				}
			}
			else if (from_tables.size() == 1)
			{
				LightTable *tables[1] = { from_tables[0].second };
				for (int ai = 0; ai < tables[0]->size(); ai++)
				{
					for (auto col : select_cols)
					{
						int addr = ai;
						int colid = std::get<2>(col);
						LightTable * bind_table = std::get<0>(col);

						auto & tuple = bind_table->get_tuple(addr);
						std::cout << tuple.at(colid) << "\t";
					}
					std::cout << "\n";
				}
			}
		}
	}
}

void DatabaseLite::exec_select_aggre(std::pair<int, int> pair, SelectEntry aggre_ent, int & aggre_counter)
{
	bool isStar = std::get<4>(aggre_ent);
	DatabaseAggregateType aggre_type = std::get<3>(aggre_ent);

	if (isStar && aggre_type == SUM)
		throw exception_t(UNEXPECTED_ERROR, "Sum(*) illegal");
	if (isStar)
	{
		switch (aggre_type)
		{
		case COUNT:
			aggre_counter++;
			break;
		default:
			throw exception_t(UNEXPECTED_ERROR, "Sum(*) illegal");
		}
	}
	else
	{
		int addr = (std::get<1>(aggre_ent) == 0) ? pair.first : pair.second;
		int colid = std::get<2>(aggre_ent);
		LightTable * bind_table = std::get<0>(aggre_ent);
		auto & tuple = bind_table->get_tuple(addr);
		uint8_t attr_type = bind_table->get_attr_type(colid);

		switch (aggre_type)
		{
		case COUNT:
			if (attr_type == ATTR_TYPE_INTEGER ||
				(attr_type == ATTR_TYPE_VARCHAR
					&& tuple.at(colid).size() > 0))
				aggre_counter++;
			break;
		case SUM:
			if (attr_type == ATTR_TYPE_VARCHAR)
				throw exception_t(UNEXPECTED_ERROR, "Varchar cannot sum.");
			aggre_counter += tuple.at(colid).Int();
			break;
		default:
			break;
		}
	}
}

void DatabaseLite::parse_select_entry(
	sql::Expr *col_ref, 
	std::vector<FromEntry> & from_tables,
	std::pair<LightTable *, LightTable *> & table_comb,
	DatabaseAggregateType aggre_type,
	std::vector<std::tuple<LightTable *, int, int, DatabaseAggregateType, bool>> & select_cols
	)
{
	if (col_ref->type == sql::kExprStar)
	{
		if (aggre_type == COUNT || aggre_type == SUM)
		{
			if (col_ref->hasTable())
			{
				for (int i = 0; i < 2 && i < from_tables.size(); i++)
				{
					if (strcmp(col_ref->table, from_tables[i].first->name) != 0)
						continue;
					if (col_ref->hasAlias() && from_tables[i].first->alias != NULL
						&& strcmp(col_ref->alias, from_tables[i].first->alias) != 0)
						continue;

					int comb_id = i;
					select_cols.emplace_back(std::make_tuple(from_tables[i].second, comb_id, -1, aggre_type, true));
					break;
				}
			}
			else
			{
				select_cols.emplace_back(std::make_tuple(nullptr, -1, -1, aggre_type, true));
			}
		}
		else
		{
			for (int i = 0; i < 2 && i < from_tables.size(); i++)
			{
				if (col_ref->hasTable())
				{
					if (strcmp(col_ref->table, from_tables[i].first->name) != 0)
						continue;
					if (col_ref->hasAlias() && from_tables[i].first->alias != NULL
						&& strcmp(col_ref->alias, from_tables[i].first->alias) != 0)
						continue;
				}
				int comb_id = i;
				for (int j = 0; j < from_tables[i].second->tuple_size(); j++)
					select_cols.emplace_back(std::make_tuple(from_tables[i].second, comb_id, j, aggre_type, false));
			}
		}
	}
	else
	{
		// First, second
		LightTable * bind_table = match_table(col_ref, from_tables);
		if (bind_table == NULL)
		{
			throw exception_t(UNEXPECTED_ERROR, "Table not found.");
		}
		int comb_id = (bind_table == table_comb.first) ? 0 : 1;

		// Tuple element id
		int tuple_ele_id = bind_table->get_attr_id(col_ref->name);
		select_cols.emplace_back(std::make_tuple(bind_table, comb_id, tuple_ele_id, aggre_type, false));
	}
}

void DatabaseLite::parse_from_clause(
	std::vector<sql::TableRef*> * from_clause, 
	std::vector<FromEntry> & from_tables)
{
	for (int i = 0; i < from_clause->size(); i++)
	{
		sql::TableRef *ref = from_clause->at(i);
		LightTable & table_ref = mDbf.get_table(ref->name);

		// if duplicated table exist, insertion failed
		from_tables.push_back({ ref, &table_ref });
	}
}

void DatabaseLite::parse_where_clause(
	sql::Expr * where_clause, 
	std::vector<std::pair<sql::TableRef*, LightTable*>> & from_tables,
	std::vector<std::vector<AddrPair>> & where_addr_pairs,
	std::pair<LightTable *, LightTable *> & table_comb)
{
	std::vector<std::pair<LightTable *, LightTable *>> tableCombs; // Used to check orders before merge
	std::stack<sql::Expr *> tokenStack;
	std::stack<sql::Expr *> opStack;
	std::stack<sql::Expr *> inputStack;
	
	inputStack.push(where_clause);

	while (!inputStack.empty())
	{
		sql::Expr *expr = inputStack.top(); inputStack.pop();
		sql::Expr * childs[2] = { expr->expr , expr->expr2 };
		switch (expr->type)
		{
		case sql::kExprOperator: // TODO: handle UMINUS
			if (expr->type == sql::Expr::UMINUS)
			{
				if(expr->expr == NULL) 
					throw exception_t(UNEXPECTED_ERROR, "Where statement parsing error: unexpected literal.");
				if(expr->expr->type != sql::kExprLiteralInt)
					throw exception_t(UNEXPECTED_ERROR, "Where statement parsing error: unexpected literal.");
				expr->expr->ival = -expr->expr->ival;
				tokenStack.push(expr->expr);
			}
			else
				opStack.push(expr);
			break;
		case sql::kExprColumnRef: case sql::kExprLiteralInt: case sql::kExprLiteralString:
			tokenStack.push(expr);
			break;
		default:
			throw exception_t(UNEXPECTED_ERROR, "Where statement parsing error: unexpected expr type.");
		}

		for (int i = 0; i < 2; i++)
		{
			if (childs[i] != NULL)
				inputStack.push(childs[i]);
		}
	}

	while (!opStack.empty())
	{
		sql::Expr * expr = opStack.top(); opStack.pop();
		assert(expr != NULL);

		// AND, OR
		if ((expr->op_type == sql::Expr::AND || expr->op_type == sql::Expr::OR) && where_addr_pairs.size() >= 2)
		{
			if(where_addr_pairs.size() != 2)
				throw exception_t(UNEXPECTED_ERROR, "No correct number of pairs when merge.");
			/// TODO: Check order
			// Check order (force them to be in same)
			auto oit = tableCombs.rbegin();
			auto & comb1 = *oit;
			oit++;
			auto & comb2 = *oit;

			where_addr_pairs.resize(where_addr_pairs.size() + 1);
			std::vector<LightTable *> candidate_tables;
			for (auto from_table : from_tables)
				candidate_tables.emplace_back(from_table.second);
			merge_type_t merge_type = (expr->op_type == sql::Expr::AND) ? AND : OR;
			table_comb = LightTable::merge(
				comb2,
				where_addr_pairs.at(0),
				merge_type, 
				comb1,
				where_addr_pairs.at(1),
				where_addr_pairs.back(),
				candidate_tables);
		}
		// =, <>, <, >
		else if (tokenStack.size() >= 2)
		{
			sql::Expr *operands[2] = { 0 };
			operands[0] = tokenStack.top(); tokenStack.pop();
			operands[1] = tokenStack.top(); tokenStack.pop();

			// Two-way join
			if (operands[0]->type == sql::kExprColumnRef && operands[1]->type == sql::kExprColumnRef)
			{
				LightTable *tables[2] = { 0 };
				for (int i = 0; i < from_tables.size(); i++)
					tables[i] = match_table(operands[i], from_tables);

				if (tables[0] == tables[1])
				{
					where_addr_pairs.resize(where_addr_pairs.size() + 1);
					tableCombs.emplace_back(
						LightTable::join_self(
							*tables[0],
							operands[0]->name,
							expr_op_to_rel(expr),
							operands[1]->name,
							where_addr_pairs.back())
					);

					// if has two table, expand it
					table_comb.first = table_comb.second = tables[0];
				}
				else
				{
					where_addr_pairs.resize(where_addr_pairs.size() + 1);
					tableCombs.emplace_back(LightTable::join_cross(
						*tables[0],
						operands[0]->name,
						expr_op_to_rel(expr),
						*tables[1],
						operands[1]->name,
						where_addr_pairs.back()));
					table_comb.first = tables[0];
					table_comb.second = tables[1];
				}
			}
			// Constant expression
			else if (operands[0]->type != sql::kExprColumnRef && operands[1]->type != sql::kExprColumnRef)
			{
				throw exception_t(UNEXPECTED_ERROR, "Constant expression is forbidden.");
			}
			// Constant join Colref
			else if (operands[1]->type == sql::kExprLiteralInt || operands[1]->type == sql::kExprLiteralString)
			{
				// Join(colref, k)
				LightTable *table = match_table(operands[0], from_tables);
				LightTable *onto_table;
				for (auto pair : from_tables)
					if (pair.second != table)
						onto_table = pair.second;
	
				where_addr_pairs.resize(where_addr_pairs.size() + 1);
				tableCombs.emplace_back(LightTable::join_self(
					*table,
					operands[0]->name,
					expr_op_to_rel(expr),
					expr_to_attr(operands[1]),
					where_addr_pairs.back()));

				// if has two table, expand it
				table_comb.first = table_comb.second = table;
			}
			else
			{
				throw exception_t(UNEXPECTED_ERROR, "Left hand side cannot be a constant.");
			}
		}
	}

	if (from_tables.size() == 2)
	{
		// Two table from, but where one, product it
		if (table_comb.first == table_comb.second)
		{
			where_addr_pairs.resize(where_addr_pairs.size() + 1);
			std::vector<AddrPair> & pairs = where_addr_pairs.at(where_addr_pairs.size() - 2);

			LightTable *other = (from_tables[0].second == table_comb.first) ? from_tables[1].second : from_tables[0].second;
			
			table_comb.second = other;
			
			for (int i = 0; i < pairs.size(); i++)
			{
				for (int j = 0; j < other->size(); j++)
				{
					where_addr_pairs.back().emplace_back(i, j);
				}
			}
		}
	}
}

LightTable * DatabaseLite::match_table(sql::Expr * colref, std::vector<std::pair<sql::TableRef*, LightTable*>> & from_tables)
{
	// Named colref 
	if (colref->hasTable())
	{
		for (auto & pair : from_tables)
		{
			// Match with real-name
			if (strcmp(colref->table, pair.first->name) == 0)
			{
				if (pair.second->has_attr(colref->name))
					return pair.second;
				else
					throw exception_t(UNEXPECTED_ERROR, "Attribute not found in table.");
			}

			// Match with alias
			if (pair.first->alias != NULL && strcmp(colref->table, pair.first->alias) == 0)
			{
				if (pair.second->has_attr(colref->name))
					return pair.second;
				else
					throw exception_t(UNEXPECTED_ERROR, "Attribute not found in table.");
			}
		}
	}
	// Unnamed colref
	else
	{
		LightTable *match_table = NULL;
		int cnt = 0;
		for (auto & pair : from_tables)
		{
			if (pair.second->has_attr(colref->name))
			{
				if (cnt > 0)
					throw exception_t(AMBIGUOUS_ERROR, "Ambiguous attribute name");
				match_table = pair.second;
				cnt++;
			}
		}

		if (match_table != NULL)
			return match_table;
	}

	throw exception_t(UNEXPECTED_ERROR, "Attribute name cannot match.");
}

relation_type_t DatabaseLite::expr_op_to_rel(sql::Expr * expr_op)
{
	assert(expr_op->type == sql::kExprOperator);
	switch (expr_op->op_type)
	{
	case sql::Expr::SIMPLE_OP:
	{
		switch (expr_op->op_char)
		{
		case '=': return EQ;
		case '<': return LESS;
		case '>': return LARGE;
		default:
			throw exception_t(UNEXPECTED_ERROR, "Operator type conversion error.");
		}
	}
	break;
	case sql::Expr::NOT_EQUALS: return NEQ;
	default:
		throw exception_t(UNEXPECTED_ERROR, "Operator type conversion error.");
	}
}

attr_t DatabaseLite::expr_to_attr(sql::Expr * expr)
{
	switch (expr->type)
	{
	case sql::kExprLiteralInt: return expr->ival;
	case sql::kExprLiteralString: return expr->name;
	default: throw exception_t(UNEXPECTED_ERROR, "Expression convert to attr error, unknown expr type.");
	}
}

bool DatabaseLite::eval_constant_op(sql::Expr * a, sql::Expr * b, sql::Expr *op)
{
	if (a->type != b->type)
		throw exception_t(UNEXPECTED_ERROR, "Integer cannot compare to String");
	assert(op != NULL && op->type == sql::kExprOperator);
	
	if (a->type == sql::kExprLiteralInt)
	{
		switch (op->op_type)
		{
		case sql::Expr::SIMPLE_OP:
		{
			switch (op->op_char)
			{
			case '=': return a->ival == b->ival;
			case '<': return a->ival < b->ival;
			case '>': return a->ival > b->ival;
			default:
				throw exception_t(UNEXPECTED_ERROR, "Operator type conversion error.");
			}
		}
		break;
		case sql::Expr::NOT_EQUALS: return a->ival != b->ival;
		default:
			throw exception_t(UNEXPECTED_ERROR, "Operator type conversion error.");
		}
	}
	else
	{
		if (op->op_type == sql::Expr::SIMPLE_OP && op->op_char == '=')
			return strcmp(a->name, b->name);
		else
			throw exception_t(UNEXPECTED_ERROR, "String only support equal");
	}

	return false;
}
