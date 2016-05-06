#pragma once

#include "SQLParser.h"

#include <stack>

namespace sql
{
	/*
		SQLExprParser

		This parser is used to parse logic expression

		for example, WHERE statement (a.id < b.id AND a.score == b.score)

		T must provide ==, <, >
	*/
	template <class T>
	class SQLExprParser
	{
	public:
		SQLExprParser(Expr * where_clause);
		~SQLExprParser();

	private:
		enum OperatorType
		{
			NEG, AND, OR, EQ, NEQ, LESS, LARGE
		};

		std::stack<OperatorType> mOpStack;
		std::stack<T> mTokenStack;
	};

}
