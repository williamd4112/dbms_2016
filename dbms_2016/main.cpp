#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <ctime>
#include <map>
#include <unordered_map>

#include "DataPage.h"
#include "BitmapPageFreeMapFile.h"
#include "RecordFile.h"
#include "TableFile.h"
#include "RecordTable.h"
#include "DatabaseFile.h"
#include "Bit.h"
#include "Database.h"
#include "IndexFile.h"

#include "system.h"

#include "SQLParser.h"
#include "SQLParserResult.h"
#include "sqlhelper.h"

#include "SequenceFile.h"
#include "LightTableFile.h"
#include "LightTable.h"
#include "DatabaseLite.h"

#include "test.h"

using namespace std;
using namespace sql;

#define ARGC_FILE_IMPORT 2
#define ARGV_FILE_IMPORT ARGC_FILE_IMPORT - 1

//static void parse_stream(istream &ifs, bool is_prompt);
//static void parse_file(const char *);

LightTable tbl;
LightTable t1, t2;

void test_light_table_create()
{
	table_attr_desc_t descs[4] = {
		{ "ID", ATTR_TYPE_INTEGER, offsetof(structure_record, id), 4, ATTR_CONSTRAINT_PRIMARY_KEY },
		{ "Name", ATTR_TYPE_VARCHAR, offsetof(structure_record, name), 40, 0 },
		{ "Addr", ATTR_TYPE_VARCHAR, offsetof(structure_record, addr), 5, 0 },
		{ "Grade", ATTR_TYPE_INTEGER, offsetof(structure_record, gender), 4, 0 }
	};

	tbl.create("TestTable", descs, 4);
	tbl.create_index("Grade", TREE);
	tbl.dump();

	char name[40], addr[40];
	AttrTuple tuple(4);
	for (int i = 0; i < 10000; i++)
	{
		sprintf(name, "name%d", i);
		sprintf(addr, "addr%d", i);
		tuple[0] = i;
		tuple[1] = name;
		tuple[2] = addr;
		tuple[3] = i;
		tbl.insert(tuple);
	}
}

void test_light_table_read()
{
	tbl.load("TestTable");
	tbl.dump();
}

void test_light_table_find()
{
	std::vector<uint32_t> match_addrs;
	tbl.filter("Grade", attr_t(9900), LARGE, match_addrs);

	for (int i = 0; i < match_addrs.size(); i++)
	{
		std::cout << tbl.get_tuple(match_addrs[i]) << std::endl;
	}
}

void test_lite_table_join()
{
	table_attr_desc_t t1_descs[4] = {
		{ "ID", ATTR_TYPE_INTEGER, 0, 4, ATTR_CONSTRAINT_PRIMARY_KEY},
		{ "Name", ATTR_TYPE_VARCHAR, 4, 40, 0 },
		{ "Addr", ATTR_TYPE_VARCHAR, 44, 10, 0 },
		{ "Grade", ATTR_TYPE_INTEGER, 54, 4, 0 }
	};

	table_attr_desc_t t2_descs[3] = {
		{ "BookID", ATTR_TYPE_INTEGER, 0, 4, ATTR_CONSTRAINT_PRIMARY_KEY },
		{ "BookName", ATTR_TYPE_VARCHAR, 4, 40, 0 },
		{ "BookGrade", ATTR_TYPE_VARCHAR, 44, 4, 0 }
	};

	t1.create("table1", t1_descs, 4);
	t1.create_index("Name", TREE);
	t1.create_index("Grade", TREE);

	AttrTuple t1_tuple(4);
	char name[40], addr[40];
	for (int i = 0; i < 10; i++)
	{
		sprintf(name, "Name%d", i);
		sprintf(addr, "Addr%d", i);
		t1_tuple[0] = i;
		t1_tuple[1] = name;
		t1_tuple[2] = addr;
		t1_tuple[3] = i * 10;
		t1.insert(t1_tuple);
	}

	AttrTuple t2_tuple(3);
	char bookname[40];
	t2.create("table2", t2_descs, 3);
	t2.create_index("BookName", TREE);
	t2.create_index("BookGrade", TREE);

	for (int i = 0; i < 1000; i++)
	{
		sprintf(bookname, "Name%d", i);
		t2_tuple[0] = i;
		t2_tuple[1] = bookname;
		t2_tuple[2] = (i % 1000) * 10;
		t2.insert(t2_tuple);
	}
}


void test_join_naive()
{
	const char *a_select[] = { "Name" , "Grade" };
	const char *b_select[] = { "BookID", "BookName", "BookGrade" };

	std::vector<AddrPair> match_addrs_id;
	std::vector<AddrPair> match_addrs_name;
	std::vector<AddrPair> matchs;

	LightTable::join_naive(t1, "Grade", EQ, t2, "BookGrade", match_addrs_id);
	//LightTable::join(t1, "Name", EQ, t2, "BookName", match_addrs_name);
	//LightTable::merge(match_addrs_id, AND, match_addrs_name, matchs);

	//LightTable::select(
	//	t1, std::vector<std::string>(a_select, a_select + 2),
	//	t2, std::vector<std::string>(b_select, b_select + 3),
	//	match_addrs_id);
	printf("Naive # %d\n", match_addrs_id.size());
}

void test_join_hash()
{
	const char *a_select[] = { "Name" , "Grade"};
	const char *b_select[] = { "BookID", "BookName", "BookGrade" };

	std::vector<AddrPair> match_addrs_id;
	std::vector<AddrPair> match_addrs_name;
	std::vector<AddrPair> matchs;

	LightTable::join(t1, "Grade", LESS, attr_t(60), t2, match_addrs_id);
	LightTable::join(t1, "Name", EQ, t2, "BookName", match_addrs_name);
	LightTable::merge(match_addrs_id, AND, match_addrs_name, matchs);

	LightTable::select(
		t1, std::vector<std::string>(a_select, a_select + 2),
		t2, std::vector<std::string>(b_select, b_select + 3),
		matchs);
	printf("Merge # %d\n", match_addrs_id.size());

	//LightTable::select(
	//	t1, std::vector<std::string>(a_select, a_select + 2),
	//	t2, std::vector<std::string>(b_select, b_select + 2),
	//	match_addrs_name);
	//printf("\n");

	//LightTable::select(
	//	t1, std::vector<std::string>(a_select, a_select + 2),
	//	t2, std::vector<std::string>(b_select, b_select + 2),
	//	matchs);
}

static DatabaseLite gDb("LiteDB.dbs");

void test_database_lite_create()
{
	gDb.exec(std::string("CREATE TABLE Book (BookId int PRIMARY KEY, BookName varchar(20), BookPrice int);"));
	gDb.exec(std::string("CREATE TABLE Student (StudentId int PRIMARY KEY, StudentName varchar(20), StudentAddr varchar(40), StudentScore int, StudentDept varchar(40));"));
}

void test_database_lite_insert()
{
	gDb.exec(std::string("INSERT INTO Book VALUES(1, 'Book1', 20);"));
}

/*
	TODO: Be more careful about address space
	TODO: ';' in insert string will cause problem
*/
int main(int argc, char *argv[])
{
	profile(test_lite_table_join);
	profile(test_join_hash);
	//test_database_lite_create();
	//test_database_lite_insert();
	
	system("pause");
	return 0;

	//quiet = false;

	//int i = 1;
	//if (i < argc)
	//{
	//	if (strcmp("-w", argv[i]) == 0)
	//	{
	//		i++;
	//		if (i + 1 < argc)
	//		{
	//			freopen(argv[i], "w", stdout);
	//			i++;
	//			pause_at_exit = false;
	//			quiet = true;
	//		}
	//	}
	//}

	//if (i < argc)
	//{
	//	if (strcmp("-i", argv[i]) == 0)
	//	{
	//		i++;
	//		interactive = true;
	//	}
	//}

	//for (; i < argc; i++)
	//{
	//	cout << PROMPT_PREFIX << "load " << argv[i] << endl;
	//	parse_file(argv[i]);
	//}
	//
	//if(interactive)
	//	parse_stream(cin, true);

	//if(pause_at_exit)
	//	system("pause");

	return 0;
}
