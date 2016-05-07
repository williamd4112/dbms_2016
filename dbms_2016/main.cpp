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

#include "test.h"

using namespace std;
using namespace sql;

#define ARGC_FILE_IMPORT 2
#define ARGV_FILE_IMPORT ARGC_FILE_IMPORT - 1

void test_db_1();

static void parse_stream(istream &ifs, bool is_prompt);
static void parse_file(const char *);

struct TestObject
{
	string sval;
	int ival;
};

void test_string()
{
	unordered_multimap<int, TestObject> hmmap;
	for (int i = 0; i < 10000000; i++)
	{
		hmmap.insert(pair<int, TestObject>(i, TestObject{"Hello", 1}));
	}
}

void test_charpointer()
{
	unordered_multimap<int, char*> hmmap;
	for (int i = 0; i < 10000000; i++)
	{
		hmmap.insert(pair<int, char*>(i, new char[40]));
	}
}

void test_unorderedmap()
{
	unordered_multimap<int, string> hmmap;
	hmmap.insert(pair<int, string>(1, "Book1"));
	hmmap.insert(pair<int, string>(1, "Book1_1"));
	hmmap.insert(pair<int, string>(1, "Book1_2"));
	hmmap.insert(pair<int, string>(2, "Book2_1"));
	hmmap.insert(pair<int, string>(2, "Book2_2"));
	hmmap.insert(pair<int, string>(3, "Book3_1"));
	hmmap.insert(pair<int, string>(3, "Book3_1"));
	hmmap.insert(pair<int, string>(4, "Book4_1"));
	hmmap.insert(pair<int, string>(5, "Book5_1"));

	auto start = hmmap.equal_range(6);

	for (auto it = start.first; it != start.second; it++)
	{
		cout << it->first << " -> " << it->second << endl;
	}


	system("pause");

}

void test_multimap()
{
	multimap<int, string> mmap;
	mmap.insert(pair<int, string>(1, "Book1"));
	mmap.insert(pair<int, string>(1, "Book1_1"));
	mmap.insert(pair<int, string>(1, "Book1_2"));
	mmap.insert(pair<int, string>(2, "Book2_1"));
	mmap.insert(pair<int, string>(2, "Book2_2"));
	mmap.insert(pair<int, string>(3, "Book3_1"));
	mmap.insert(pair<int, string>(3, "Book3_1"));
	mmap.insert(pair<int, string>(4, "Book4_1"));
	mmap.insert(pair<int, string>(5, "Book5_1"));

	multimap<int, string>::iterator start = mmap.lower_bound(1);
	multimap<int, string>::iterator end = mmap.upper_bound(3);

	for (multimap<int, string>::iterator it = start; it != end; it++)
	{
		cout << it->first << " -> " << it->second << endl;
	}

}

void test_multimap2()
{
	multimap<int, string> mmap;
	for (int i = 0; i < 10000000; i++)
	{
		mmap.insert(pair<int, string>(i, "A"));
	}
}

void test_hashindex_write()
{
	HashIndexFile hindex(INTEGER_DOMAIN, sizeof(int));
	hindex.open("hashindex.idx", "w+");
	for (int i = 0; i < 10; i++)
	{
		hindex.set(attr_t(i % 10), i * 10);
	}
	hindex.dump();
	hindex.write_back();
}

void test_hashindex_read()
{
	HashIndexFile hindex(INTEGER_DOMAIN, sizeof(int));
	hindex.open("hashindex.idx", "r+");
	hindex.read_from();
	hindex.dump();
}

void test_pkindex_write()
{
	PrimaryIndexFile pkindexx(VARCHAR_DOMAIN, 20);
	pkindexx.open("pkindex.idx", "w+");
	
	char buff[ATTR_SIZE_MAX];
	for (int i = 0; i < 100; i++)
	{
		sprintf(buff, "Book%d",i);
		pkindexx.set(attr_t(buff), i);
	}
	pkindexx.dump();
	pkindexx.write_back();
}

void test_pkindex_read()
{
	PrimaryIndexFile pkindexx(VARCHAR_DOMAIN, 20);
	pkindexx.open("pkindex.idx", "r+");
	pkindexx.read_from();
	pkindexx.dump();
}

void test_treeindex_write()
{
	TreeIndexFile tindex(INTEGER_DOMAIN, sizeof(int));
	tindex.open("tindex.idx", "w+");

	for (int i = 0; i < 100; i++)
	{
		tindex.set(i, i * 10);
	}
	tindex.dump();
	tindex.write_back();
}

void test_treeindex_read()
{
	TreeIndexFile tindex(INTEGER_DOMAIN, sizeof(int));
	tindex.open("tindex.idx", "r+");
	tindex.read_from();
	tindex.dump();
}

void test_treeindex_read_range()
{
	TreeIndexFile tindex(INTEGER_DOMAIN, sizeof(int));
	tindex.open("tindex.idx", "r+");
	tindex.read_from();
	
	vector<uint32_t> addrs;
	tindex.get(10, 20, addrs);

	for (vector<uint32_t>::iterator it = addrs.begin(); it != addrs.end(); it++)
		cout << *it << endl;
}

void test_createindex()
{
	Database<PAGESIZE_8K> database("database_test.dbs");
	database.execute(std::string("CREATE TABLE Employee (id INT PRIMARY KEY, name VARCHAR(20), addr VARCHAR(20), gender VARCHAR(15))"));
	//database.create_index("Employee", "name", "Emp_name.idx", HASH);
	char buff[512];
	for (int i = 0; i < 1000; i++)
	{
		sprintf(buff, "INSERT INTO Employee VALUES (%d, 'Apple%d', 'Addr%d', 'Male');", i, i, i);
		database.execute(std::string(buff));
	}
	//database.execute(std::string("SELECT Employee.id, Employee.name FROM Employee;"));
}

void test_no_page()
{
	std::unordered_map<int, int> m;
	std::vector<attr_t> v(1000000);
	char name[255], addr[255];
	for (int i = 0; i < 1000000; i++)
	{
		//v.push_back(structure_record());
		v[i] = i;
		m[i] = i;
	}
}

void test_seq_file()
{
	const SequenceElementType types[] = {
		SEQ_INT, SEQ_VARCHAR, SEQ_VARCHAR, SEQ_INT
	};

	char name[255], addr[255];

	std::unordered_map<int, int> m;
	SequenceFile<attr_t> seqfile(types, 4);
	seqfile.open("seqfile.seq", "w+");
	std::vector<attr_t> tuple(4);

	for (int i = 0; i < 1000; i++)
	{
		sprintf(name, "Name%d", i);
		sprintf(addr, "Addr%d", i);
		tuple[0] = i;
		tuple[1] = name;
		tuple[2] = addr;
		tuple[3] = i * 10;
		seqfile.put(tuple);
		m[tuple[0].Int()] = i;
	}

	for (auto it = seqfile.begin(); it != seqfile.end(); it++)
	{
		for (auto e = it->begin(); e != it->end(); e++)
		{
			std::cout << *e << "\t";
		}
		std::cout << std::endl;
	}
	seqfile.write_back();
}

void test_seq_file_read()
{
	const SequenceElementType types[] = {
		SEQ_INT, SEQ_VARCHAR, SEQ_VARCHAR, SEQ_INT
	};

	SequenceFile<attr_t> seqfile(types, 4);
	seqfile.open("seqfile.seq", "r+");
	seqfile.read_from();
	for (auto it = seqfile.begin(); it != seqfile.end(); it++)
	{
		for (auto e = it->begin(); e != it->end(); e++)
		{
			std::cout << *e << "\t";
		}
		std::cout << std::endl;
	}
}

void test_light_tablefile_write()
{
	table_attr_desc_t descs[4] = {
		{ "ID", ATTR_TYPE_INTEGER, offsetof(structure_record, id), 4, ATTR_CONSTRAINT_PRIMARY_KEY },
		{ "Name", ATTR_TYPE_VARCHAR, offsetof(structure_record, name), 40, 0 },
		{ "Addr", ATTR_TYPE_VARCHAR, offsetof(structure_record, addr), 5, 0 },
		{ "Gender", ATTR_TYPE_INTEGER, offsetof(structure_record, gender), 4, 0 }
	};

	LightTableFile ltbf("LightTable", descs, 4);
	ltbf.open("LightTable.tbl", "wb+");
	ltbf.dump_info();
	ltbf.write_back();
}

void test_light_tablefile_read()
{
	LightTableFile ltbf;
	ltbf.open("LightTable.tbl", "rb+");
	ltbf.read_from();
	ltbf.dump_info();
	ltbf.write_back();
}

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
	tbl.find("Grade", attr_t(9900), LARGE, match_addrs);

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

	for (int i = 0; i < 20; i++)
	{
		sprintf(bookname, "Name%d", i);
		t2_tuple[0] = i;
		t2_tuple[1] = bookname;
		t2_tuple[2] = (i % 10) * 10;
		t2.insert(t2_tuple);
	}
}


void test_join_naive()
{
	const char *a_select[] = { "ID" };
	const char *b_select[] = { "BookID", "BookName" };
	std::vector<AddrPair> match_addrs;

	LightTable::join_naive(t1, "ID", NEQ, t2, "BookID", match_addrs);

	LightTable::select(
		t1, std::vector<std::string>(a_select, a_select + 1),
		t2, std::vector<std::string>(b_select, b_select + 2),
		match_addrs);
}

void test_join_hash()
{
	const char *a_select[] = { "Name" , "Grade"};
	const char *b_select[] = { "BookID", "BookName", "BookGrade" };

	std::vector<AddrPair> match_addrs_id;
	std::vector<AddrPair> match_addrs_name;
	std::vector<AddrPair> matchs;

	LightTable::join(t1, "Grade", LARGE, t2, "BookGrade", match_addrs_id);
	//LightTable::join(t1, "Name", EQ, t2, "BookName", match_addrs_name);
	//LightTable::merge(match_addrs_id, AND, match_addrs_name, matchs);

	LightTable::select(
		t1, std::vector<std::string>(a_select, a_select + 2),
		t2, std::vector<std::string>(b_select, b_select + 3),
		match_addrs_id);
	printf("# %d\n", match_addrs_id.size());

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

static Database<PAGESIZE_8K> database("database.dbs");

/*
	TODO: Be more careful about address space
	TODO: ';' in insert string will cause problem
*/
int main(int argc, char *argv[])
{
	//printf("Insertion done\n");
	test_lite_table_join();
	profile_pefromance(test_join_hash);
	//profile_pefromance(test_join_naive);
	
	system("pause");
	return 0;

	quiet = false;

	int i = 1;
	if (i < argc)
	{
		if (strcmp("-w", argv[i]) == 0)
		{
			i++;
			if (i + 1 < argc)
			{
				freopen(argv[i], "w", stdout);
				i++;
				pause_at_exit = false;
				quiet = true;
			}
		}
	}

	if (i < argc)
	{
		if (strcmp("-i", argv[i]) == 0)
		{
			i++;
			interactive = true;
		}
	}

	for (; i < argc; i++)
	{
		cout << PROMPT_PREFIX << "load " << argv[i] << endl;
		parse_file(argv[i]);
	}
	
	if(interactive)
		parse_stream(cin, true);

	if(pause_at_exit)
		system("pause");

	return 0;
}

void test_db_1()
{
	Database<PAGESIZE_8K> db("test.dbs");
	db.execute(std::string("CREATE TABLE book (id int, name varchar(20), price int, isbn varchar(15));"));
	db.execute(std::string("CREATE TABLE stu (id int, name varchar(30), score int, addr varchar(15));"));

	char buff[1024];
	for (int i = 0; i < 100; i++)
	{
		sprintf(buff, "INSERT INTO book VALUES (%d, \'Book%d\', %d, '\ISBN%d\');", i, i, i * 10, i);
		db.execute(std::string(buff));
	}

	for (int i = 0; i < 100; i++)
	{
		sprintf(buff, "INSERT INTO stu VALUES (%d, \'Student%d\', %d, '\Road.%d\');", i, i, i * 10, i);
		db.execute(std::string(buff));
	}

	db.execute(std::string("SELECT * FROM book, stu WHERE book.id = stu.id;")); 
}

void parse_stream(istream &ifs, bool is_prompt)
{	
	stringstream input_buff;
	string input_str;
	bool statement_end = false;

	if(is_prompt) cout << PROMPT_PREFIX;
	while (getline(ifs, input_str))
	{
		size_t scan_offset = 0;
		while (scan_offset < input_str.length())
		{
			size_t semicol_pos = input_str.find(";", scan_offset);

			std::string str_part = input_str.substr(scan_offset, semicol_pos - scan_offset);
			input_buff << str_part << std::endl;

			if (semicol_pos == std::string::npos)
				break; // no ';'
			else
			{
				scan_offset = semicol_pos + 1;

				database.execute(input_buff.str());

				input_buff.str("");
				input_buff.clear();
				statement_end = true;
			}
		}
		statement_end = false;
		if (is_prompt) cout << PROMPT_PREFIX;
	}

}

void parse_file(const char *filename)
{
	std::ifstream ifs(filename);
	std::string content((std::istreambuf_iterator<char>(ifs)),
		(std::istreambuf_iterator<char>()));
	database.execute(content);
}
