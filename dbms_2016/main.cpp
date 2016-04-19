#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <ctime>

#include "DataPage.h"
#include "BitmapPageFreeMapFile.h"
#include "RecordFile.h"
#include "TableFile.h"
#include "RecordTable.h"
#include "DatabaseFile.h"
#include "Bit.h"
#include "Database.h"

#include "system.h"

#include "SQLParser.h"
#include "SQLParserResult.h"
#include "sqlhelper.h"

using namespace std;
using namespace sql;

#define ARGC_FILE_IMPORT 2
#define ARGV_FILE_IMPORT ARGC_FILE_IMPORT - 1

void test_db_1();

static void parse_stream(istream &ifs, bool is_prompt);
static void parse_file(const char *);

static Database<PAGESIZE_8K> database("database.dbs");

/*
	TODO: Be more careful about address space
	TODO: ';' in insert string will cause problem
*/
int main(int argc, char *argv[])
{
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
