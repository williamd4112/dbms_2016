#include <iostream>
#include <string>
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

/*
	TODO: Be more careful about address space
*/
int main(int argc, char *argv[])
{
	Database<PAGESIZE_8K> db("test.dbs");
	db.execute(std::string("CREATE TABLE book (id int, name varchar(20), price int, isbn varchar(15));"));

	char buff[1024];
	for (int i = 0; i < 100; i++)
	{
		sprintf(buff, "INSERT INTO book VALUES (%d, \'Book%d\', %d, '\ISBN%d\');", i, i, i * 10, i);
		db.execute(std::string(buff));
	}

	db.execute(std::string("SELECT id AS BID, * FROM book AS B;")); // Check select
	db.execute(std::string("SELECT count(*), SUM(book.id) FROM book;"));
	//db.shutdown();										

	system("pause");

	return 0;
}

