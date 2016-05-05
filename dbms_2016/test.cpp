#include "test.h"

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

#include "SQLParser.h"
#include "SQLParserResult.h"
#include "sqlhelper.h"

using namespace std;
using namespace sql;

void test_dbms_parser_create_table()
{
	std::string query;
Parse:
	std::getline(std::cin, query);

	sql::SQLParserResult *parser = sql::SQLParser::parseSQLString(query);

	if (parser->isValid)
	{
		printf("valid\n");
		for (sql::SQLStatement *stmt : parser->statements)
		{
			sql::SelectStatement* se_st;
			sql::InsertStatement* in_st;
			sql::CreateStatement* cr_st;
			switch (stmt->type())
			{
			case sql::kStmtSelect:
				se_st = (sql::SelectStatement*)stmt;
				std::cout << se_st->hasAggregation() << std::endl;
				//cout << "se_st " << se_st->aggregation_list->front()->attribute << endl;
				break;
			case sql::kStmtInsert:
				in_st = (sql::InsertStatement*)stmt;
				break;
			case sql::kStmtCreate:
				cr_st = (sql::CreateStatement*)stmt;
				cout << "cr_st ,Test" << cr_st->columns->front()->length << endl;
				cout << "cr_st " << cr_st->tableName << endl;
				cout << "cr_st " << cr_st->columns->front()->length << endl;
				cout << "cr_st " << cr_st->columns->front()->name << endl;
				cout << "cr_st " << cr_st->columns->front()->type << endl;
				break;
			default:
				break;
			}
		}
	}
	else
	{
		printf("invalid\n");
	}
	goto Parse;
}

void test_datapage()
{
	unsigned char mem_pool[16 + 1];
	char str[16 + 1] = "ABCDEFGHIJKLMNOP";

	DataPage<PAGESIZE_8K> p(17);
	for (int j = 0; j < 16; j++)
	{
		for (int i = 0; i < 17 - 1; i++)
			mem_pool[i] = str[i];
		p.write_row(mem_pool);
	}
	p.dump_info();

	FILE *file = fopen("page", "wb");
	p.write_back(file, 0);
	fclose(file);
}

void test_freemapfile()
{
	BitmapPageFreeMapFile<PAGESIZE_8K> fmp;
	fmp.open("test.fmp", "wb+");
	fmp.set_page_full(1);
	fmp.get_free_page();
	fmp.dump_info();
	fmp.write_back();
}

void test_bitutil_get_val_uint32()
{
	unsigned int v = 0xfff0;
	unsigned int rv = get_val_uint32(v, 8, 15);
	printf("0x%x\n", rv);
}

void test_recordfile_write()
{
	unsigned char mem_pool[15 + 1];
	char str[15 + 1] = "ABCDEFGHIJKLMNO";
	unsigned char result;

	RecordFile<PAGESIZE_8K> rf(16);
	rf.open("test.dat", "wb+");
	for (int i = 0; i < 10; i++)
	{
		unsigned int addr = rf.put_record(0, str, &result, PAGEBUFFER_WRITE);
		if (result & BIT_SUCCESS)
			printf("Put ok:\nPage ID: %d\nPage Offset: %d\n", get_page_id(addr), get_page_offset(addr));
		else
			printf("Put error\n");
		str[0]++;
	}
	rf.write_back();
}

void test_recordfile_write_struct_record()
{
	structure_record record{ 1, "Williamd", "NTHU", 2 };

	unsigned char result;

	RecordFile<PAGESIZE_8K> rf(sizeof(structure_record));
	rf.open("test.dat", "wb+");
	for (int i = 0; i < 10; i++)
	{
		unsigned int addr = rf.put_record(0, &record, &result, PAGEBUFFER_WRITE);
		if (result & BIT_SUCCESS)
			printf("Put ok:\tPage ID: %d\tPage Offset: %d\n", get_page_id(addr), get_page_offset(addr));
		else
			printf("Put error\n");
		record.id++;
	}

	rf.write_back();
}

void test_recordfile_read()
{
	RecordFile<PAGESIZE_8K> rf(16);
	rf.open("test.dat", "rb");

	char mem_pool[15 + 1];
	unsigned int pid = 0;
	for (unsigned int poffset = 0; poffset < 10; poffset++)
	{
		unsigned int addr = get_page_addr(pid, poffset);
		if (rf.get_record(addr, mem_pool))
		{
			printf("%s\n", mem_pool);
		}
	}
}

void test_recordfile_read_struct_record()
{
	RecordFile<PAGESIZE_8K> rf(sizeof(structure_record));
	rf.open("test.dat", "rb");

	structure_record record;
	unsigned int pid = 0;
	for (unsigned int poffset = 0; poffset < 10; poffset++)
	{
		unsigned int addr = get_page_addr(pid, poffset);
		if (rf.get_record(addr, &record))
		{
			printf("%d\t%s\t%s\t%d\n", record.id, record.name, record.addr, record.gender);
		}
	}
}

void test_recordfile_find()
{
	RecordFile<PAGESIZE_8K> rf(16);
	rf.open("test.dat", "rb");

	char str[15 + 1] = "ABCDEFGHIJKLMNO";
	for (int i = 0; i < 11; i++)
	{
		bool result;
		unsigned int addr = rf.find_record(str, 1, &result);
		if (result)
		{
			printf("Record %s found at Page %d, Row %d\n", str, get_page_id(addr), get_page_offset(addr));
		}
		else
		{
			printf("Record %s not found\n", str);
		}
		str[0]++;
	}
}

void test_recordfile_find_col()
{
	RecordFile<PAGESIZE_8K> rf(sizeof(structure_record));
	rf.open("test.dat", "rb");

	structure_record record;
	for (int i = 1; i < 11; i++)
	{
		int id = rand() % 10;
		bool result;
		unsigned int addr = rf.find_record_with_col(&id, 1, offsetof(structure_record, id), sizeof(unsigned int), &result);
		if (result)
		{
			printf("Record %d found at Page %d, Row %d\n", id, get_page_id(addr), get_page_offset(addr));

			if (rf.get_record(addr, &record))
			{
				printf("%d\t%s\t%s\t%d\n", record.id, record.name, record.addr, record.gender);
			}
		}
		else
		{
			printf("Record %d not found\n", id);
		}
	}
}

void test_tablefile_init()
{
	table_attr_desc_t descs[3] = {
		{ "ID", ATTR_TYPE_INTEGER, 0, 4 },
		{ "Name", ATTR_TYPE_VARCHAR, 4, 40 },
		{ "Phone", ATTR_TYPE_VARCHAR, 44, 10 },
	};

	TableFile tb;
	tb.open("test.tbl", "wb+");
	tb.init("Student", 3, descs, -1);
	tb.write_back();
}

void test_tablefile_readfrom()
{
	TableFile tb;
	tb.open("test.tbl", "rb+");
	tb.read_from();
	tb.dump_info();
}

void test_recordtable_create()
{
	structure_record record{ 1, "Williamd", "NTHU", 2 };
	table_attr_desc_t descs[4] = {
		{ "ID", ATTR_TYPE_INTEGER, offsetof(structure_record, id), 4, ATTR_CONSTRAINT_PRIMARY_KEY },
		{ "Name", ATTR_TYPE_VARCHAR, offsetof(structure_record, name), 40, 0 },
		{ "Addr", ATTR_TYPE_VARCHAR, offsetof(structure_record, addr), 5, 0 },
		{ "Gender", ATTR_TYPE_INTEGER, offsetof(structure_record, gender), 4, 0 }
	};

	RecordTable<PAGESIZE_8K> rt;
	rt.create("table", 4, descs);

	for (int i = 0; i < 1000000; i++)
	{
		rt.insert(&record);
		record.id++;
	}
	rt.save_table();
	rt.save_freemap();
	rt.save_record();
}

void test_recordtable_create_duplicate()
{
	structure_record record{ 1, "Williamd", "NTHU", 2 };
	table_attr_desc_t descs[4] = {
		{ "ID", ATTR_TYPE_INTEGER, offsetof(structure_record, id), 4 },
		{ "Name", ATTR_TYPE_VARCHAR, offsetof(structure_record, name), 40 },
		{ "Addr", ATTR_TYPE_VARCHAR, offsetof(structure_record, addr), 5 },
		{ "Gender", ATTR_TYPE_INTEGER, offsetof(structure_record, gender), 4 }
	};

	RecordTable<PAGESIZE_8K> rt;
	rt.create("table", 4, descs, -1);

	for (int i = 0; i < 10; i++)
	{
		rt.insert(&record);
	}
	rt.save_table();
	rt.save_freemap();
	rt.save_record();
}

void test_recordtable_select()
{
	structure_record record{ 1, "Williamd", "NTHU", 2 };
	table_attr_desc_t descs[4] = {
		{ "ID", ATTR_TYPE_INTEGER, offsetof(structure_record, id), 4 },
		{ "Name", ATTR_TYPE_VARCHAR, offsetof(structure_record, name), 40 },
		{ "Addr", ATTR_TYPE_VARCHAR, offsetof(structure_record, addr), 5 },
		{ "Gender", ATTR_TYPE_INTEGER, offsetof(structure_record, gender), 4 }
	};

	const char *cols[] = { "ID", "Addr", "Gender" };

	RecordTable<PAGESIZE_8K> rt;
	rt.load("table");

	rt.select_show(cols, 3);
}

void test_recordtable_primarykey()
{
	structure_record record{ 1, "Williamd", "NTHU", 2 };
	table_attr_desc_t descs[4] = {
		{ "ID", ATTR_TYPE_INTEGER, offsetof(structure_record, id), 4 },
		{ "Name", ATTR_TYPE_VARCHAR, offsetof(structure_record, name), 40 },
		{ "Addr", ATTR_TYPE_VARCHAR, offsetof(structure_record, addr), 5 },
		{ "Gender", ATTR_TYPE_INTEGER, offsetof(structure_record, gender), 4 }
	};

	RecordTable<PAGESIZE_8K> rt;
	rt.create("table", 4, descs, 0);

	for (int i = 0; i < 10; i++)
	{
		rt.insert(&record);
		record.id++;
	}
	rt.save_table();
	rt.save_freemap();
	rt.save_record();
}

void test_dbms_table_create()
{
	table_attr_desc_t descs[4] = {
		{ "ID", ATTR_TYPE_INTEGER, offsetof(structure_record, id), 4 },
		{ "Name", ATTR_TYPE_VARCHAR, offsetof(structure_record, name), 40 },
		{ "Addr", ATTR_TYPE_VARCHAR, offsetof(structure_record, addr), 5 },
		{ "Gender", ATTR_TYPE_INTEGER, offsetof(structure_record, gender), 4 }
	};
	structure_record record{ 1, "Williamd", "NTHU", 2 };
	const char *cols[] = { "ID", "Addr", "Gender" };

	DatabaseFile<PAGESIZE_8K> dbf;
	dbf.open("test.dbs", "w");
	dbf.create_table("test_table1", descs, 4, -1);

	RecordTable<PAGESIZE_8K> *rt = dbf.get_table("test_table1");
	for (int i = 0; i < 10; i++)
	{
		rt->insert(&record);
		record.id++;
	}

	record.id = 1;
	dbf.create_table("test_table2", descs, 4, -1);

	RecordTable<PAGESIZE_8K> *rt2 = dbf.get_table("test_table2");
	for (int i = 0; i < 10; i++)
	{
		rt2->insert(&record);
		record.id++;
	}

	dbf.write_back();
}

void test_dbms_table_read()
{
	DatabaseFile<PAGESIZE_8K> dbf;
	dbf.open("test.dbs", "r+");
	dbf.read_from();

	RecordTable<PAGESIZE_8K> *rt = dbf.get_table("test_table1");
	const char *cols[] = { "ID", "Addr", "Gender", "Name" };

	rt->select_show(cols, 4);
}

void test_dbms_table_create_duplicate()
{
	table_attr_desc_t descs[4] = {
		{ "ID", ATTR_TYPE_INTEGER, offsetof(structure_record, id), 4 },
		{ "Name", ATTR_TYPE_VARCHAR, offsetof(structure_record, name), 40 },
		{ "Addr", ATTR_TYPE_VARCHAR, offsetof(structure_record, addr), 5 },
		{ "Gender", ATTR_TYPE_INTEGER, offsetof(structure_record, gender), 4 }
	};
	structure_record record{ 1, "Williamd", "NTHU", 2 };
	const char *cols[] = { "ID", "Addr", "Gender" };

	DatabaseFile<PAGESIZE_8K> dbf;
	dbf.open("test.dbs", "w");
	dbf.create_table("test_table1", descs, 4, -1);
	dbf.create_table("test_table1", descs, 4, -1);

	RecordTable<PAGESIZE_8K> *rt = dbf.get_table("test_table1");
	for (int i = 0; i < 10; i++)
	{
		rt->insert(&record);
		record.id++;
	}
	rt->select_show(cols, 3);

	dbf.write_back();
}

void test_dbms_table_iterator()
{
	DatabaseFile<PAGESIZE_8K> dbf;
	dbf.open("test.dbs", "r+");
	dbf.read_from();

	table_attr_desc_t descs[4] = {
		{ "ID", ATTR_TYPE_INTEGER, offsetof(structure_record, id), 4 },
		{ "Name", ATTR_TYPE_VARCHAR, offsetof(structure_record, name), 40 },
		{ "Addr", ATTR_TYPE_VARCHAR, offsetof(structure_record, addr), 5 },
		{ "Gender", ATTR_TYPE_INTEGER, offsetof(structure_record, gender), 4 }
	};

	table_attr_desc_t *pDescs[4] = {
		&descs[0],
		&descs[1],
		&descs[2],
		&descs[3]
	};

	RecordTable<PAGESIZE_8K> *rt1 = dbf.get_table("test_table1");
	RecordTable<PAGESIZE_8K>::fast_iterator it1(rt1);

	unsigned char *record1, *record2;
	unsigned int count = 0;
	while ((record1 = it1.next()) != NULL)
	{
		count += get_int_from_record(record1, descs[3].offset);

		rt1->print_record(pDescs, 4, record1);
	}
	printf("Count: %d\n", count);
}