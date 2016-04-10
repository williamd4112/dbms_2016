#include <iostream>

#include "DataPage.h"
#include "BitmapPageFreeMapFile.h"
#include "RecordFile.h"
#include "TableFile.h"
#include "RecordTable.h"
#include "Bit.h"

struct structure_record {
	int id;
	char name[40];
	char addr[5];
	int gender;
};

void test_datapage();
void test_freemapfile();
void test_bitutil_get_val_uint32();
void test_recordfile_write();
void test_recordfile_write_struct_record();
void test_recordfile_read();
void test_recordfile_read_struct_record();
void test_recordfile_find();
void test_recordfile_find_col();
void test_tablefile_init();
void test_tablefile_readfrom();
void test_recordtable_create();
void test_recordtable_select();

int main(int argc, char *argv[])
{
	test_recordtable_create();
	system("pause");

	return 0;
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
	printf("0x%x\n",rv);
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
		unsigned int addr = rf.put_record(0, str, &result);
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
	structure_record record{1, "Williamd", "NTHU", 2};

	unsigned char result;

	RecordFile<PAGESIZE_8K> rf(sizeof(structure_record));
	rf.open("test.dat", "wb+");
	for (int i = 0; i < 10; i++)
	{
		unsigned int addr = rf.put_record(0, &record, &result);
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
			printf("%s\n",mem_pool);
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
			printf("%d\t%s\t%s\t%d\n",record.id, record.name, record.addr, record.gender);
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
			printf("Record %s found at Page %d, Row %d\n",str,get_page_id(addr),get_page_offset(addr));
		}
		else 
		{
			printf("Record %s not found\n",str);
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
		unsigned int addr = rf.find_record_with_int(id, offsetof(structure_record, id), 1, &result);
		if (result)
		{
			printf("Record %d found at Page %d, Row %d\n",id, get_page_id(addr), get_page_offset(addr));
			
			if (rf.get_record(addr, &record))
			{
				printf("%d\t%s\t%s\t%d\n", record.id, record.name, record.addr, record.gender);
			}
		}
		else
		{
			printf("Record %d not found\n",id);
		}
	}
}

void test_tablefile_init()
{
	table_attr_desc_t descs[3] = {
		{ "ID", 0, 4 },
		{ "Name", 4, 40 },
		{ "Phone", 44, 10 }
	};

	TableFile tb;
	tb.open("test.tbl", "wb+");
	tb.init("Student", 3, descs);
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
	structure_record record{1, "Williamd", "NTHU", 2 };
	table_attr_desc_t descs[4] = {
		{ "ID", 0, 4 },
		{ "Name", 4, 40 },
		{ "Phone", 44, 10 },
		{ "Gender", 54, 4}
	};

	RecordTable<PAGESIZE_8K> rt;
	rt.create("table", 4, descs);
	
	for (int i = 0; i < 10; i++)
	{
		rt.insert(&record);
		record.id++;
	}
}

void test_recordtable_select()
{
	structure_record record{ 1, "Williamd", "NTHU", 2 };
	table_attr_desc_t descs[4] = {
		{ "ID", 0, 4 },
		{ "Name", 4, 40 },
		{ "Phone", 44, 10 },
		{ "Gender", 54, 4 }
	};

	RecordTable<PAGESIZE_8K> rt;
	rt.create("table", 4, descs);

	for (int i = 0; i < 10; i++)
	{
		rt.insert(&record);
		record.id++;
	}
}


