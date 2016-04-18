#pragma once


#pragma pack(push)
#pragma pack(1)
struct structure_record {
	int id;
	char name[40];
	char addr[5];
	int gender;
};
#pragma pack(pop)

void test_dbms_parser_create_table();

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
void test_recordtable_create_duplicate();
void test_recordtable_select();
void test_recordtable_primarykey();

//void test_recordtable_table_alias();
//void test_recordtable_column_alias();
//void test_recordtable_select_where();
//void test_recordtable_count();
//void test_recordtable_sum();
//void test_recordtable_innerjoin();

void test_dbms_table_create();
void test_dbms_table_read();
void test_dbms_table_create_duplicate();
void test_dbms_table_iterator();