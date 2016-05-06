#pragma once

#include "database_type.h"
#include "database_table_type.h"
#include "DiskFile.h"
#include "IndexFile.h"

#define TOO_MANY_PK 0x1
#define TYPE_UNDEFINED 0x2
#define UNSUPPORTED_INDEX_TYPE 0x3
#define DUPLICATE_INDEX_FILE 0x4
#define ATTR_NOT_FOUND 0x5

typedef table_attr_desc_t AttrDesc;
typedef table_header_t TableHeader;
typedef std::unordered_map<std::string, int> AttrDescTable;
typedef std::vector<AttrDesc> AttrDescPool;

/*
	LightTableFile

	as prefix, Light* series is designed for lightweight (memory intensive) components

	another Disk intensive implementation is TableFile
*/
class LightTableFile
	: public DiskFile
{
	friend class LightTable;

	typedef std::pair<std::string, IndexFile*> IndexRecord;
	typedef std::unordered_map<std::string, IndexFile*> IndexFileMap;
public:
	LightTableFile(const char *tablename, AttrDesc *descs, int num);
	LightTableFile();
	~LightTableFile();
	
	inline void create(const char *tablename, AttrDesc *descs, int num);
	inline void create_index(const AttrDesc &desc, IndexType type, const char *idx_path);

	const AttrDesc &get_attr_desc(const char *attr_name);
	const AttrDescPool &get_attr_descs();
	const int get_attr_id(const char *attr_name);
	const TableHeader &get_header() { return mTableHeader; }
	IndexFile *get_index_file(const char *attr_name);
	const char *get_pk_attr_name();

	inline void write_back();
	inline void read_from();

	void dump_info();
private:
	TableHeader mTableHeader;
	AttrDescTable mAttrDescTable;
	AttrDescPool mAttrDescPool;
	IndexFileMap mIndexFileMap;

	inline void build_attr_desc_index();
	std::string get_index_file_name_str(const char *attr_name);
	inline IndexFile *gen_indexfile(const AttrDesc &desc, IndexType index_type);
};

