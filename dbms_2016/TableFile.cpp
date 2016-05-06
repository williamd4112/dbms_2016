#include "TableFile.h"
#include "FileUtil.h"
#include "system.h"
#include "database_util.h"
#include "database_type.h"

#include <cassert>

typedef std::pair<std::string, AttrRecord> attr_name_desc_pair;


TableFile::TableFile()
	: mHeader{"null", 0, 0, -1},
	mAttrDescs(NULL), mIndexDescs(NULL)
{
}


TableFile::~TableFile()
{
	delete[] mAttrDescs;
	delete[] mIndexDescs;
}

const table_header_t &TableFile::get_table_header() const
{
	return mHeader;
}

table_attr_desc_t *TableFile::get_attr_desc(const char *attrName) const
{
	AttrDictionary::const_iterator it = mAttrLookupTable.find(attrName);
	if (it != mAttrLookupTable.end())
	{
		return it->second.first;
	}
	return nullptr;
}

table_attr_desc_t *TableFile::get_attr_desc(unsigned int index) const
{	
	if (index >= 0 && index < mHeader.attrNum)
		return &mAttrDescs[index];
	return nullptr;
}

const table_attr_desc_t *TableFile::get_attr_descs()
{
	return mAttrDescs;
}

const bool TableFile::get_attr_descs(const char **names, unsigned int num, table_attr_desc_t **pDescs)
{
	assert(num <= mHeader.attrNum && pDescs != NULL);

	// Since attribute number is so small (at most 5), use linear search more quickly
	for (int i = 0; i < num; i++)
	{
		table_attr_desc_t *p = get_attr_desc(names[i]);
		if (p)
		{
			pDescs[i] = p;
		}
		else 
		{
			// Get an error attribute
			return false;
		}
	}
}

IndexFile * TableFile::get_index(const char *attr_name, IndexType type)
{
	auto res = mAttrLookupTable.find(attr_name);
	if (res != mAttrLookupTable.end())
	{
		return mIndexDescs[res->second.second].indices[type % INDEX_NUM].index_file;
	}
	return NULL;
}

const unsigned int TableFile::get_row_size() const
{
	return mHeader.rowsize;
}

void TableFile::init(const char *name, unsigned int num, table_attr_desc_t *pDescs, int primaryKeyIndex)
{
	assert(name != NULL && num > 0);

	strncpy(mHeader.name, name, TABLE_NAME_MAX);
	mHeader.attrNum = num;
	mHeader.primaryKeyIndex = primaryKeyIndex;
	mHeader.rowsize = 0;
	
	mAttrDescs = new table_attr_desc_t[num];
	mIndexDescs = new table_index_desc_t[num];

	for (int i = 0; i < num; i++)
	{
		mAttrDescs[i] = pDescs[i];
		mHeader.rowsize += mAttrDescs[i].size;
	}
	build_lookup_table();
	build_primary_key_index(name);
}

void TableFile::init(const char *name, unsigned int num, table_attr_desc_t *pDescs)
{
	assert(name != NULL && num > 0);

	strncpy(mHeader.name, name, TABLE_NAME_MAX);
	
	mHeader.attrNum = num;
	mHeader.rowsize = 0;
	mHeader.primaryKeyIndex = -1; // No PK

	mAttrDescs = new table_attr_desc_t[num];
	mIndexDescs = new table_index_desc_t[num];

	for (int i = 0; i < num; i++)
	{
		mAttrDescs[i] = pDescs[i];

		if (mAttrDescs[i].constraint & ATTR_CONSTRAINT_PRIMARY_KEY)
		{
			if (mHeader.primaryKeyIndex != -1)
				throw TABLEFILE_EXCEPTION_TOO_MANY_PK;
			mHeader.primaryKeyIndex = i;
		}
	
		mHeader.rowsize += mAttrDescs[i].size;
	}
	build_lookup_table();
	build_primary_key_index(name);

}

void TableFile::init(const char *name, std::vector<sql::ColumnDefinition*>& col_defs)
{
	assert(name != NULL && col_defs.size() > 0);

	strncpy(mHeader.name, name, TABLE_NAME_MAX);

	mHeader.attrNum = col_defs.size();
	mHeader.rowsize = 0;
	mHeader.primaryKeyIndex = -1; // No PK

	mAttrDescs = new table_attr_desc_t[mHeader.attrNum];
	mIndexDescs = new table_index_desc_t[mHeader.attrNum];

	unsigned int offset = 0;
	for (int i = 0; i < col_defs.size(); i++)
	{
		strncpy(mAttrDescs[i].name, col_defs[i]->name, ATTR_NAME_MAX);
		mAttrDescs[i].offset = offset;
		mAttrDescs[i].size = col_defs[i]->length;
		mAttrDescs[i].type = (col_defs[i]->type == sql::ColumnDefinition::INT) ? ATTR_TYPE_INTEGER : ATTR_TYPE_VARCHAR;
		mAttrDescs[i].constraint = col_defs[i]->IsPK ? ATTR_CONSTRAINT_PRIMARY_KEY : 0x0;

		if (mAttrDescs[i].type == ATTR_TYPE_VARCHAR)
			mAttrDescs[i].size++;

		if (mAttrDescs[i].constraint & ATTR_CONSTRAINT_PRIMARY_KEY)
		{
			if (mHeader.primaryKeyIndex != -1)
				sub_error("Assign two primary key, use first one.");
			else
				mHeader.primaryKeyIndex = i;
		}

		mHeader.rowsize += mAttrDescs[i].size;
		offset += mAttrDescs[i].size;
	}

	build_lookup_table();
	
	// build primary index
	build_primary_key_index(name);
}

uint8_t TableFile::init_index(const char * attr_name, const char * index_filename, IndexType index_type)
{
	auto res = mAttrLookupTable.find(attr_name);
	if (res != mAttrLookupTable.end())
	{
		const table_attr_desc_t *desc = res->second.first;
		const int attr_id = res->second.second;
		const int index_id = index_type % INDEX_NUM;

		// Check duplicated
		if (mIndexDescs[attr_id].indices[index_id].index_file != NULL)
		{
			return TABLEFILE_ERROR_DUPLICATE_INDEX;
		}

		IndexFile *index_file = allocate_index_file(desc, index_type);
		mIndexDescs[attr_id].indices[index_id].index_name = index_filename;
		mIndexDescs[attr_id].indices[index_id].index_file = index_file;
		
		index_file->open(attr_name, "w+");
	}

	return TABLEFILE_NO_ERROR;
}

void TableFile::update_index(void * src, uint32_t addr)
{
	for (int i = 0; i < mHeader.attrNum; i++)
	{
		const table_attr_desc_t &attr_desc = mAttrDescs[i];
		for (int j = 0; j < INDEX_NUM; j++)
		{
			IndexFile *index_file = mIndexDescs[i].indices[j].index_file;
			if (index_file != NULL)
			{
				switch (attr_desc.type)
				{
				case ATTR_TYPE_INTEGER:
					index_file->set(db::parse_int((unsigned char * const)src, attr_desc), addr);
					break;
				case ATTR_TYPE_VARCHAR:
					index_file->set(db::parse_varchar((unsigned char * const)src, attr_desc), addr);
					break;
				default:
					throw table_exception_t("Undefined type is not accepted.");
				}
			}
		}
	}
}

inline void TableFile::write_back()
{
	FileUtil::write_back(mFile, 0, &mHeader, sizeof(table_header_t));
	FileUtil::write_back(mFile, sizeof(table_header_t), mAttrDescs, mHeader.attrNum * sizeof(table_attr_desc_t));

	// Write back index file
	// Format: <attr_id> <index_type_id> <index_file_name>
	char buff[INDEX_RECORD_SIZE_MAX];
	for (int i = 0; i < mHeader.attrNum; i++)
	{
		for (int j = 0; j < INDEX_NUM; j++)
		{
			auto index_record = mIndexDescs[i].indices[j];
			if (index_record.index_file != NULL)
			{
				sprintf(buff, "%d\t%d\t%s\n", i, index_record.index_file->type(), index_record.index_name.c_str());
				fwrite(buff, strlen(buff), 1, mFile);
				mIndexDescs[i].indices[j].index_file->write_back();
			}

		}
	}
}

inline void TableFile::read_from()
{
	FileUtil::read_at(mFile, 0, &mHeader, sizeof(table_header_t));
	
	assert(mAttrDescs == NULL);
	mAttrDescs = new table_attr_desc_t[mHeader.attrNum];
	mIndexDescs = new table_index_desc_t[mHeader.attrNum];

	FileUtil::read_at(mFile, sizeof(table_header_t), mAttrDescs, mHeader.attrNum * sizeof(table_attr_desc_t));

	build_lookup_table();

	char index_name[INDEX_FILENAME_MAX];
	int attr_id;
	IndexType index_type;

	while (fscanf(mFile, "%d%d%s", &attr_id, &index_type, index_name) == 3)
	{
		IndexFile *index_file = allocate_index_file(&mAttrDescs[attr_id], index_type);
		mIndexDescs[attr_id].indices[index_type % INDEX_NUM].index_name = index_name;
		mIndexDescs[attr_id].indices[index_type % INDEX_NUM].index_file = index_file;
		
		index_file->open(index_name, "r+");
		index_file->read_from();
	}
}

void TableFile::dump_info()
{
	printf("Table Name: %s\n",mHeader.name);
	printf("Table Attr num: %d\n",mHeader.attrNum);
	printf("Table Row Size: %d\n",mHeader.rowsize);
	printf("Name\tType\tOffset\tSize\tPrimaryKey\n");
	for (int i = 0; i < mHeader.attrNum; i++)
	{
		printf("%s\t%s\t%d\t%d\t%d\n",
			mAttrDescs[i].name, 
			kAttrTypeNames[mAttrDescs[i].type], 
			mAttrDescs[i].offset,
			mAttrDescs[i].size,
			(mAttrDescs[i].constraint & ATTR_CONSTRAINT_PRIMARY_KEY ? 1 : 0));
		for (int j = 0; j < INDEX_NUM; j++)
		{
			auto index_record = mIndexDescs[i].indices[j];
			if(index_record.index_file != NULL)
				printf("Index:\t%s\t%d\n", index_record.index_name.c_str(), index_record.index_file->type());
		}
	}
}

inline void TableFile::build_lookup_table()
{
	for (int i = 0; i < mHeader.attrNum; i++)
	{
		auto result = mAttrLookupTable.insert(std::pair<std::string, AttrRecord>(mAttrDescs[i].name, AttrRecord(&mAttrDescs[i], i)));

		// No duplicated attribute here, should check at top first, not here
		assert(result.second);
	}
}

inline void TableFile::build_primary_key_index(const char *name)
{
	// build primary index
	char buff[INDEX_FILENAME_MAX];
	sprintf(buff, "%s_pk.idx", name);
	init_index(mAttrDescs[mHeader.primaryKeyIndex].name, buff, PHASH);
}

inline IndexFile * TableFile::allocate_index_file(const table_attr_desc_t * desc, IndexType type)
{
	assert(desc->type != ATTR_TYPE_UNDEFINED);

	IndexFile *index_file = NULL;
	attr_domain_t domain;
	switch (desc->type)
	{
	case ATTR_TYPE_INTEGER:
		domain = INTEGER_DOMAIN;
		break;
	case ATTR_TYPE_VARCHAR:
		domain = VARCHAR_DOMAIN;
		break;
	default:
		fatal_error();
		break;
	}

	switch (type)
	{
	case HASH:
		index_file = new HashIndexFile(domain, desc->size);
		break;
	case TREE:
		index_file = new TreeIndexFile(domain, desc->size);
		break;
	case PHASH:
		index_file = new PrimaryIndexFile(domain, desc->size);
		break;
	default:
		fatal_error();
		break;
	}
	
	return index_file;
}

table_index_desc_t::table_index_desc_t()
{
	for (int i = 0; i < INDEX_NUM; i++)
		indices[i].index_file = NULL;
}

table_index_desc_t::~table_index_desc_t()
{
	for (int i = 0; i < INDEX_NUM; i++)
		if (indices[i].index_file != NULL)
			delete indices[i].index_file;
}
