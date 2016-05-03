#include "TableFile.h"
#include "FileUtil.h"
#include "system.h"

#include <cassert>

typedef std::pair<std::string, table_attr_desc_t*> attr_name_desc_pair;

const char *kAttrTypeNames[] = {"NULL", "INTEGER", "VARCHAR"};

TableFile::TableFile()
	: mHeader{"null", 0, 0, -1},
	mAttrDescs(NULL), mIndexDescs(NULL)
{
}


TableFile::~TableFile()
{
	delete[] mAttrDescs;
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
		return it->second;
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
}

inline void TableFile::write_back()
{
	FileUtil::write_back(mFile, 0, &mHeader, sizeof(table_header_t));
	FileUtil::write_back(mFile, sizeof(table_header_t), mAttrDescs, mHeader.attrNum * sizeof(table_attr_desc_t));

	// Load index desc
	char buff[ATTR_NAME_MAX + 1];
	for (int i = 0; i < mHeader.attrNum; i++)
	{
		for (int j = 0; j < 2; j++)
		{
			if (mIndexDescs[i].indexes[j].second != NULL)
			{
				const std::string &idx_filename = mIndexDescs[i].indexes[j].first;
				sprintf(buff, "%d\t%d\t%s\n", i, mIndexDescs[i].indexes[j].second->, idx_filename.c_str());
				fwrite(buff, strlen(buff), 1, mFile);
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

	// Load index desc
	int attr_id;
	int type_id;
	char index_name[ATTR_NAME_MAX + 2];

	while (fscanf(mFile, "%d%d%s", &attr_id, &type_id, index_name) == 3)
	{
		attr_domain_t domain = (mAttrDescs[attr_id].type == ATTR_TYPE_INTEGER) ? INTEGER_DOMAIN : VARCHAR_DOMAIN;
		size_t keysize = mAttrDescs[attr_id].size;
		IndexFile *idx_file = NULL;
		switch (type_id)
		{
		case HASH:
			idx_file = new HashIndexFile(domain, keysize);
			break;
		case TREE:
			idx_file = new TreeIndexFile(domain, keysize);
			break;
		case PHASH:
			idx_file = new PrimaryIndexFile(domain, keysize);
			break;
		case PTREE:
			assert("Not yet supported");
			break;
		default:
			Error("TableFile: index type not found.\n");
			break;
		}
		idx_file->open(index_name, "r+");
		idx_file->read_from();

		mIndexDescs[attr_id].indexes[type_id % 2] = std::pair<std::string, IndexFile*>(index_name, idx_file);
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
	}
}

inline void TableFile::build_lookup_table()
{
	for (int i = 0; i < mHeader.attrNum; i++)
	{
		auto result = mAttrLookupTable.insert(attr_name_desc_pair(std::string(mAttrDescs[i].name), &mAttrDescs[i]));

		// No duplicated attribute here, should check at top first, not here
		assert(result.second);
	}
}
