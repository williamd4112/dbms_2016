#include "TableFile.h"
#include "FileUtil.h"

#include <cassert>

typedef std::pair<std::string, table_attr_desc_t*> attr_name_desc_pair;

const char *kAttrTypeNames[] = {"NULL", "INTEGER", "VARCHAR"};

TableFile::TableFile()
	: mHeader{"null", 0, 0, -1},
	mAttrDescs(NULL)
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

table_attr_desc_t *TableFile::get_attr_desc(const char *attrName)
{
	AttrDictionary::iterator it = mAttrHashIndex.find(attrName);
	if (it != mAttrHashIndex.end())
	{
		return it->second;
	}
	return nullptr;
}

table_attr_desc_t *TableFile::get_attr_desc(unsigned int index)
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
	for (int i = 0; i < num; i++)
	{
		mAttrDescs[i] = pDescs[i];
		mHeader.rowsize += mAttrDescs[i].size;
	}
	build_index();
}

inline void TableFile::write_back()
{
	FileUtil::write_back(mFile, 0, &mHeader, sizeof(table_header_t));
	FileUtil::write_back(mFile, sizeof(table_header_t), mAttrDescs, mHeader.attrNum * sizeof(table_attr_desc_t));
}

inline void TableFile::read_from()
{
	FileUtil::read_at(mFile, 0, &mHeader, sizeof(table_header_t));
	
	assert(mAttrDescs == NULL);
	mAttrDescs = new table_attr_desc_t[mHeader.attrNum];

	FileUtil::read_at(mFile, sizeof(table_header_t), mAttrDescs, mHeader.attrNum * sizeof(table_attr_desc_t));

	build_index();
}

void TableFile::dump_info()
{
	printf("Table Name: %s\n",mHeader.name);
	printf("Table Attr num: %d\n",mHeader.attrNum);
	printf("Table Row Size: %d\n",mHeader.rowsize);
	for (int i = 0; i < mHeader.attrNum; i++)
	{
		printf("[%s\t%s\t%d\t%d]\n",mAttrDescs[i].name, kAttrTypeNames[mAttrDescs[i].type], mAttrDescs[i].offset,mAttrDescs[i].size);
	}
}

inline void TableFile::build_index()
{
	for (int i = 0; i < mHeader.attrNum; i++)
	{
		auto result = mAttrHashIndex.insert(attr_name_desc_pair(std::string(mAttrDescs[i].name), &mAttrDescs[i]));

		// No duplicated attribute here, should check at top first, not here
		assert(result.second);
	}
}
