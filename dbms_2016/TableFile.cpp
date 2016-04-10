#include "TableFile.h"
#include "FileUtil.h"

#include <cassert>

typedef std::pair<std::string, table_attr_desc_t*> attr_name_desc_pair;

TableFile::TableFile()
	: mHeader{"null", 0, 0},
	mAttrDescs(NULL)
{
}


TableFile::~TableFile()
{
	delete[] mAttrDescs;
}

inline const table_header_t &TableFile::get_table_header()
{
	return mHeader;
}

inline const table_attr_desc_t *TableFile::get_attr_desc(const char *attrName)
{
	AttrDictionary::iterator it = mAttrHashIndex.find(attrName);
	if (it != mAttrHashIndex.end())
	{
		return it->second;
	}
	return nullptr;
}

inline const table_attr_desc_t *TableFile::get_attr_desc(unsigned int index)
{	
	if (index >= 0 && index < mHeader.attrNum)
		return &mAttrDescs[index];
	return nullptr;
}

const unsigned int TableFile::get_row_size() const
{
	return mHeader.rowsize;
}

void TableFile::init(const char *name, unsigned int num, table_attr_desc_t *pDescs)
{
	assert(name != NULL && num > 0);

	strncpy(mHeader.name, name, TABLE_NAME_MAX);
	mHeader.attrNum = num;
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
	for (int i = 0; i < mHeader.attrNum; i++)
	{
		printf("[%s\t%d\t%d]\n",mAttrDescs[i].name,mAttrDescs[i].offset,mAttrDescs[i].size);
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
