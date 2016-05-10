#include "LightTableFile.h"

#include "system.h"

LightTableFile::LightTableFile(const char *tablename, AttrDesc *descs, int num)
{
	create(tablename, descs, num);
}

LightTableFile::LightTableFile()
{
}


LightTableFile::~LightTableFile()
{
	for (auto it = mIndexFileMap.begin(); it != mIndexFileMap.end(); it++)
		delete it->second;
}

inline void LightTableFile::create(const char *tablename, AttrDesc * descs, int num)
{
	strncpy(mTableHeader.name, tablename, TABLE_NAME_MAX);
	mTableHeader.attrNum = num;
	mTableHeader.rowsize = 0;
	mTableHeader.primaryKeyIndex = -1;

	mAttrDescPool.clear();
	mAttrDescPool.assign(descs, descs + num);

	for (int i = 0; i < mTableHeader.attrNum; i++)
	{
		mTableHeader.rowsize += mAttrDescPool[i].size;
		if (mAttrDescPool[i].constraint & ATTR_CONSTRAINT_PRIMARY_KEY)
		{
			if (mTableHeader.primaryKeyIndex != -1)
				throw exception_t(TOO_MANY_PK, "Too many primary key.");
			mTableHeader.primaryKeyIndex = i;

			create_index(mAttrDescPool[i], PHASH, get_index_file_name_str(mAttrDescPool[i].name).c_str());
		}
	}

	build_attr_desc_index();
}

inline IndexFile & LightTableFile::create_index(const AttrDesc & desc, IndexType type, const char * idx_path)
{
	IndexFile *idxFile = gen_indexfile(desc, type);
	auto res = mIndexFileMap.insert({ desc.name, idxFile });
	if (res.second)
	{
		idxFile->open(idx_path, "wb+");
	}
	else
	{
		delete idxFile;
		throw exception_t(DUPLICATE_INDEX_FILE, "Duplicated index file");
	}
	return *idxFile;
}

const AttrDesc &LightTableFile::get_attr_desc(const char * attr_name)
{
	auto res = mAttrDescTable.find(attr_name);
	if (res == mAttrDescTable.end())
		throw exception_t(ATTR_NOT_FOUND, attr_name);
	return mAttrDescPool.at(res->second);
}

const AttrDescPool & LightTableFile::get_attr_descs()
{
	return mAttrDescPool;
}

const int LightTableFile::get_attr_id(const char * attr_name)
{
	auto res = mAttrDescTable.find(attr_name);
	if (res != mAttrDescTable.end())
	{
		return res->second;
	}
	return -1;
}

IndexFile * LightTableFile::get_index_file(const char *attr_name)
{
	auto res = mIndexFileMap.find(attr_name);
	if (res != mIndexFileMap.end())
	{
		return res->second;
	}

	return nullptr;
}

const char * LightTableFile::get_pk_attr_name()
{
	assert(mTableHeader.primaryKeyIndex >= 0 && mTableHeader.primaryKeyIndex < mTableHeader.attrNum);
	return mAttrDescPool[mTableHeader.primaryKeyIndex].name;
}

inline void LightTableFile::write_back()
{
	fseek(mFile, 0, SEEK_SET);

	fwrite(&mTableHeader, sizeof(TableHeader), 1, mFile);
	
	if(!mAttrDescPool.empty())
		fwrite(&mAttrDescPool[0], sizeof(AttrDesc) * mAttrDescPool.size(), 1, mFile);

	for (IndexFileMap::iterator it = mIndexFileMap.begin(); it != mIndexFileMap.end(); it++)
	{
		table_index_record_t idx_record(it->first, it->second->get_filepath(), it->second->type());
		fwrite(&idx_record, sizeof(table_index_record_t), 1, mFile);

		it->second->write_back();
		delete it->second;
	}
}

inline void LightTableFile::read_from()
{
	fseek(mFile, 0, SEEK_SET);

	fread(&mTableHeader, sizeof(TableHeader), 1, mFile);
	
	mAttrDescPool.resize(mTableHeader.attrNum);

	if (!mAttrDescPool.empty())
		fread(&mAttrDescPool[0], sizeof(AttrDesc) * mAttrDescPool.size(), 1, mFile);
	
	build_attr_desc_index();
	
	table_index_record_t index_record;
	while (fread(&index_record, sizeof(table_index_record_t), 1, mFile))
	{
		IndexFile *idx_file = gen_indexfile(get_attr_desc(index_record.attr_name), index_record.index_type);
		
		auto res = mIndexFileMap.insert({ index_record.attr_name, idx_file });
		if (!res.second)
		{
			delete idx_file;
			throw exception_t(DUPLICATE_INDEX_FILE, "Duplicated index file found when read from disk.");
		}
		
		idx_file->open(index_record.index_name, "rb+");
		idx_file->read_from();
	}
}

void LightTableFile::dump_info()
{
	printf("Table Name: %s\n", mTableHeader.name);
	printf("Table Attr num: %d\n", mTableHeader.attrNum);
	printf("Name\tType\tSize\tPrimaryKey\tIndex\n");
	for (int i = 0; i < mTableHeader.attrNum; i++)
	{
		printf("%s\t%s\t%d\t%d\t",
			mAttrDescPool[i].name,
			kAttrTypeNames[mAttrDescPool[i].type],
			mAttrDescPool[i].size,
			(mAttrDescPool[i].constraint & ATTR_CONSTRAINT_PRIMARY_KEY ? 1 : 0));
		
		auto res = mIndexFileMap.find(mAttrDescPool[i].name);
		if (res != mIndexFileMap.end())
		{
			printf("%s(%d)", res->second->get_filepath().c_str(), res->second->type());
		}
		printf("\n");
	}
}

inline void LightTableFile::build_attr_desc_index()
{
	mAttrDescTable.clear();
	for (int i = 0; i < mAttrDescPool.size(); i++)
	{
		mAttrDescTable[mAttrDescPool[i].name] = i;
	}
}

std::string LightTableFile::get_index_file_name_str(const char * attr_name)
{
	return std::string(mTableHeader.name) + "_" + std::string(attr_name) + ".idx";
}

inline IndexFile * LightTableFile::gen_indexfile(const AttrDesc & desc, IndexType index_type)
{
	attr_domain_t domain = UNDEFINED_DOMAIN;
	switch (desc.type)
	{
	case ATTR_TYPE_INTEGER:
		domain = INTEGER_DOMAIN;
		break;
	case ATTR_TYPE_VARCHAR:
		domain = VARCHAR_DOMAIN;
		break;
	default:
		throw exception_t(TYPE_UNDEFINED, "Cannot set_table index, unknown type.");
	}

	switch (index_type)
	{
	case PHASH:
		return  new PrimaryIndexFile(domain, desc.size);
		break;
	case HASH:
		return new HashIndexFile(domain, desc.size);
		break;
	case TREE:
		return new TreeIndexFile(domain, desc.size);
		break;
	default:
		throw exception_t(UNSUPPORTED_INDEX_TYPE, "Unsupported index type");
	}

	return nullptr;
}
