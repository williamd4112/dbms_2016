#include "LightTable.h"



LightTable::LightTable()
{
}


LightTable::~LightTable()
{
}

void LightTable::create(const char * tablename, AttrDesc * descs, int num)
{
	mTablename = tablename;

	std::string tbl_path = mTablename + ".tbl";
	std::string dat_path = mTablename + ".dat";

	mTablefile.open(tbl_path.c_str(), "wb+");
	mTablefile.create(tablename, descs, num);

	init_seq_types(descs, num);

	mDatafile.open(dat_path.c_str(), "w+");
	mDatafile.init(mSeqTypes.data(), num);
}

void LightTable::load(const char * tablename)
{
	mTablename = tablename;

	std::string tbl_path = mTablename + ".tbl";
	std::string dat_path = mTablename + ".dat";
	
	mTablefile.open(tbl_path.c_str(), "rb+");
	mDatafile.open(dat_path.c_str(), "r+");

	mTablefile.read_from();
	init_seq_types(mTablefile.mAttrDescPool.data(), mTablefile.mAttrDescPool.size());

	mDatafile.init(mSeqTypes.data(), mSeqTypes.size());
	mDatafile.read_from();
}

void LightTable::save()
{
	mTablefile.write_back();
	mDatafile.write_back();
}

void LightTable::insert(AttrTuple & tuple)
{

}

void LightTable::dump()
{
	mTablefile.dump_info();
}

AttrTupleIterator LightTable::begin()
{
	return mDatafile.begin();
}

AttrTupleIterator LightTable::end()
{
	return mDatafile.end();
}

inline void LightTable::init_seq_types(AttrDesc *descs, int num)
{
	mSeqTypes.clear();
	mSeqTypes.resize(num);
	for (int i = 0; i < num; i++)
	{
		switch (descs[i].type)
		{
		case ATTR_TYPE_INTEGER:
			mSeqTypes[i] = SEQ_INT;
			break;
		case ATTR_TYPE_VARCHAR:
			mSeqTypes[i] = SEQ_VARCHAR;
			break;
		default:
			throw exception_t(ATTR_TYPE_TO_SEQ_TYPE_ERROR, "Attr type to seq type error.");
		}
	}
}
