#pragma once

#include <vector>
#include <sstream>
#include "DiskFile.h"
#include "database_type.h"
#include "database_util.h"

#define SEQFILE_UNEXPECTED_TYPE 0x1
#define SEQFILE_BAD_POS 0x2
#define SEQFILE_STRING_MAX_LEN 64

enum SequenceElementType
{
	SEQ_INT, SEQ_VARCHAR
};

/*
	SequenceFile

	No paging mechnisim file, fast sequential read/write

	In memory, just a vector
*/
template <typename E>
class SequenceFile
	: public DiskFile
{
	typedef std::vector<E> Tuple;
	typedef std::vector<Tuple> TupleVector;
	typedef std::vector<SequenceElementType> TypeVector;
	typedef typename std::vector<std::vector<E>>::iterator TupleIterator;
public:
	SequenceFile(const SequenceElementType *types, int num);
	SequenceFile();
	~SequenceFile();

	inline std::vector<E> &get(uint32_t index);
	inline uint32_t put(Tuple &tuple);
	inline void init(const SequenceElementType *types, int num);

	TupleIterator begin() { return mTuples.begin(); }
	TupleIterator end() { return mTuples.end(); }
	uint32_t size() { return mTuples.size(); }

	inline void write_back();
	inline void read_from();

private:
	TupleVector mTuples;
	TypeVector mTypes;

	inline bool read_tuple();
};

template<class E>
inline SequenceFile<E>::SequenceFile(const SequenceElementType *types, int num)
	: mTypes(types , types + num)
{

}

template<typename E>
inline SequenceFile<E>::SequenceFile()
{
}

template<class E>
inline SequenceFile<E>::~SequenceFile()
{
}

template<class E>
inline std::vector<E> & SequenceFile<E>::get(uint32_t index)
{
	assert(!mTypes.empty());
	if (index < 0 || index >= mTuples.size())
		throw SEQFILE_BAD_POS;
	return mTuples[index];
}

template<class E>
inline uint32_t SequenceFile<E>::put(Tuple & tuple)
{
	mTuples.push_back(tuple);
	return mTuples.size() - 1;
}

template<typename E>
inline void SequenceFile<E>::init(const SequenceElementType * types, int num)
{
	mTuples.clear();
	mTypes.clear();
	mTypes.assign(types, types + num);
}

template<class E>
inline void SequenceFile<E>::write_back()
{	
	assert(!mTypes.empty());

	fseek(mFile, 0, SEEK_SET);
	const int tuple_size = mTypes.size();
	for (TupleVector::iterator it = mTuples.begin(); it != mTuples.end(); it++)
	{
		std::stringstream ss;
		const Tuple tuple = *it;

		for (int i = 0; i < tuple_size; i++)
		{
			ss << tuple[i] << ",";
		}

		fprintf(mFile, "%s\n", ss.str().c_str());
	}
}

template<class E>
inline void SequenceFile<E>::read_from()
{
	fseek(mFile, 0, SEEK_SET);
	while (read_tuple());
}

template<class E>
inline bool SequenceFile<E>::read_tuple()
{
	assert(!mTypes.empty());

	int ival_buff = 0;
	std::string sval_buff;
	static char line_buff[8192];

	if (fgets(line_buff, 8192, mFile) == NULL)
		return false;

	std::istringstream line(line_buff);

	Tuple tuple;
	tuple.resize(mTypes.size());

	for (int i = 0; i < mTypes.size(); i++)
	{
		switch (mTypes[i])
		{
		case SEQ_INT:
			if (!(line >> ival_buff))
				return false;
			tuple[i] = ival_buff;
			line.ignore();
			break;
		case SEQ_VARCHAR:
			if (!std::getline(line, sval_buff, ','))
				return false;
			tuple[i] = sval_buff.c_str();
			break;
		default:
			throw exception_t(SEQFILE_UNEXPECTED_TYPE, "Unexpected type");
		}
	}

	mTuples.push_back(tuple);

	return true;
}
