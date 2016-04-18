#pragma once

class BinaryObject
{
public:
	enum ObjectType
	{
		OBJECT_INTEGER,
		OBJECT_VARCHAR,
		OBJECT_NULL
	};

	BinaryObject(int);
	BinaryObject(const char *);
	BinaryObject();
	~BinaryObject();

private:
	union Value
	{
		int integer;
		char *varchar;
	};

	Value value;
};

