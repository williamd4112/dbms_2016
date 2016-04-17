#pragma once

#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <vector>

class RecordBuffer
{
public:
	RecordBuffer(size_t buffsize, size_t rowsize);
	~RecordBuffer();

	inline void push(unsigned int addr);
	inline unsigned int pop();
private:
	std::vector<unsigned int> addrs;
};

