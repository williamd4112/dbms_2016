#pragma once

#include <cstdio>
#include <cstdlib>
#include <iostream>

#define get_val_uint32(val, low, high) ((val) >> (low)) & ((1 << ((high) - (low) + 1)) - 1)

void printm(void *src, unsigned int size);

template <unsigned int BIT_NUM,
	unsigned int PARTITION_NUM = BIT_NUM / 8 + 1>
class Bitmap
{
public:
	Bitmap() : bits{ 0 } {}
	~Bitmap() {}

	inline void Set(int i)
	{
		unsigned int part = i >> 3;
		bits[part] = (bits[part]) | (1 << (i % 8));
	}

	inline bool Test(int i)
	{
		return (bits[i >> 3]) & (1 << (i % 8));
	}

	inline void Reset()
	{
		memset(bits, 0, sizeof(unsigned char) * PARTITION_NUM);
	}

	inline void Dump()
	{
		for (int i = 0; i < PARTITION_NUM; i++)
		{
			for (int j = 0; j < 8 && j < BIT_NUM; j++)
			{
				std::cout << ((bits[i] & (1 << j)) ? 1 : 0);
			}
			std::cout << std::endl;
		}
		std::cout << std::endl;
	}

	inline void dump_info()
	{
		for (int i = 0; i < BIT_NUM; i++)
		{
			std::cout << "[" << i << "] = " << Test(i) << std::endl;
		}
	}
private:
	unsigned char bits[PARTITION_NUM];
};
