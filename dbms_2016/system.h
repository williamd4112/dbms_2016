#pragma once

#include <stdint.h>
#include <ctime>
#include <iostream>

#define Error(fmt, ...) printf(fmt, __VA_ARGS__)
#define Msg if(!quiet) printf
#define Warning(fmt, ...) printf(fmt, __VA_ARGS__)

extern bool quiet;
extern bool pause_at_exit;
extern bool interactive;

typedef void(*Routine)();

void fatal_error();
void sub_error(const char *);

void profile(Routine routine);

struct exception_t
{
	uint32_t code;
	std::string msg;

	exception_t(uint32_t _code);
	exception_t(uint32_t _code, const char *_msg);
	~exception_t();
};