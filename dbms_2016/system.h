#pragma once

#define Error printf
#define Msg if(!quiet) printf

extern bool quiet;

void fatal_error();
void sub_error(const char *);