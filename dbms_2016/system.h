#pragma once

#define Error printf
#define Msg if(!quiet) printf
#define Warning printf

extern bool quiet;

typedef void(*Routine)();

void fatal_error();
void sub_error(const char *);

void profile_pefromance(Routine routine);