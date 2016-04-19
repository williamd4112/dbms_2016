#pragma once

#define Error(fmt, ...) printf(fmt, __VA_ARGS__)
#define Msg if(!quiet) printf
#define Warning printf

extern bool quiet;
extern bool pause_at_exit;
extern bool interactive;

typedef void(*Routine)();

void fatal_error();
void sub_error(const char *);

void profile_pefromance(Routine routine);