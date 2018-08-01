/**
 * FILE     : log.h
 * DESC     : log output
 * AUTHOR   : v0.1 written by Alex Stocks
 * DATE     : on June 13, 2015
 * LICENCE  : GPL 2.0
 * MOD      :
 **/

#ifndef __LOG_H__
#define __LOG_H__

#define pline(fmt, ...)     printf(fmt"\n", ##__VA_ARGS__)
#define pinfo(fmt, ...)     \
    printf("\033[1;34;40m%s-%s-%d: "fmt"\033[0m\n", ((char*)__FILE__), \
            (char*)__func__, (int)__LINE__, ##__VA_ARGS__)
#define perr(fmt, ...)     \
    fprintf(stderr, "\033[1;31;40m%s-%s-%d: error: "fmt"\033[0m\n", \
            (char*)__FILE__, (char*)__func__, (int)__LINE__, ##__VA_ARGS__)

#define FATAL  perr
#define DEBUG  pinfo

#endif

