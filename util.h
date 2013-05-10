#ifndef UTIL_H_INCLUDED
#define UTIL_H_INCLUDED

/**
操作系统编译开关
当前操作系统是Unix\Linux为0，是windows为1
*/
#define OPERATING_SYS WIN
#define WIN 1
#define LINUX 0

#define MAX_T_NAME 64
#define MAX_F_NAME 64

#define MAX_UI32 0xffffffff
#define ADDR_WOL12 0xfffffffffffff000
#define INTERPAGE_ADDR 0xfff

#define DICT_FILE_PATH "tiny.data"

#define MAX_PATH_LEN 256

#define MAX_FIELDS 16

enum FieldType
{
    INT,
    CHAR,
    STR,
    FLOAT
};

enum Comparison
{
    EQ,
    GE,
    LE,
    GT,
    LT,
    NE
};

typedef int Key;
typedef long long Addr;
typedef short PageAddr;
typedef unsigned int HValue;
typedef unsigned long long uint64;
typedef unsigned int uint;
typedef unsigned short ushort;
typedef unsigned char byte;

#endif // UTIL_H_INCLUDED
