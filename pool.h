#ifndef POOL_H_INCLUDED
#define POOL_H_INCLUDED

#include "dict.h"
#include "table.h"
#include <string>
#include <map>
using namespace std;

class DictPool
{
private:
    map<string, Dictionary*> dictMap;
public:
    Dictionary* Get(char *dictName);
    bool Free(char *dictName);
    void Close();
};

class TablePool
{
private:
    map<string, BufferedTable*> tableMap;
public:
    BufferedTable* Get(TableInfo *info);
    bool Free(TableInfo *info);
    void Close();
};

extern DictPool g_DictPool;
extern TablePool g_TablePool;

#endif // POOL_H_INCLUDED
