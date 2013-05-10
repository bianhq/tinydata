#include "pool.h"

DictPool g_DictPool;
TablePool g_TablePool;

Dictionary* DictPool::Get(char *dictName)
{
    string dname(dictName);
    map<string, Dictionary*>::iterator it = dictMap.find(dname);
    if (it != dictMap.end())
    {
        return it->second;
    }
    else
    {
        Dictionary *dict = new Dictionary(dictName);
        dict->Create();
        dictMap.insert(map<string, Dictionary*>::value_type(dname, dict));
        return dict;
    }
}

bool DictPool::Free(char *dictName)
{
    string dname(dictName);
    map<string, Dictionary*>::iterator it = dictMap.find(dname);
    if (it != dictMap.end())
    {
        it->second->Close();
        delete it->second;
        dictMap.erase(it);
        return true;
    }
    else
    {
        return false;
    }
}

void DictPool::Close()
{
    map<string, Dictionary*>::iterator it = dictMap.begin();
    for (; it != dictMap.end(); ++it)
    {
        it->second->Close();
        delete it->second;
    }
    dictMap.clear();
}

BufferedTable* TablePool::Get(TableInfo *info)
{
    string tname(info->TName);
    map<string, BufferedTable*>::iterator it = tableMap.find(tname);
    if (it != tableMap.end())
    {
        return it->second;
    }
    else
    {
        BufferedTable *table = new BufferedTable(info);
        table->Open(true);
        tableMap.insert(map<string, BufferedTable*>::value_type(tname, table));
        return table;
    }
}

bool TablePool::Free(TableInfo *info)
{
    string tname(info->TName);
    map<string, BufferedTable*>::iterator it = tableMap.find(tname);
    if (it != tableMap.end())
    {
        it->second->Close();
        delete it->second;
        tableMap.erase(it);
        return true;
    }
    else
    {
        return false;
    }
}

void TablePool::Close()
{
    map<string, BufferedTable*>::iterator it = tableMap.begin();
    for (; it != tableMap.end(); ++it)
    {
        it->second->Close();
        delete it->second;
    }
    tableMap.clear();
}
