/**
数据字典，管理表和字段的信息
*/

#ifndef DICT_H_INCLUDED
#define DICT_H_INCLUDED

#include "util.h"
#include <stdio.h>

struct TableDictItem
{
    uint TID;
    char TName[MAX_T_NAME];
    char MetaPath[MAX_PATH_LEN];
    char DataPath[MAX_PATH_LEN];
    uint FieldNum;
    uint64 TupleNum;
    uint64 BlockNum;
    bool Valid;
};

struct FieldDictItem
{
    uint TID;
    char FName[MAX_F_NAME];
    bool IsPrimaryKey;
    bool IsUnique;
    bool BuildHashIndex;
    bool BuildBPTIndex;
    bool Valid;
    FieldType Type;
    uint Size;
};

struct FieldInfo
{
    char FName[MAX_F_NAME];
    bool IsPrimaryKey;
    bool IsUnique;
    bool BuildHashIndex;
    bool BuildBPTIndex;
    FieldType Type;
    uint Size;
    FieldInfo();
    FieldInfo(char fname[], bool isPrimKey, bool isUniq, bool buildHash, bool buildBpt, FieldType type, uint size);
    void SetInfo(char fname[], bool isPrimKey, bool isUniq, bool buildHash, bool buildBpt, FieldType type, uint size);
};

struct TableInfo
{
    char TName[MAX_T_NAME];
    char MetaPath[MAX_PATH_LEN];
    char DataPath[MAX_PATH_LEN];
    FieldInfo *Fields;
    uint FieldNum;
    uint64 TupleNum;
    uint64 BlockNum;
    TableInfo();
    TableInfo(char tname[], char dpath[], char mpath[], uint fnum, FieldInfo *fields);
    ~TableInfo();
};

class Dictionary
{
private:
    uint ShowTabIndex;
    uint TableNum, FieldNum;
    uint TabDicLen, FieDicLen;
    Addr TabDicAddr, FieDicAddr;
    bool M;
    TableDictItem *TableDict;
    FieldDictItem *FieldDict;
    char DBPath[MAX_PATH_LEN];
    uint MaxTID;
public:
    Dictionary(char *dbPath);
    ~Dictionary();
    void Open();
    void Close();
    void Flush();
    void Create();
    void InitShowTable();
    char *GetNextTabName();
    bool GetTableInfo(char *tableName, TableInfo* tableInfo);
    bool AddTable(TableInfo* tableInfo);
    bool DelTable(char *tableName);
    bool TableExist(char *tableName);
    bool UpdateStatis(char *tableName, uint64 tupleNum, uint64 blockNum);
    int UpdateIndex(char *tableName, char *fieldName, bool bpt, bool hash, char type);
};

#endif // DICT_H_INCLUDED
