/**
数据字典，管理表和字段的信息
*/

#ifndef DICT_H_INCLUDED
#define DICT_H_INCLUDED

#include "util.h"
#include <stdio.h>

#define MAX_T_NAME 64
#define MAX_F_NAME 64

enum FieldType
{
    INT,
    STR,
    CHAR,
    FLOAT
};

struct TableDictItem
{
    uint TID;
    char TName[MAX_T_NAME];
    char MetaPath[MAX_PATH_LEN];
    char DataPath[MAX_PATH_LEN];
    uint FieldNum;
};

struct FieldDictItem
{
    uint TID;
    char FName[MAX_F_NAME];
    bool IsPrimaryKey;
    bool IsUnique;
    bool BuildHashIndex;
    bool BuildBPTIndex;
    FieldType Type;
    uint Length;
};

struct FieldInfo
{
    char FName[MAX_F_NAME];
    bool IsPrimaryKey;
    bool IsUnique;
    bool BuildHashIndex;
    bool BuildBPTIndex;
    FieldType Type;
    uint Length;
    FieldInfo();
    FieldInfo(char fname[], bool isPrimKey, bool isUniq, bool buildHash, bool buildBpt, FieldType type, uint len);
    void SetInfo(char fname[], bool isPrimKey, bool isUniq, bool buildHash, bool buildBpt, FieldType type, uint len);
};

struct TableInfo
{
    char TName[MAX_T_NAME];
    char MetaPath[MAX_PATH_LEN];
    char DataPath[MAX_PATH_LEN];
    FieldInfo *Fields;
    uint FieldNum;
    TableInfo();
    TableInfo(char tname[], char dpath[], char mpath[], uint fnum, FieldInfo *fields);
    ~TableInfo();
};

class Dictionary
{
private:
    uint TableNum, FieldNum;
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
    void Create();
    void GetTableInfo(char *tableName, TableInfo* tableInfo);
    bool AddTable(TableInfo* tableInfo);
    bool DelTable(char *tableName);
};

#endif // DICT_H_INCLUDED
