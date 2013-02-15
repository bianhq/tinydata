#include "dict.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

FieldInfo::FieldInfo():
IsPrimaryKey(false),
IsUnique(false),
BuildHashIndex(false),
BuildBPTIndex(false) {}
FieldInfo::FieldInfo(char fname[], bool isPrimKey, bool isUniq, bool buildHash, bool buildBpt, FieldType type, uint len)
{
    strcpy(FName, fname);
    IsPrimaryKey = isPrimKey;
    IsUnique = isUniq;
    BuildHashIndex = buildHash;
    BuildBPTIndex = buildBpt;
    Type = type;
    Length = len;
}
void FieldInfo::SetInfo(char fname[], bool isPrimKey, bool isUniq, bool buildHash, bool buildBpt, FieldType type, uint len)
{
    strcpy(FName, fname);
    IsPrimaryKey = isPrimKey;
    IsUnique = isUniq;
    BuildHashIndex = buildHash;
    BuildBPTIndex = buildBpt;
    Type = type;
    Length = len;
}

TableInfo::TableInfo():Fields(NULL), FieldNum(0u) {}
TableInfo::~TableInfo() {delete Fields;}
TableInfo::TableInfo(char tname[], char dpath[], char mpath[], uint fnum, FieldInfo *fields)
{
    strcpy(TName, tname);
    strcpy(DataPath, dpath);
    strcpy(MetaPath, mpath);
    FieldNum = fnum;
    Fields = fields;
}

Dictionary::Dictionary(char *dbPath)
{
    TableNum = 0, FieldNum = 0;
    TabDicAddr = 1024, FieDicAddr = 10240 + 1024;
    M = false;
    TableDict = NULL;
    FieldDict = NULL;
    strcpy(DBPath, dbPath);
    MaxTID = 0;
}

Dictionary::~Dictionary()
{
    if (TableDict != NULL)
    {
        delete []TableDict;
    }
    if (FieldDict != NULL)
    {
        delete []FieldDict;
    }
}

void Dictionary::Open()
{
    FILE *fp = fopen(DBPath, "rb");
    fread(&TableNum, sizeof(uint), 1, fp);////////////////////////////暂不检测各种值的有效性，有一定的安全隐患
    fread(&TabDicAddr, sizeof(Addr), 1, fp);
    fread(&FieDicAddr, sizeof(Addr), 1, fp);
    if (TableNum == 0)
    {
        return;
    }
    TableDict = new TableDictItem[TableNum];
    fseek(fp, TabDicAddr, SEEK_SET);
    uint i;
    FieldNum = 0;
    for (i = 0; i < TableNum; ++i)
    {
        fread(&(TableDict[i]), sizeof(TableDictItem), 1, fp);
        FieldNum += TableDict[i].FieldNum;
        if (TableDict[i].TID > MaxTID)
        {
            MaxTID = TableDict[i].TID;
        }
    }
    FieldDict = new FieldDictItem[FieldNum];
    fseek(fp, FieDicAddr, SEEK_SET);
    for (i = 0; i < FieldNum; ++i)
    {
        fread(&(FieldDict[i]), sizeof(FieldDictItem), 1, fp);
    }
    fclose(fp);
}

void Dictionary::Close()
{
    if (M == true)
    {
        FILE *fp = fopen(DBPath, "wb");
        fwrite(&TableNum, sizeof(uint), 1, fp);////////////////////////////暂不检测各种值的有效性，有一定的安全隐患
        fwrite(&TabDicAddr, sizeof(Addr), 1, fp);
        fwrite(&FieDicAddr, sizeof(Addr), 1, fp);
        fseek(fp, TabDicAddr, SEEK_SET);//////形成一个文件空洞，好像不会对读有影响
        uint i;
        for (i = 0; i < TableNum; ++i)
        {
            fwrite(&(TableDict[i]), sizeof(TableDictItem), 1, fp);
        }
        fseek(fp, FieDicAddr, SEEK_SET);//////形成一个文件空洞，好像不会对读有影响
        for (i = 0; i < FieldNum; ++i)
        {
            fwrite(&(FieldDict[i]), sizeof(FieldDictItem), 1, fp);
        }
        fclose(fp);
    }
}

void Dictionary::Create()
{
    TableNum = 0;
    TabDicAddr = 1024, FieDicAddr = 10240 + 1024;
    FILE *fp = fopen(DBPath, "wb");
    fwrite(&TableNum, sizeof(uint), 1, fp);////////////////////////////暂不检测各种值的有效性，有一定的安全隐患
    fwrite(&TabDicAddr, sizeof(Addr), 1, fp);
    fwrite(&FieDicAddr, sizeof(Addr), 1, fp);
    fseek(fp, TabDicAddr, SEEK_SET);
    fseek(fp, FieDicAddr, SEEK_SET);
    fclose(fp);
}

void Dictionary::GetTableInfo(char *tableName, TableInfo* tableInfo)
{
    uint i, k;
    for (i = 0; i < TableNum; ++i)
    {
        if (strcmp(TableDict[i].TName, tableName) == 0)
        {
            break;
        }
    }
    if (i >= TableNum)
        return;
    uint tid = TableDict[i].TID;
    tableInfo->FieldNum = TableDict[i].FieldNum;
    strcpy(tableInfo->TName, TableDict[i].TName);
    strcpy(tableInfo->DataPath, TableDict[i].DataPath);
    strcpy(tableInfo->MetaPath, TableDict[i].MetaPath);
    tableInfo->Fields = new FieldInfo[TableDict[i].FieldNum];
    for (i = 0, k = 0; i < FieldNum; ++i)
    {
        if (tid == FieldDict[i].TID)
        {
            strcpy(tableInfo->Fields[k].FName, FieldDict[i].FName);
            tableInfo->Fields[k].IsPrimaryKey = FieldDict[i].IsPrimaryKey;
            tableInfo->Fields[k].IsUnique = FieldDict[i].IsUnique;
            tableInfo->Fields[k].BuildHashIndex = FieldDict[i].BuildHashIndex;
            tableInfo->Fields[k].BuildBPTIndex = FieldDict[i].BuildBPTIndex;
            tableInfo->Fields[k].Length = FieldDict[i].Length;
            tableInfo->Fields[k].Type = FieldDict[i].Type;
            ++k;
        }
    }
}

bool Dictionary::AddTable(TableInfo* tableInfo)
{
    if (tableInfo == NULL || tableInfo->Fields == NULL)
        return false;
    uint tid = ++MaxTID;
    TableNum++;
    TableDict = (TableDictItem *)realloc(TableDict, sizeof(TableDictItem)*TableNum);
    TableDict[TableNum-1].TID = tid;
    strcpy(TableDict[TableNum-1].DataPath, tableInfo->DataPath);
    strcpy(TableDict[TableNum-1].MetaPath, tableInfo->MetaPath);
    strcpy(TableDict[TableNum-1].TName, tableInfo->TName);
    TableDict[TableNum-1].FieldNum = tableInfo->FieldNum;
    uint i = FieldNum, j;
    FieldNum += tableInfo->FieldNum;
    FieldDict = (FieldDictItem *)realloc(FieldDict, sizeof(FieldDictItem)*FieldNum);
    for (j = 0; i < FieldNum; ++i, ++j)
    {
        strcpy(FieldDict[i].FName, tableInfo->Fields[j].FName);
        FieldDict[i].IsPrimaryKey = tableInfo->Fields[j].IsPrimaryKey;
        FieldDict[i].IsUnique = tableInfo->Fields[j].IsUnique;
        FieldDict[i].BuildHashIndex = tableInfo->Fields[j].BuildHashIndex;
        FieldDict[i].BuildBPTIndex = tableInfo->Fields[j].BuildBPTIndex;
        FieldDict[i].Length = tableInfo->Fields[j].Length;
        FieldDict[i].Type = tableInfo->Fields[j].Type;
        FieldDict[i].TID = tid;
    }
    M = true;
    return true;
}
bool Dictionary::DelTable(char *tableName)
{
    /////////////////////
    return false;
}
