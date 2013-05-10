#include "dict.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

FieldInfo::FieldInfo():
IsPrimaryKey(false),
IsUnique(false),
BuildHashIndex(false),
BuildBPTIndex(false) {}

FieldInfo::FieldInfo(char fname[], bool isPrimKey, bool isUniq, bool buildHash, bool buildBpt, FieldType type, uint size)
{
    strcpy(FName, fname);
    IsPrimaryKey = isPrimKey;
    IsUnique = isUniq | isPrimKey;
    BuildHashIndex = buildHash;
    BuildBPTIndex = buildBpt;
    Type = type;
    Size = size;
}

void FieldInfo::SetInfo(char fname[], bool isPrimKey, bool isUniq, bool buildHash, bool buildBpt, FieldType type, uint size)
{
    strcpy(FName, fname);
    IsPrimaryKey = isPrimKey;
    IsUnique = isUniq | isPrimKey;
    BuildHashIndex = buildHash;
    BuildBPTIndex = buildBpt;
    Type = type;
    Size = size;
}

TableInfo::TableInfo():Fields(NULL), FieldNum(0u), TupleNum(0u), BlockNum(0u) {}
TableInfo::~TableInfo() {delete Fields;}
TableInfo::TableInfo(char tname[], char dpath[], char mpath[], uint fnum, FieldInfo *fields):
    TupleNum(0u), BlockNum(0u)
{
    strcpy(TName, tname);
    strcpy(DataPath, dpath);
    strcpy(MetaPath, mpath);
    FieldNum = fnum;
    Fields = fields;
}

Dictionary::Dictionary(char *dbPath)
{
    TableNum = FieldNum = 0;
    TabDicLen = FieDicLen = 0;
    ShowTabIndex = 0;
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
    TabDicLen = TableNum;
    FieDicLen = FieldNum;
    fclose(fp);
}

void Dictionary::Close()
{
    Flush();
    TableNum = FieldNum = 0;
    TabDicLen = FieDicLen = 0;
}

void Dictionary::Flush()
{
    if (M == true)
    {
        FILE *fp = fopen(DBPath, "wb");
        fwrite(&TableNum, sizeof(uint), 1, fp);////////////////////////////暂不检测各种值的有效性，有一定的安全隐患
        fwrite(&TabDicAddr, sizeof(Addr), 1, fp);
        fwrite(&FieDicAddr, sizeof(Addr), 1, fp);
        fseek(fp, TabDicAddr, SEEK_SET);//////形成一个文件空洞，好像不会对读有影响
        uint i;
        for (i = 0; i < TabDicLen; ++i)
        {
            if (TableDict[i].Valid)
            {
                fwrite(&(TableDict[i]), sizeof(TableDictItem), 1, fp);
            }
        }
        fseek(fp, FieDicAddr, SEEK_SET);//////形成一个文件空洞，好像不会对读有影响
        for (i = 0; i < FieDicLen; ++i)
        {
            if (FieldDict[i].Valid)
            {
                fwrite(&(FieldDict[i]), sizeof(FieldDictItem), 1, fp);
            }
        }
        fclose(fp);
    }
}

void Dictionary::Create()
{
    TableNum = FieldNum = 0;
    TabDicLen = FieDicLen = 0;
    TabDicAddr = 1024, FieDicAddr = 10240 + 1024;
    M = false;
    TableDict = NULL;
    FieldDict = NULL;
    MaxTID = 0;
    FILE *fp = fopen(DBPath, "wb");
    fwrite(&TableNum, sizeof(uint), 1, fp);////////////////////////////暂不检测各种值的有效性，有一定的安全隐患
    fwrite(&TabDicAddr, sizeof(Addr), 1, fp);
    fwrite(&FieDicAddr, sizeof(Addr), 1, fp);
    fseek(fp, TabDicAddr, SEEK_SET);
    fseek(fp, FieDicAddr, SEEK_SET);
    fclose(fp);
}

bool Dictionary::GetTableInfo(char *tableName, TableInfo* tableInfo)
{
    uint i, k;
    for (i = 0; i < TabDicLen; ++i)
    {
        if (TableDict[i].Valid == true && strcmp(TableDict[i].TName, tableName) == 0)
        {
            break;
        }
    }
    if (i >= TabDicLen)
        return false;
    uint tid = TableDict[i].TID;
    tableInfo->FieldNum = TableDict[i].FieldNum;
    tableInfo->TupleNum = TableDict[i].TupleNum;
    tableInfo->BlockNum = TableDict[i].BlockNum;
    strcpy(tableInfo->TName, TableDict[i].TName);
    strcpy(tableInfo->DataPath, TableDict[i].DataPath);
    strcpy(tableInfo->MetaPath, TableDict[i].MetaPath);
    tableInfo->Fields = new FieldInfo[TableDict[i].FieldNum];
    for (i = 0, k = 0; i < FieDicLen; ++i)
    {
        if (tid == FieldDict[i].TID)
        {
            strcpy(tableInfo->Fields[k].FName, FieldDict[i].FName);
            tableInfo->Fields[k].IsPrimaryKey = FieldDict[i].IsPrimaryKey;
            tableInfo->Fields[k].IsUnique = FieldDict[i].IsUnique;
            tableInfo->Fields[k].BuildHashIndex = FieldDict[i].BuildHashIndex;
            tableInfo->Fields[k].BuildBPTIndex = FieldDict[i].BuildBPTIndex;
            tableInfo->Fields[k].Size = FieldDict[i].Size;
            tableInfo->Fields[k].Type = FieldDict[i].Type;
            ++k;
        }
    }
    return true;
}

bool Dictionary::TableExist(char *tableName)
{
    uint i;
    for (i = 0; i < TabDicLen; ++i)
    {
        if (TableDict[i].Valid == true && strcmp(TableDict[i].TName, tableName) == 0)
        {
            break;
        }
    }
    if (i >= TabDicLen)
        return false;
    return true;
}

bool Dictionary::AddTable(TableInfo* tableInfo)
{
    if (tableInfo == NULL || tableInfo->Fields == NULL)
        return false;
    if (TableExist(tableInfo->TName))
    {
        return false;
    }
    uint tid = ++MaxTID;
    TableNum++;
    TabDicLen++;
    TableDict = (TableDictItem *)realloc(TableDict, sizeof(TableDictItem)*TabDicLen);
    TableDict[TabDicLen-1].TID = tid;
    strcpy(TableDict[TabDicLen-1].DataPath, tableInfo->DataPath);
    strcpy(TableDict[TabDicLen-1].MetaPath, tableInfo->MetaPath);
    strcpy(TableDict[TabDicLen-1].TName, tableInfo->TName);
    TableDict[TabDicLen-1].FieldNum = tableInfo->FieldNum;
    TableDict[TabDicLen-1].TupleNum = tableInfo->TupleNum;
    TableDict[TabDicLen-1].BlockNum = tableInfo->BlockNum;
    TableDict[TabDicLen-1].Valid = true;
    uint i = FieDicLen, j;
    FieldNum += tableInfo->FieldNum;
    FieDicLen += tableInfo->FieldNum;
    FieldDict = (FieldDictItem *)realloc(FieldDict, sizeof(FieldDictItem)*FieDicLen);
    for (j = 0; i < FieDicLen; ++i, ++j)
    {
        strcpy(FieldDict[i].FName, tableInfo->Fields[j].FName);
        FieldDict[i].IsPrimaryKey = tableInfo->Fields[j].IsPrimaryKey;
        FieldDict[i].IsUnique = tableInfo->Fields[j].IsUnique;
        FieldDict[i].BuildHashIndex = tableInfo->Fields[j].BuildHashIndex;
        FieldDict[i].BuildBPTIndex = tableInfo->Fields[j].BuildBPTIndex;
        FieldDict[i].Size = tableInfo->Fields[j].Size;
        FieldDict[i].Type = tableInfo->Fields[j].Type;
        FieldDict[i].TID = tid;
        FieldDict[i].Valid = true;
    }
    M = true;
    return true;
}

bool Dictionary::DelTable(char *tableName)
{
    uint i;
    for (i = 0; i < TabDicLen; ++i)
    {
        if (TableDict[i].Valid == true && strcmp(TableDict[i].TName, tableName) == 0)
        {
            break;
        }
    }
    if (i >= TabDicLen)
        return false;
    uint tid = TableDict[i].TID;
    TableDict[i].Valid = false;
    TableNum --;
    FieldNum -= TableDict[i].FieldNum;
    for (i = 0; i < FieDicLen; ++i)
    {
        if (tid == FieldDict[i].TID)
        {
            FieldDict[i].Valid = false;

        }
    }
    M = true;
    return true;
}

bool Dictionary::UpdateStatis(char *tableName, uint64 tupleNum, uint64 blockNum)
{
    uint i;
    for (i = 0; i < TabDicLen; ++i)
    {
        if (TableDict[i].Valid == true && strcmp(TableDict[i].TName, tableName) == 0)
        {
            break;
        }
    }
    if (i >= TabDicLen)
        return false;
    TableDict[i].TupleNum = tupleNum;
    TableDict[i].BlockNum = blockNum;
    this->M = true;
    return true;
}

void Dictionary::InitShowTable()
{
    this->ShowTabIndex = 0;
}

char *Dictionary::GetNextTabName()
{
    while (ShowTabIndex < this->TabDicLen &&
           this->TableDict[ShowTabIndex].Valid == false)
    {
        ++ShowTabIndex;
    }
    if (ShowTabIndex < this->TabDicLen)
    {
        ShowTabIndex++;
        return this->TableDict[ShowTabIndex-1].TName;
    }
    return NULL;
}

int Dictionary::UpdateIndex(char *tableName, char *fieldName, bool bpt, bool hash, char type)
{
    uint i, tid;
    for (i = 0; i < TabDicLen; ++i)
    {
        if (TableDict[i].Valid == true && strcmp(TableDict[i].TName, tableName) == 0)
        {
            break;
        }
    }
    if (i >= TabDicLen)
    {
        return 1;
    }

    tid = TableDict[i].TID;
    int res = 2;
    for (i = 0; i < FieDicLen; ++i)
    {
        if (tid == FieldDict[i].TID && strcmp(FieldDict[i].FName, fieldName) == 0)
        {
            if ((FieldDict[i].BuildBPTIndex == bpt && type == 'b') ||
                (FieldDict[i].BuildHashIndex == hash && type == 'h') ||
                (FieldDict[i].BuildBPTIndex == bpt &&
                 FieldDict[i].BuildHashIndex == hash && type == 'a'))
            {
                res = 3;
            }
            else
            {
                if (type == 'a')
                {
                    FieldDict[i].BuildBPTIndex = bpt;
                    FieldDict[i].BuildHashIndex = hash;
                }
                else if (type == 'b')
                {
                    FieldDict[i].BuildBPTIndex = bpt;
                }
                else if (type == 'h')
                {
                    FieldDict[i].BuildHashIndex = hash;
                }

                this->M = true;
                res = 0;
            }
            break;
        }
    }
    return res;
}
