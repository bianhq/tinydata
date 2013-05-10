#include "spj.h"
#include <iostream>
#include <string.h>
using namespace std;

void OutputFields(TableInfo *info, char *fields, FILE *out)
{
    int pos = 0;
    char *tmpfield = new char[GetRecordSize(info)];
    for (int i = 0; i < info->FieldNum; ++i)
    {
        if (info->Fields[i].Type == INT)
        {
            fprintf(out, "%d\t", *((int *)(fields+pos)));
        }
        else
        {
            strncpy(tmpfield, fields+pos, info->Fields[i].Size);
            tmpfield[info->Fields[i].Size] = '\0';
            fprintf(out, "%s\t", tmpfield);
        }
        pos += info->Fields[i].Size;
    }
    fprintf(out, "\n");
    delete []tmpfield;
}

void OutputSomeFields(TableInfo *info, TableFieldName* tfList, int tfListLen, char *fields, FILE *out)
{
    int pos = 0;
    char *tmpfield = new char[GetRecordSize(info)];
    int j;
    for (int i = 0; i < info->FieldNum; ++i)
    {
        for (j = 0; j < tfListLen; ++j)
        {
            if (strcmp(tfList[j].FieldName, info->Fields[i].FName) == 0)
            {
                break;
            }
        }
        if (j < tfListLen)
        {
            if (info->Fields[i].Type == INT)
            {
                fprintf(out, "%d\t", *((int *)(fields+pos)));
            }
            else
            {
                strncpy(tmpfield, fields+pos, info->Fields[i].Size);
                tmpfield[info->Fields[i].Size] = '\0';
                fprintf(out, "%s\t", tmpfield);
            }
        }
        pos += info->Fields[i].Size;
    }
    fprintf(out, "\n");
    delete []tmpfield;
}

TableFieldName *Joiner::Init(FromListItem *fromList, uint fromListLen, CondListItem *cond)
{
    if (fromListLen != 2)
    {
        cout << "Sorry: too many tables to join";
        return NULL;
    }
    Dictionary *dict = g_DictPool.Get(DICT_FILE_PATH);
    dict->GetTableInfo(fromList[0].TableName, &leftInfo);
    this->leftTable = g_TablePool.Get(&leftInfo);
    dict->GetTableInfo(fromList[1].TableName, &rightInfo);
    this->rightTable = g_TablePool.Get(&rightInfo);
    this->condition = cond;
    TableFieldName * tfList = NULL;
    if (cond == NULL)
    {
        this->leftTable->TabScan_Init();
        this->rightTable->TabScan_Init();
        leftTable->TabScan_Next(leftFields);
    }
    else
    {
        if (cond->Op != EQ)
        {
            cout << "Sorry: do not support not equal join";
            return NULL;
        }
        //没有检查是否有b+树索引
        this->leftTable->IndexScan_Init(0, 10000000);//这里其实不应该用索引扫描的
        this->rightTable->IndexScan_Init(0, 10000000);
    }
    tfList = new TableFieldName[leftInfo.FieldNum+rightInfo.FieldNum];
    for (int i = 0; i < leftInfo.FieldNum; ++i)
    {
        strcpy(tfList[i].TableName, leftInfo.TName);
        tfList[i].HasTableName = 1;
        strcpy(tfList[i].FieldName, leftInfo.Fields[i].FName);
    }
    for (int i = 0, j = leftInfo.FieldNum; i < leftInfo.FieldNum; ++i, ++j)
    {
        strcpy(tfList[j].TableName, leftInfo.TName);
        tfList[j].HasTableName = 1;
        strcpy(tfList[j].FieldName, leftInfo.Fields[i].FName);
    }
    return tfList;
}

ValueListItem *Joiner::GetNext(ValueListItem *valueList)
{
    if (this->condition == NULL)
    {
        //迪卡尔积
        if (rightTable->TabScan_Next(rightFields) < 0)
        {
            if (leftTable->TabScan_Next(leftFields) < 0)
            {
                return NULL;
            }
            else
            {
                rightTable->TabScan_Init();
                rightTable->TabScan_Next(rightFields);
            }
        }
    }
    else
    {
        //等值链接
        if (leftTable->IndexScan_Next(leftFields) < 0) return NULL;
        if (rightTable->IndexScan_Next(rightFields) < 0) return NULL;
        while (*((int *)leftFields) != *((int *)rightFields))
        {
            while (*((int *)leftFields) < *((int *)rightFields))
            {
                if (leftTable->IndexScan_Next(leftFields) < 0) return NULL;
            }
            while (*((int *)leftFields) > *((int *)rightFields))
            {
                if (rightTable->IndexScan_Next(rightFields) < 0) return NULL;
            }
        }
    }
    int pos = 0;
    for (int i = 0; i < leftInfo.FieldNum; ++i)
    {
        if (leftInfo.Fields[i].Type == INT)
        {
            valueList[i].Integer = *((int *)(leftFields+pos));
        }
        else
        {
            strncpy(valueList[i].String, leftFields+pos, leftInfo.Fields[i].Size);
            valueList[i].String[leftInfo.Fields[i].Size] = '\0';
        }
        pos += leftInfo.Fields[i].Size;
    }
    pos = 0;
    for (int i = 0, j = leftInfo.FieldNum; i < rightInfo.FieldNum; ++i, ++j)
    {
        if (rightInfo.Fields[i].Type == INT)
        {
            valueList[j].Integer = *((int *)(rightFields+pos));
        }
        else
        {
            strncpy(valueList[j].String, rightFields+pos, rightInfo.Fields[i].Size);
            valueList[j].String[rightInfo.Fields[i].Size] = '\0';
        }
        pos += rightInfo.Fields[i].Size;
    }
    return valueList;
}

void Output2Fields(TableInfo *leftInfo, TableInfo *rightInfo, ValueListItem *valueList, FILE *out, bool skip)
{
    for (int i = 0; i < leftInfo->FieldNum; ++i)
    {
        if (leftInfo->Fields[i].Type == INT)
        {
            fprintf(out, "%d\t", valueList[i].Integer);
        }
        else
        {
            fprintf(out, "%s\t", valueList[i].String);
        }
    }
    for (int i = skip?1:0, j = leftInfo->FieldNum; i < leftInfo->FieldNum; ++i, ++j)
    {
        if (rightInfo->Fields[i].Type == INT)
        {
            fprintf(out, "%d\t", valueList[j].Integer);
        }
        else
        {
            fprintf(out, "%s\t", valueList[j].String);
        }
    }
    fprintf(out, "\n");
}

void OutputSome2Fields(TableInfo *leftInfo, TableInfo *rightInfo, TableFieldName* tfList, int tfListLen, ValueListItem *valueList, FILE *out)
{
    int j;
    for (int i = 0; i < leftInfo->FieldNum; ++i)
    {
        for (j = 0; j < tfListLen; ++j)
        {
            if (strcmp(tfList[j].TableName, leftInfo->TName) == 0 &&
                strcmp(tfList[j].FieldName, leftInfo->Fields[i].FName) == 0)
            {
                break;
            }
        }
        if (j < tfListLen)
        {
            if (leftInfo->Fields[i].Type == INT)
            {
                fprintf(out, "%d\t", valueList[i].Integer);
            }
            else
            {
                fprintf(out, "%s\t", valueList[i].String);
            }
        }

    }
    for (int i = 0, k = leftInfo->FieldNum; i < leftInfo->FieldNum; ++i, ++k)
    {
        for (j = 0; j < tfListLen; ++j)
        {
            if (strcmp(tfList[j].TableName, rightInfo->TName) == 0 &&
                strcmp(tfList[j].FieldName, rightInfo->Fields[i].FName) == 0)
            {
                break;
            }
        }
        if (j < tfListLen)
        {
            if (rightInfo->Fields[i].Type == INT)
            {
                fprintf(out, "%d\t", valueList[k].Integer);
            }
            else
            {
                fprintf(out, "%s\t", valueList[k].String);
            }
        }

    }
    fprintf(out, "\n");
}
