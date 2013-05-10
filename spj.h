#ifndef SPJ_H_INCLUDED
#define SPJ_H_INCLUDED

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "sqlexe.h"
#include "pool.h"

class Joiner
{
private:
    char leftFields[150], rightFields[150];
    BufferedTable *leftTable;
    BufferedTable *rightTable;
    CondListItem *condition;
public:
    TableInfo leftInfo;
    TableInfo rightInfo;
    TableFieldName *Init(FromListItem *fromList, uint fromListLen, CondListItem *cond);
    ValueListItem *GetNext(ValueListItem *valueList);
};

class Selector
{
private:
    TableFieldName *tfList;
    CondListItem *condList;
public:
    void Init(TableFieldName *tfList, CondListItem *condList);
    bool Select(ValueListItem *valueList);
};

class Projector
{
private:
    TableFieldName *tfListDis;
    TableFieldName *tfListRes;
public:
    void Init(SelectListItem *selectList, TableFieldName *tfList);
    ValueListItem *Project(ValueListItem *valueList);
};

void OutputFields(TableInfo *info, char *fields, FILE *out);
void OutputSomeFields(TableInfo *info, TableFieldName* tfList, int tfListLen, char *fields, FILE *out);
void Output2Fields(TableInfo *leftInfo, TableInfo *rightInfo, ValueListItem *valueList, FILE *out, bool skip=false);
void OutputSome2Fields(TableInfo *leftInfo, TableInfo *rightInfo, TableFieldName* tfList, int tfListLen, ValueListItem *valueList, FILE *out);

#endif // SPJ_H_INCLUDED
