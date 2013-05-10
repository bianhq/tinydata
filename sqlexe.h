#ifndef SQLEXE_H_INCLUDED
#define SQLEXE_H_INCLUDED

#include "util.h"

#define MAX_SELECT_LIST_LEN 4
#define MAX_FROM_LIST_LEN 4
#define MAX_COND_LIST_LEN 4
#define MAX_ORDER_LIST_LEN 4
#define MAX_FIELD_LIST_LEN 4
#define MAX_VALUE_LIST_LEN 4
#define MAX_STRING_LEN 50

typedef struct
{
    char Name[MAX_T_NAME];
    unsigned char IsPrimaryKey;
    unsigned char IsUnique;
    enum FieldType Type;
    uint Size;
} Field;

typedef struct
{
    unsigned char HasTableName;
    char TableName[MAX_T_NAME];
    char FieldName[MAX_F_NAME];
} SelectListItem, TableFieldName;

typedef struct
{
    char TableName[MAX_T_NAME];
} FromListItem;

typedef union
{
    TableFieldName TFOpd;
    char StrOpd[MAX_STRING_LEN];
    int NumOpd;
} Operand;

typedef struct
{
    Operand Left;
    unsigned char LeftType;//0表示TableFieldName, 1表示char[]，2表示int
    enum Comparison Op;
    Operand Right;
    unsigned char RightType;//0表示TableFieldName, 1表示char[]，2表示int
} CondListItem;

typedef struct
{
    unsigned char HasTableName;
    char TableName[MAX_T_NAME];
    char FieldName[MAX_F_NAME];
    unsigned char IsASC;
} OrderListItem;

typedef struct
{
    unsigned char IsUnique;//等于0表示select的结果不需要去重，为1反之
    uint SelectListLen;//等于0表示select * from...
    uint FromListLen;
    uint CondListLen;//等于0表示没有where子句
    uint OrderListLen;//等于0表示没有order by子句
    SelectListItem *SelectList;
    FromListItem *FromList;
    CondListItem *CondList;
    OrderListItem *OrderList;
} SelectSyntaxTree;

typedef struct
{
    char TableName[MAX_T_NAME];
    uint CondListLen;
    CondListItem *CondList;
} DeleteSyntaxTree;

typedef struct
{
    char FieldName[MAX_F_NAME];
} FieldListItem;

typedef union
{
    char String[MAX_STRING_LEN];
    int Integer;
} ValueListItem;

typedef struct
{
    char TableName[MAX_T_NAME];
    uint FieldListLen;//等于0表示没有field list
    uint ValueListLen;
    FieldListItem *FieldList;
    ValueListItem *ValueList;
    enum FieldType *ValueTypeList;
} InsertSyntaxTree;

void ShowTables();
void CreateTable(char *tableName, uint fieldNum, Field *fields);
void DropTable(char *tableName);
void CreateIndex(char type, char *tableName, char *fieldName);
void DropIndex(char type, char *tableName, char *fieldName);
void SelectFromTable(SelectSyntaxTree tree);
void InsertIntoTable(InsertSyntaxTree tree);
void DeleteFromTable(DeleteSyntaxTree tree);
void ShowHelp();
void LoadData(char *fileName, char *tableName);

#endif // SQLEXE_H_INCLUDED
