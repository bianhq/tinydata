#include "sqlexe.h"
#include "pool.h"
#include "spj.h"
#include <iostream>
#include <iomanip>
#include <string.h>
#include <stdlib.h>
using namespace std;

void SelectFromTable(SelectSyntaxTree tree)
{
    Selector select;
    Projector project;
    Joiner join;
    Dictionary *dict = g_DictPool.Get(DICT_FILE_PATH);
    TableInfo *info = NULL;
    BufferedTable *table = NULL;
    if (tree.SelectListLen == 0)
    {
        //select *
        if (tree.FromListLen == 1)
        {
            //single table
            info = new TableInfo();
            dict->GetTableInfo(tree.FromList[0].TableName, info);
            table = g_TablePool.Get(info);
            if (tree.CondListLen == 0)
            {
                //no condition
                table->TabScan_Init();
                int num = 0;
                char *fields = new char[GetRecordSize(info)];
                FILE *out = fopen("/home/hadoop/Desktop/exp4-5/select", "w+");
                while(table->TabScan_Next(fields) > 0)
                {
                    OutputFields(info, fields, out);
                    ++num;
                }
                fclose(out);
                cout << "Query OK, " << num << " rows in set";
                delete []fields;

            }
            else
            {
                //>=1 conditions，这一块没有完全实现
                if (tree.CondListLen > 1)
                {
                    cout << "Sorry, too many conditions!";
                    return;
                }
                int i = 0;
                for (; i < info->FieldNum; ++i)
                {
                    if (info->Fields[i].Type == INT &&
                        strcmp(info->Fields[i].FName, tree.CondList[0].Left.TFOpd.FieldName) == 0)
                    {
                        break;
                    }
                }
                if (i == info->FieldNum)
                {
                    cout << "ERROR: the field '" << tree.CondList[0].Left.TFOpd.FieldName << "' does not exist";
                    return;
                }
                switch (tree.CondList[0].Op)
                {
                case EQ:
                    if (info->Fields[i].BuildBPTIndex)
                    {
                        ResultSet rs;
                        table->BPT_Seek(tree.CondList[0].Right.NumOpd, &rs);
                        for (int j = 0; j < rs.RecNum; ++j)
                        {
                            OutputFields(info, rs[j], stdout);
                        }
                        cout << "Query OK, " << rs.RecNum << " rows in set";
                    }
                    else if (info->Fields[i].BuildHashIndex)
                    {

                    }
                    else
                    {
                        table->TabScan_Init();
                        int num = 0;
                        char *fields = new char[GetRecordSize(info)];
                        while(table->TabScan_Next(fields) > 0)
                        {
                            if (*((int *)(fields)) == tree.CondList[0].Right.NumOpd)
                            {
                                OutputFields(info, fields, stdout);
                                ++num;
                            }
                        }
                        cout << "Query OK, " << num << " rows in set";
                        delete []fields;
                    }
                    break;
                case GT:
                    tree.CondList[0].Right.NumOpd++;
                case GE:
                    break;
                case LT:
                    tree.CondList[0].Right.NumOpd--;
                case LE:
                    break;
                case NE:
                    break;
                }
            }
            delete info;
        }
        else if (tree.FromListLen > 1)
        {
            //multi table
            if (tree.CondListLen == 0)
            {
                //no condition
                TableFieldName *tfList = join.Init(tree.FromList, tree.FromListLen, NULL);
                ValueListItem *valueList = new ValueListItem[join.leftInfo.FieldNum+join.rightInfo.FieldNum];
                FILE *out = fopen("/home/hadoop/Desktop/exp4-5/select", "w+");
                int num = 0;
                while (join.GetNext(valueList) != NULL)
                {
                    Output2Fields(&join.leftInfo, &join.rightInfo, valueList, out);
                    ++num;
                }
                cout << "Query OK, " << num << " rows in set";
                fclose(out);
                delete []valueList;
                delete []tfList;
            }
            else
            {
                //>=1 conditions
                TableFieldName *tfList = join.Init(tree.FromList, tree.FromListLen, tree.CondList);
                ValueListItem *valueList = new ValueListItem[join.leftInfo.FieldNum+join.rightInfo.FieldNum];
                FILE *out = fopen("/home/hadoop/Desktop/exp4-5/select", "w+");
                int num = 0;
                while (join.GetNext(valueList) != NULL)
                {
                    Output2Fields(&join.leftInfo, &join.rightInfo, valueList, out);
                    ++num;
                }
                cout << "Query OK, " << num << " rows in set";
                fclose(out);
                delete []valueList;
                delete []tfList;
            }
        }
        else
        {

        }
    }
    else
    {
        //select ...
        if (tree.FromListLen == 1)
        {
            //single table
            info = new TableInfo();
            dict->GetTableInfo(tree.FromList[0].TableName, info);
            table = g_TablePool.Get(info);
            if (tree.CondListLen == 0)
            {
                //no condition
                table->TabScan_Init();
                int num = 0;
                char *fields = new char[GetRecordSize(info)];
                FILE *out = fopen("/home/hadoop/Desktop/exp4-5/select", "w+");
                while(table->TabScan_Next(fields) > 0)
                {
                    OutputSomeFields(info, tree.SelectList, tree.SelectListLen, fields, out);
                    ++num;
                }
                fclose(out);
                cout << "Query OK, " << num << " rows in set";
                delete []fields;

            }
            else
            {
                //>=1 conditions，这一块没有完全实现
                if (tree.CondListLen > 1)
                {
                    cout << "Sorry, too many conditions!";
                    return;
                }
                int i = 0;
                for (; i < info->FieldNum; ++i)
                {
                    if (info->Fields[i].Type == INT &&
                        strcmp(info->Fields[i].FName, tree.CondList[0].Left.TFOpd.FieldName) == 0)
                    {
                        break;
                    }
                }
                if (i == info->FieldNum)
                {
                    cout << "ERROR: the field '" << tree.CondList[0].Left.TFOpd.FieldName << "' does not exist";
                    return;
                }
                switch (tree.CondList[0].Op)
                {
                case EQ:
                    if (info->Fields[i].BuildBPTIndex)
                    {
                        ResultSet rs;
                        table->BPT_Seek(tree.CondList[0].Right.NumOpd, &rs);
                        for (int j = 0; j < rs.RecNum; ++j)
                        {
                            OutputSomeFields(info, tree.SelectList, tree.SelectListLen, rs[j], stdout);
                        }
                        cout << "Query OK, " << rs.RecNum << " rows in set";
                    }
                    else if (info->Fields[i].BuildHashIndex)
                    {

                    }
                    else
                    {
                        table->TabScan_Init();
                        int num = 0;
                        char *fields = new char[GetRecordSize(info)];
                        while(table->TabScan_Next(fields) > 0)
                        {
                            if (*((int *)(fields)) == tree.CondList[0].Right.NumOpd)
                            {
                                OutputSomeFields(info, tree.SelectList, tree.SelectListLen, fields, stdout);
                                ++num;
                            }
                        }
                        cout << "Query OK, " << num << " rows in set";
                        delete []fields;
                    }
                    break;
                case GT:
                    tree.CondList[0].Right.NumOpd++;
                case GE:
                    break;
                case LT:
                    tree.CondList[0].Right.NumOpd--;
                case LE:
                    break;
                case NE:
                    break;
                }
            }
            delete info;
        }
        else if (tree.FromListLen > 1)
        {
            //multi table
            if (tree.CondListLen == 0)
            {
                //no condition
                TableFieldName *tfList = join.Init(tree.FromList, tree.FromListLen, NULL);
                ValueListItem *valueList = new ValueListItem[join.leftInfo.FieldNum+join.rightInfo.FieldNum];
                FILE *out = fopen("/home/hadoop/Desktop/exp4-5/select", "w+");
                int num = 0;
                while (join.GetNext(valueList) != NULL)
                {
                    OutputSome2Fields(&join.leftInfo, &join.rightInfo, tree.SelectList, tree.SelectListLen, valueList, out);
                    ++num;
                }
                cout << "Query OK, " << num << " rows in set";
                fclose(out);
                delete []valueList;
                delete []tfList;
            }
            else
            {
                //>=1 conditions
                TableFieldName *tfList = join.Init(tree.FromList, tree.FromListLen, tree.CondList);
                ValueListItem *valueList = new ValueListItem[join.leftInfo.FieldNum+join.rightInfo.FieldNum];
                FILE *out = fopen("/home/hadoop/Desktop/exp4-5/select", "w+");
                int num = 0;
                while (join.GetNext(valueList) != NULL)
                {
                    OutputSome2Fields(&join.leftInfo, &join.rightInfo, tree.SelectList, tree.SelectListLen, valueList, out);
                    ++num;
                }
                cout << "Query OK, " << num << " rows in set";
                fclose(out);
                delete []valueList;
                delete []tfList;
            }
        }
        else
        {

        }
    }
}

void ShowTables()
{
    cout <<
    "+--------------------------+" << endl <<
    "| Tables in Tiny Data      |" << endl <<
    "+--------------------------+" << endl;
    Dictionary *dict = g_DictPool.Get(DICT_FILE_PATH);
    dict->InitShowTable();
    char *tableName;
    uint i = 0;
    while ((tableName = dict->GetNextTabName()) != NULL)
    {
        cout << "| " << tableName << setw(26-strlen(tableName)) << "|" << endl;
        i++;
    }
    cout <<
    "+--------------------------+" << endl <<
    i << " rows in set";
}

void CreateTable(char *tableName, uint fieldNum, Field *fields)
{
    if (fieldNum > 2 && strcmp(fields[1].Name, fields[2].Name)==0)
    {
        cout << "ERROR: Duplicated field name '" << fields[1].Name << "'";
        return;
    }
    if (fieldNum > 1 && strcmp(fields[0].Name, fields[1].Name)==0)
    {
        cout << "ERROR: Duplicated field name '" << fields[1].Name << "'";
        return;
    }
    Dictionary *dict = g_DictPool.Get(DICT_FILE_PATH);
    char dipath[MAX_T_NAME+3];
    strcpy(dipath, tableName);
    strcat(dipath, ".di");
    char sppath[MAX_T_NAME+3];
    strcpy(sppath, tableName);
    strcat(sppath, ".sp");
    TableInfo *info = new TableInfo(tableName, dipath, sppath, fieldNum, new FieldInfo[fieldNum]);
    for (int i = 0; i < fieldNum; ++i)
    {
        strcpy(info->Fields[i].FName, fields[i].Name);
        info->Fields[i].IsPrimaryKey = fields[i].IsPrimaryKey;
        info->Fields[i].IsUnique = fields[i].IsUnique;
        info->Fields[i].Size = fields[i].Size;
        info->Fields[i].Type = fields[i].Type;
        info->Fields[i].BuildBPTIndex = false;
        info->Fields[i].BuildHashIndex = false;
    }
    if (dict->AddTable(info))
    {
        cout << "Query OK, 0 rows affected";
    }
    else
    {
        cout << "ERROR: Table '" << tableName << "' already exists";
    }
    delete info;
}

void DropTable(char *tableName)
{
    Dictionary *dict = g_DictPool.Get(DICT_FILE_PATH);
    TableInfo *info = new TableInfo();
    if (dict->GetTableInfo(tableName, info) && dict->DelTable(tableName))
    {
        cout << "Query OK, " << info->TupleNum << " rows affected";
    }
    else
    {
        cout << "ERROR: Unknown table '" << tableName << '\'';
    }
    delete info;
}

void CreateIndex(char type, char *tableName, char *fieldName)
{
    Dictionary *dict = g_DictPool.Get(DICT_FILE_PATH);
    TableInfo *info = new TableInfo();
    int res = 1;
    if (dict->GetTableInfo(tableName, info) &&
        (res = dict->UpdateIndex(tableName, fieldName, true, true, type)) == 0)
    {
        cout << "Query OK, " << info->TupleNum << " rows affected";
    }
    else
    {
        if (res == 1)
            cout << "ERROR: Unknown table '" << tableName << '\'';
        else if (res == 2)
            cout << "ERROR: Unknown field '" << tableName << '.' << fieldName << '\'';
        else
            cout << "ERROR: This(These) index(es) already exist on '" << tableName << '.' << fieldName << '\'';
    }
    delete info;
}

void DropIndex(char type, char *tableName, char *fieldName)
{
    Dictionary *dict = g_DictPool.Get(DICT_FILE_PATH);
    int res = 1;
    TableInfo *info = new TableInfo();
    if (dict->GetTableInfo(tableName, info) &&
        (res = dict->UpdateIndex(tableName, fieldName, false, false, type)) == 0)
    {
        cout << "Query OK, " << info->TupleNum << " rows affected";
    }
    else
    {
        if (res == 1)
            cout << "ERROR: Unknown table '" << tableName << '\'';
        else if (res == 2)
            cout << "ERROR: Unknown field '" << tableName << '.' << fieldName << '\'';
        else
            cout << "ERROR: No such index(es) exist on '" << tableName << '.' << fieldName << '\'';
    }
    delete info;
}

void InsertIntoTable(InsertSyntaxTree tree)
{
    Dictionary *dict = g_DictPool.Get(DICT_FILE_PATH);
    TableInfo *info = new TableInfo();
    if (dict->GetTableInfo(tree.TableName, info))
    {
        int i, j;
        char *fields = new char[GetRecordSize(info)];
        int pos = 0;
        Key key;
        bool bpt = false, hash = false, unique = false;
        for (i = 0; i < info->FieldNum; ++i)
        {
            if (tree.FieldListLen == 0)
            {
                if (tree.ValueListLen == info->FieldNum)
                {
                    if (tree.ValueTypeList[i] == STR)
                    {
                        memcpy(fields+pos, tree.ValueList[i].String, info->Fields[i].Size);
                    }
                    else
                    {
                        memcpy(fields+pos, &(tree.ValueList[i].Integer), info->Fields[i].Size);
                    }
                }
                else
                {
                    cout << "ERROR: Too few values to match the schema of table '" << tree.TableName << '\'';
                    delete []fields;
                    delete info;
                    return;
                }

            }
            else
            {
                for (j = 0; j < tree.FieldListLen; ++j)
                {
                    if (strcmp(info->Fields[i].FName, tree.FieldList[j].FieldName) == 0)
                    {
                        break;
                    }
                }
                if (j < tree.FieldListLen)
                {
                    if (tree.ValueTypeList[j] == STR)
                    {
                        memcpy(fields+pos, tree.ValueList[j].String, info->Fields[i].Size);
                    }
                    else
                    {
                        memcpy(fields+pos, &(tree.ValueList[j].Integer), info->Fields[i].Size);
                    }
                }
                else
                {
                    memset(fields+pos, 0, info->Fields[i].Size);
                }
            }
            if (info->Fields[i].BuildBPTIndex || info->Fields[i].BuildHashIndex)
            {
                bpt = info->Fields[i].BuildBPTIndex;
                hash = info->Fields[i].BuildHashIndex;
                unique = info->Fields[i].IsUnique;
                key = *(Key*)(fields+pos);
            }
            pos += info->Fields[i].Size;
        }
        BufferedTable *table = g_TablePool.Get(info);
        Addr addr = table->AllocRec();
        bool bptinsert = false, hashinsert = false;
        if (bpt)
        {
            if (table->BPTInited == false)
            {
                table->BPT_Init(true);
            }
            bptinsert = table->BPT_Insert(key, addr, unique);
        }
        if (hash)
        {
            if (table->HashInited == false)
            {
                table->Hash_Init(true);
            }
            bptinsert = table->Hash_Insert(key, addr, unique);
        }
        if (bpt != bptinsert || hash != hashinsert)
        {
            table->FreeRec(addr);
            cout << "ERROR: Conflict value on '" << key << "', 0 rows affected";
        }
        else
        {
            table->WriteRec(fields, addr);
            dict->UpdateStatis(tree.TableName, info->TupleNum+1, table->GetDataBlockNum());
            cout << "Query OK, 1 rows affected";
        }
        delete []fields;
    }
    else
    {
        cout << "ERROR: Unknown table '" << tree.TableName << '\'';
    }
    delete info;
}

void DeleteFromTable(DeleteSyntaxTree tree)
{
    if (tree.CondListLen > 0 && tree.CondList[0].Op == EQ &&
        tree.CondList[0].LeftType == 0 && tree.CondList[0].RightType == 2)
    {
        char tableName[MAX_T_NAME];
        char fieldName[MAX_F_NAME];
        strcpy(tableName, tree.TableName);
        strcpy(fieldName, tree.CondList[0].Left.TFOpd.FieldName);
        Key key = tree.CondList[0].Right.NumOpd;
        Dictionary *dict = g_DictPool.Get(DICT_FILE_PATH);
        TableInfo *info = new TableInfo();
        if (dict->GetTableInfo(tableName, info) == false)
        {
            delete info;
            cout << "ERROR: Unknown table '" << tableName << '\'';
            return;
        }
        BufferedTable *table = g_TablePool.Get(info);
        int res;
        res=table->BPT_Delete(key, true);
        cout << "Query OK, " << res << " rows affected";
        delete info;
        return;
    }
    cout << "ERROR: not supported this SQL";
}

void ShowHelp()
{
    cout << "Welcome to Tiny Data!" << endl << endl;
    FILE *in = fopen("lex+yacc/sql_man", "r");
    char line[200];
    while(fgets(line, 199, in))
    {
        cout << line;
    }
    fclose(in);
}

void LoadData(char *fileName, char *tableName)
{
    Dictionary *dict = g_DictPool.Get(DICT_FILE_PATH);
    TableInfo *info = new TableInfo();
    if (dict->GetTableInfo(tableName, info))
    {
        int i;
        char *fields = new char[GetRecordSize(info)];
        int pos;
        Key key;
        bool bpt = false, hash = false, unique = false;
        FILE *in = fopen(fileName, "r");
        char line[150];
        BufferedTable *table = g_TablePool.Get(info);
        int b, e;
        int recnum = 0, linenum = 0;

        while (fgets(line, 149, in) != NULL)
        {
            linenum++;
            pos = 0;
            b = e = 0;
            for (i = 0; i < info->FieldNum; ++i)
            {
                while(line[e] != ',' && line[e] != '\n' &&
                      line[e] != '\r' && line[e] != '\0')
                    ++e;
                if (info->Fields[i].Type == INT)
                {
                    line[e] = '\0';
                    key = atoi(line+b);
                    memcpy(fields+pos, &key, info->Fields[i].Size);
                }
                else
                {
                    memcpy(fields+pos, line+b, info->Fields[i].Size);
                }
                if (info->Fields[i].BuildBPTIndex || info->Fields[i].BuildHashIndex)
                {
                    bpt = info->Fields[i].BuildBPTIndex;
                    hash = info->Fields[i].BuildHashIndex;
                    unique = info->Fields[i].IsUnique;
                    key = *(Key*)(fields+pos);
                }
                pos += info->Fields[i].Size;

                b = ++e;
            }

            Addr addr = table->AllocRec();
            table->WriteRec(fields, addr);
            bool bptinsert = false, hashinsert = false;
            if (bpt)
            {
                if (table->BPTInited == false)
                {
                    table->BPT_Init(true);
                }
                bptinsert = table->BPT_Insert(key, addr, unique);
            }
            if (hash)
            {
                if (table->HashInited == false)
                {
                    table->Hash_Init(true);
                }
                bptinsert = table->Hash_Insert(key, addr, unique);
            }
            if (bpt != bptinsert || hash != hashinsert)
            {
                table->FreeRec(addr);
            }
            else
            {
                dict->UpdateStatis(tableName, ++(info->TupleNum), table->GetDataBlockNum());
                recnum++;
            }
        }
        cout << "Query OK, " << linenum << " read, " << recnum << " rows affected, "
            << linenum - recnum << " skiped";
        fclose(in);
        delete[] fields;

    }
    else
    {
        cout << "ERROR: Unknown table '" << tableName << '\'';
    }
    delete info;
}
