#include <iostream>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include "sqlexe.h"
#include "pool.h"
using namespace std;

//下面3个main函数：
//第2个的测试文件是3.in或者4.in或者5.in
//第2\3个的测试文件是scan.txt
#define EXE 4

#if EXE==0
int main()
{
    //创建数据字典
    Dictionary dict("db");
    dict.Create();
    dict.Open();
    TableInfo info("123", "123_data", "123_meta", 1, new FieldInfo[1]);
    info.Fields[0].SetInfo("Id", true, INT, sizeof(Key));
    dict.AddTable(&info);
    dict.Close();

    BufferedTable *table = new BufferedTable(&info);
    //创建表
    table->Open(true);
    //创建索引
    table->BPT_Init(true);
    //table->Hash_Init(true);
    char cmd[100];
    Key key;
    Record rec;
    ResultSet rs;
    rec.fields = new char[GetRecordSize(&info)];
    bool ok = false;
    while (fgets(cmd, 99, stdin) != NULL)
    {
        cmd[99] = '\0';
        if (cmd[strlen(cmd)-2] == '\r')
        {
            cmd[strlen(cmd)-2] = '\0';
        }
        else
        {
            cmd[strlen(cmd)-1] = '\0';
        }
        switch (cmd[0])
        {
        case 'i':
            rec.key = atoi(cmd + 7);
            memcpy(rec.fields, &rec.key, sizeof(Key));
            ok = table->BPT_Insert(&rec);
            //ok = table->Hash_Insert(&rec);
            break;
        case 's':
            key = atoi(cmd + 5);
            ok = (table->BPT_Seek(key, &rs) != NULL);
            //ok = (table->Hash_Seek(&rec) != NULL);
            break;
        case 'd':
            key = atoi(cmd + 7);
            ok = (table->BPT_Delete(key) > 0);
            //ok = table->Hash_Delete(key);
            break;
        case 'p':
            table->BPT_Print();
            break;
        case 'e':
            table->Close();
            exit(0);
        default:
            break;
        }
        if (cmd[0] == 's' || cmd[0] == 'd' || cmd[0] == 'i')
        {
            printf("%s %s\r\n", cmd, ok?"ok":"failed");
        }
    }

    //初始化全表扫描
    table->TabScan_Init();
    char fields[4];
    //开始全表扫描
    while(table->TabScan_NextRec(fields) > 0)
    {
        cout << *((Key *)fields) << endl;
    }

    table->Close();
    delete table;
    return 0;
}
#endif

#if EXE==1
int main()
{
    //创建数据字典
    Dictionary dict("db");
    dict.Create();
    dict.Open();
    TableInfo info("scan", "scan_data", "scan_meta", 2, new FieldInfo[2]);
    info.Fields[0].SetInfo("Id", true, INT, sizeof(Key));
    info.Fields[1].SetInfo("Value", false, STR, 96);
    dict.AddTable(&info);
    dict.Close();

    BufferedTable *table = new BufferedTable(&info);
    //创建表
    table->Open(true);
    //创建B+树索引
    table->BPT_Init(true);
    char cmd[150];
    Key key;
    Record rec;
    rec.fields = new char[96];
    uint i, sum = 0;
    FILE *in = fopen("test/Scan.txt", "r");
    FILE *out = fopen("test/Scan.out.txt", "w");
    //开始插入数据并创建B+树索引
    while (fgets(cmd, 149, in) != NULL)
    {
        for (i = 0; cmd[i] != ' '; ++i);
        cmd[i] = '\0';
        rec.key = atoi(cmd);
        for (i = i+2; cmd[i] == ' '; ++i);
        memcpy(rec.fields, &rec.key, sizeof(Key));
        strncpy(rec.fields+sizeof(Key), cmd+i, 8);
        strncpy(rec.fields+sizeof(Key)+8, cmd+i+8+6, 12);
        strncpy(rec.fields+sizeof(Key)+8+12, cmd+i+8+6+12+6, 20);
        strncpy(rec.fields+sizeof(Key)+8+12+20, cmd+i+8+6+12+6+20+6, 48);
        rec.fields[sizeof(Key)+88] = '\0';
        if (table->BPT_Insert(&rec))
        {
            sum ++;
        }
    }
    //初始化全表扫描
    table->TabScan_Init();
    char fields[96];
    //开始全表扫描
    while(table->TabScan_NextRec(fields) > 0)
    {
        fprintf(out, "%d\t%s\n", *((Key *)fields), fields+4);
    }
    fprintf(out, "%d\n", sum);
    table->Close();
    fclose(in);
    fclose(out);
    delete table;
    return 0;
}
#endif

#if EXE==2
int main()
{
    //创建数据字典
    Dictionary dict("db");
    dict.Create();
    dict.Open();
    TableInfo info("scan", "scan_data", "scan_meta", 2, new FieldInfo[2]);
    info.Fields[0].SetInfo("Id", true, INT, sizeof(Key));
    info.Fields[1].SetInfo("Value", false, STR, 96);
    dict.AddTable(&info);
    dict.Close();

    BufferedTable *table = new BufferedTable(&info);
    //创建表
    table->Open(true);
    //创建B+树索引
    table->Hash_Init(true);
    char cmd[150];
    Record rec;
    rec.fields = new char[96];
    uint i, sum = 0;
    FILE *in = fopen("test/Scan.txt", "r");
    FILE *out = fopen("test/Scan.out.txt", "w");
    while (fgets(cmd, 149, in) != NULL)
    {
        for (i = 0; cmd[i] != ' '; ++i);
        cmd[i] = '\0';
        rec.key = atoi(cmd);
        for (i = i+2; cmd[i] == ' '; ++i);
        memcpy(rec.fields, &rec.key, sizeof(Key));
        strncpy(rec.fields+sizeof(Key), cmd+i, 8);
        strncpy(rec.fields+sizeof(Key)+8, cmd+i+8+6, 12);
        strncpy(rec.fields+sizeof(Key)+8+12, cmd+i+8+6+12+6, 20);
        strncpy(rec.fields+sizeof(Key)+8+12+20, cmd+i+8+6+12+6+20+6, 48);
        rec.fields[sizeof(Key)+88] = '\0';
        if (table->Hash_Insert(&rec))
        {
            sum ++;
        }
    }
    cout << "total: "<< sum << " lines.\n";
    uint num = 0;
    ResultSet list;
    for (i = 0; i < 1000000; ++i)
    {
        table->Hash_Seek(i, &list);
        {
            num += list.RecNum;
            if (list.RecNum)
                cout << *((Key*)list[0]) << '\t' << list[0]+4 << endl;
        }
        list.Clear();
    }

    table->TabScan_Init();
    char fields[96];
    while(table->TabScan_Next(fields) == 100)
    {
        fprintf(out, "%d\t%s\n", *((Key *)fields), fields+4);
    }
    cout << num << endl;
    table->Close();
    fclose(in);
    fclose(out);
    delete table;
    return 0;
}
#endif

#if EXE==3
int main()
{
    //创建数据字典
    Dictionary dict("db");
    dict.Create();
    dict.Open();
    TableInfo info("scan", "scan_data", "scan_meta", 2, new FieldInfo[2]);
    info.Fields[0].SetInfo("Id", false, false, true, false, INT, sizeof(Key));
    info.Fields[1].SetInfo("Value", false, false, true, false, STR, 92);
    dict.AddTable(&info);
    dict.Close();

    BufferedTable *table = new BufferedTable(&info);
    //创建表
    table->Open(true);
    //创建B+树索引
    table->Hash_Init(true);
    char cmd[150];
    char *fields = new char[95];
    Key key;
    Addr addr;
    uint i, sum = 0;
    FILE *in = fopen("test/Scan.txt", "r");
    FILE *out = fopen("test/Scan.out.txt", "w");
    while (fgets(cmd, 149, in) != NULL)
    {
        for (i = 0; cmd[i] != ' '; ++i);
        cmd[i] = '\0';
        key = atoi(cmd);
        for (i = i+2; cmd[i] == ' '; ++i);
        memcpy(fields, &key, sizeof(Key));
        strncpy(fields+sizeof(Key), cmd+i, 8);
        strncpy(fields+sizeof(Key)+8, cmd+i+8+6, 12);
        strncpy(fields+sizeof(Key)+8+12, cmd+i+8+6+12+6, 20);
        strncpy(fields+sizeof(Key)+8+12+20, cmd+i+8+6+12+6+20+6, 50);
        fields[sizeof(Key)+90] = '\0';
        addr = table->AllocRec();
        table->WriteRec(fields, addr);
        if (table->Hash_Insert(key, addr, false))
        {
            sum ++;

        }
    }
    cout << "total: "<< sum << " lines.\n";

    //table->IndexScan_Init(0, 1000000);
    //while(table->IndexScan_Next(fields)>0)
    {
        //fprintf(out, "%d\t%s\n", *((Key *)fields), fields+4);
    }

    //FILE *out = fopen("1234", "w");
    /*    uint num = 0;
        ResultSet list;

        for (i = 0; i < 10000000; ++i)
        {
            table->Hash_Seek(i, &list);
            {
                num += list.RecNum;

                if (list.RecNum)
                {*(list[0]+93)='\0';
                    fprintf(out, "%d\t%s\n", *((Key*)list[0]), list[0]+4);
                }

            }
            list.Clear();
        }
    */

    table->TabScan_Init();
    char field111[96];

    int nnn = GetRecordSize(&info);
    while(table->TabScan_Next(field111) == nnn)
    {
        field111[94]='\0';
        fprintf(out, "%d\t%s\n", *((Key *)field111), field111+4);
        fflush(out);
    }
    //fprintf(out, "%d\n", num);


    table->Close();
    fclose(in);
    fclose(out);
    delete []fields;
    delete table;
    return 0;
}
#endif

#if EXE==4
extern int parserInit();
extern int yyparse();
int main()
{
    cout << "Welcome to Tiny Data !"
     << "\n\nVersion 0.0.1. \nCopyright (C) 2012-2013. Info, RUC. All rights reserved.\n\n";
    cout << "Tiny Data> ";
    while(true)
    {
        parserInit();
        if (yyparse() == 10)
        {
            break;
        }
    }
    g_DictPool.Close();
    g_TablePool.Close();
    cout << "Byebye." << endl;
    return 0;
}
#endif
