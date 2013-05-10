%{
#include <stdio.h>
#include <string.h>
#include <time.h>
#include "sqlexe.h"

#define yyPrintRunTime(t1, t2) printf(" (%.3lf sec)\n\n", (double)((t2) - (t1)) / CLOCKS_PER_SEC)

extern int lineno;
extern int yylex();
extern int yyerror(char *msg);

clock_t start, end;
uint fieldNum;
Field fields[MAX_FIELDS];

SelectSyntaxTree selectSyntaxTree;
InsertSyntaxTree insertSyntaxTree;
DeleteSyntaxTree deleteSyntaxTree;

CondListItem condList[MAX_COND_LIST_LEN];
uint condListLen;
OrderListItem orderList[MAX_ORDER_LIST_LEN];
uint orderListLen;
FromListItem fromList[MAX_FROM_LIST_LEN];
uint fromListLen;
SelectListItem selectList[MAX_SELECT_LIST_LEN];
uint selectListLen;

uint fieldListLen;//等于0表示没有field list
uint valueListLen;
FieldListItem fieldList[MAX_FIELD_LIST_LEN];
ValueListItem valueList[MAX_VALUE_LIST_LEN];
FieldType valueTypeList[MAX_VALUE_LIST_LEN];//0表示String, 1表示Integer

%}
%union {
	int intval;
	char *strval;
}

%start sql_list

%token <strval> NAME
%token <strval> STRING
%token <intval> NUMBER
%token <strval> COMPARISON
%type <strval> table_name
%type <strval> field_name
%type <strval> file_name
%type <intval> primary_key
%type <intval> unique
%type <intval> index_type

	/* operator */
%left AND
%left COMPARISON /* < <= > >= <> = */
%left '+' '-'
%left '*' '/'

	/* reserved keywords */
%token SELECT FROM WHERE ORDER BY ASC DESC
%token ALL UNIQUE DISTINCT
%token CREATE TABLE DROP INDEX
%token INSERT INTO VALUES DELETE
%token CHARACTER INTEGER DATE
%token SHOW TABLES
%token EXIT
%token LOAD DATA HASH BPT HELP ON PRIMARY KEY

%%

	/* start place */
sql_list:
		sql
		{
			lineno = 1;
			return 0;
		}
	|	EXIT
		{
			return 10;
		}
	|	HELP
		{
			ShowHelp();
			return 0;
		}
	;
sql:
		';'
	|	create_table
	|	drop_table
	|	create_index
	|	drop_index
	|	select_stat
	|	insert_stat
	|	delete_stat
	|	show_table
	|	load_data
	;
	/* show all tables */
show_table:
		SHOW TABLES ';'
		{
			start = clock();
			ShowTables();
			end = clock();
			yyPrintRunTime(start, end);
		}
	;
	/* create table */
create_table:
		CREATE TABLE table_name '(' field_def_list ')' ';'
		{
			start = clock();
			CreateTable($3, fieldNum, fields);
			end = clock();
			yyPrintRunTime(start, end);
		}
	;
table_name:
		NAME
		{
			$$ = $1;
		}
	;
field_def_list:
		field_def_list_item
		{
			fieldNum ++;
		}
	|	field_def_list ',' field_def_list_item
		{
			fieldNum ++;
		}
	;
field_def_list_item:
		field_name data_type primary_key unique
		{
			strcpy(fields[fieldNum].Name, $1);
			fields[fieldNum].IsPrimaryKey = $3;
			if ($3 == 1)
			{
				fields[fieldNum].IsUnique = 1;
			}
			else
			{
				fields[fieldNum].IsUnique = $4;
			}
		}
	;
field_name:
		NAME
		{
			$$ = $1;
		}
	;
data_type:
		CHARACTER '(' NUMBER ')'
		{
			fields[fieldNum].Type = CHAR;
			fields[fieldNum].Size = $3;
		}
	|	INTEGER
		{
			fields[fieldNum].Type = INT;
			fields[fieldNum].Size = sizeof(int);
		}
	|	DATE
		{
			fields[fieldNum].Type = STR;
			fields[fieldNum].Size = 10;
		}
	;
primary_key:
		{
			$$ = 0;
		}
	|	PRIMARY KEY
		{
			$$ = 1;
		}
	;
	/* drop table */
drop_table:
		DROP TABLE table_name ';'
		{
			start = clock();
			DropTable($3);
			end = clock();
			yyPrintRunTime(start, end);
		}
	;
	/* create index */
create_index:
		CREATE index_type INDEX NAME ON table_name '(' field_name ')' ';'
		{
			start = clock();
			switch ($2)
			{
			case 0:
				CreateIndex('b', $6, $8);
				break;
			case 1:
				CreateIndex('h', $6, $8);
				break;
			case 2:
				CreateIndex('b', $6, $8);
				break;
			case 3:
				CreateIndex('a', $6, $8);
				break;
			}
			end = clock();
			yyPrintRunTime(start, end);
		}
	;
	/* drop index */
drop_index:
		DROP index_type INDEX ON table_name '(' field_name ')' ';'
		{
			start = clock();
			switch ($2)
			{
			case 0:
				DropIndex('a', $5, $7);
				break;
			case 1:
				DropIndex('h', $5, $7);
				break;
			case 2:
				DropIndex('b', $5, $7);
				break;
			case 3:
				DropIndex('a', $5, $7);
				break;
			}
			end = clock();
			yyPrintRunTime(start, end);
		}
	;
index_type:
		/* empty */
		{
			$$ = 0;
		}
	|	HASH
		{
			$$ = 1;
		}
	|	BPT
		{
			$$ = 2;
		}
	|	ALL
		{
			$$ = 3;
		}
	;
	/* select statements */
select_stat:
		select_clause FROM from_list where_clause order_clause ';'
		{
			start = clock();
			selectSyntaxTree.SelectList = selectList;
			selectSyntaxTree.SelectListLen = selectListLen;
			selectSyntaxTree.FromList = fromList;
			selectSyntaxTree.FromListLen = fromListLen;
			selectSyntaxTree.CondList = condList;
			selectSyntaxTree.CondListLen = condListLen;
			selectSyntaxTree.OrderList = orderList;
			selectSyntaxTree.OrderListLen = orderListLen;
			SelectFromTable(selectSyntaxTree);
			end = clock();
			yyPrintRunTime(start, end);
		}
	;
select_clause:
		SELECT unique select_list
		{
			selectSyntaxTree.IsUnique = $2;
		}
	|	SELECT unique '*'
		{
			selectSyntaxTree.IsUnique = $2;
			selectListLen = 0;
		}
	;
unique:
		/* empty */
		{
			$$ = 0;
		}
	|	ALL
		{
			$$ = 0;
		}
	|	DISTINCT
		{
			$$ = 1;
		}
	|	UNIQUE
		{
			$$ = 1;
		}
	;
select_list:
		field_name
		{
			strcpy(selectList[selectListLen].FieldName, $1);
			selectList[selectListLen].HasTableName = 0;
			selectListLen ++;
		}
	|	select_list ',' field_name
		{
			strcpy(selectList[selectListLen].FieldName, $3);
			selectList[selectListLen].HasTableName = 0;
			selectListLen ++;
		}
	|	table_name '.' field_name
		{
			strcpy(selectList[selectListLen].TableName, $1);
			strcpy(selectList[selectListLen].FieldName, $3);
			selectList[selectListLen].HasTableName = 1;
			selectListLen ++;
		}
	|	select_list ',' table_name '.' field_name
		{
			strcpy(selectList[selectListLen].TableName, $3);
			strcpy(selectList[selectListLen].FieldName, $5);
			selectList[selectListLen].HasTableName = 1;
			selectListLen ++;
		}
	;
from_list:
		table_name
		{
			strcpy(fromList[fromListLen].TableName, $1);
			fromListLen ++;
		}
	|	from_list ',' table_name
		{
			strcpy(fromList[fromListLen].TableName, $3);
			fromListLen ++;
		}
	;
where_clause:
		/* empty */
		{
			condListLen = 0;
		}
	|	WHERE condition
	;
condition:
		expr
		{
			condListLen ++;
		}
	|	condition AND expr
		{
			condListLen ++;
		}
	;
expr:
		field_name COMPARISON field_name
		{
			strcpy(condList[condListLen].Left.TFOpd.FieldName, $1);
			condList[condListLen].Left.TFOpd.HasTableName = 0;
			condList[condListLen].LeftType = 0;
			if (strcmp($2, "=") == 0)
				condList[condListLen].Op = EQ;
			else if (strcmp($2, ">=") == 0)
				condList[condListLen].Op = GE;
			else if (strcmp($2, "<=") == 0)
				condList[condListLen].Op = LE;
			else if (strcmp($2, ">") == 0)
				condList[condListLen].Op = GT;
			else if (strcmp($2, "<") == 0)
				condList[condListLen].Op = LT;
			else if (strcmp($2, "<>") == 0)
				condList[condListLen].Op = NE;
			strcpy(condList[condListLen].Right.TFOpd.FieldName, $3);
			condList[condListLen].Right.TFOpd.HasTableName = 0;
			condList[condListLen].RightType = 0;
		}
	|	table_name '.' field_name COMPARISON field_name
		{
			strcpy(condList[condListLen].Left.TFOpd.TableName, $1);
			strcpy(condList[condListLen].Left.TFOpd.FieldName, $3);
			condList[condListLen].Left.TFOpd.HasTableName = 1;
			condList[condListLen].LeftType = 0;
			if (strcmp($4, "=") == 0)
				condList[condListLen].Op = EQ;
			else if (strcmp($4, ">=") == 0)
				condList[condListLen].Op = GE;
			else if (strcmp($4, "<=") == 0)
				condList[condListLen].Op = LE;
			else if (strcmp($4, ">") == 0)
				condList[condListLen].Op = GT;
			else if (strcmp($4, "<") == 0)
				condList[condListLen].Op = LT;
			else if (strcmp($4, "<>") == 0)
				condList[condListLen].Op = NE;
			strcpy(condList[condListLen].Right.TFOpd.FieldName, $5);
			condList[condListLen].Right.TFOpd.HasTableName = 0;
			condList[condListLen].RightType = 0;
		}
	|	field_name COMPARISON table_name '.' field_name
		{
			strcpy(condList[condListLen].Left.TFOpd.FieldName, $1);
			condList[condListLen].Left.TFOpd.HasTableName = 0;
			condList[condListLen].LeftType = 0;
			if (strcmp($2, "=") == 0)
				condList[condListLen].Op = EQ;
			else if (strcmp($2, ">=") == 0)
				condList[condListLen].Op = GE;
			else if (strcmp($2, "<=") == 0)
				condList[condListLen].Op = LE;
			else if (strcmp($2, ">") == 0)
				condList[condListLen].Op = GT;
			else if (strcmp($2, "<") == 0)
				condList[condListLen].Op = LT;
			else if (strcmp($2, "<>") == 0)
				condList[condListLen].Op = NE;
			strcpy(condList[condListLen].Right.TFOpd.TableName, $3);
			strcpy(condList[condListLen].Right.TFOpd.FieldName, $5);
			condList[condListLen].Right.TFOpd.HasTableName = 1;
			condList[condListLen].RightType = 0;
		}
	|	table_name '.' field_name COMPARISON table_name '.' field_name
		{
			strcpy(condList[condListLen].Left.TFOpd.TableName, $1);
			strcpy(condList[condListLen].Left.TFOpd.FieldName, $3);
			condList[condListLen].Left.TFOpd.HasTableName = 1;
			condList[condListLen].LeftType = 0;
			if (strcmp($4, "=") == 0)
				condList[condListLen].Op = EQ;
			else if (strcmp($4, ">=") == 0)
				condList[condListLen].Op = GE;
			else if (strcmp($4, "<=") == 0)
				condList[condListLen].Op = LE;
			else if (strcmp($4, ">") == 0)
				condList[condListLen].Op = GT;
			else if (strcmp($4, "<") == 0)
				condList[condListLen].Op = LT;
			else if (strcmp($4, "<>") == 0)
				condList[condListLen].Op = NE;
			strcpy(condList[condListLen].Right.TFOpd.TableName, $5);
			strcpy(condList[condListLen].Right.TFOpd.FieldName, $7);
			condList[condListLen].Right.TFOpd.HasTableName = 1;
			condList[condListLen].RightType = 0;
		}
	|	field_name COMPARISON NUMBER
		{
			strcpy(condList[condListLen].Left.TFOpd.FieldName, $1);
			condList[condListLen].Left.TFOpd.HasTableName = 0;
			condList[condListLen].LeftType = 0;
			if (strcmp($2, "=") == 0)
				condList[condListLen].Op = EQ;
			else if (strcmp($2, ">=") == 0)
				condList[condListLen].Op = GE;
			else if (strcmp($2, "<=") == 0)
				condList[condListLen].Op = LE;
			else if (strcmp($2, ">") == 0)
				condList[condListLen].Op = GT;
			else if (strcmp($2, "<") == 0)
				condList[condListLen].Op = LT;
			else if (strcmp($2, "<>") == 0)
				condList[condListLen].Op = NE;
			condList[condListLen].Right.NumOpd = $3;
			condList[condListLen].RightType = 2;
		}
	|	table_name '.' field_name COMPARISON NUMBER
		{
			strcpy(condList[condListLen].Left.TFOpd.TableName, $1);
			strcpy(condList[condListLen].Left.TFOpd.FieldName, $3);
			condList[condListLen].Left.TFOpd.HasTableName = 1;
			condList[condListLen].LeftType = 0;
			if (strcmp($4, "=") == 0)
				condList[condListLen].Op = EQ;
			else if (strcmp($4, ">=") == 0)
				condList[condListLen].Op = GE;
			else if (strcmp($4, "<=") == 0)
				condList[condListLen].Op = LE;
			else if (strcmp($4, ">") == 0)
				condList[condListLen].Op = GT;
			else if (strcmp($4, "<") == 0)
				condList[condListLen].Op = LT;
			else if (strcmp($4, "<>") == 0)
				condList[condListLen].Op = NE;
			condList[condListLen].Right.NumOpd = $5;
			condList[condListLen].RightType = 2;
		}
	|	field_name COMPARISON STRING
		{
			strcpy(condList[condListLen].Left.TFOpd.FieldName, $1);
			condList[condListLen].Left.TFOpd.HasTableName = 0;
			condList[condListLen].LeftType = 0;
			if (strcmp($2, "=") == 0)
				condList[condListLen].Op = EQ;
			else if (strcmp($2, ">=") == 0)
				condList[condListLen].Op = GE;
			else if (strcmp($2, "<=") == 0)
				condList[condListLen].Op = LE;
			else if (strcmp($2, ">") == 0)
				condList[condListLen].Op = GT;
			else if (strcmp($2, "<") == 0)
				condList[condListLen].Op = LT;
			else if (strcmp($2, "<>") == 0)
				condList[condListLen].Op = NE;
			strcpy(condList[condListLen].Right.StrOpd, $3);
			condList[condListLen].RightType = 1;
		}
	|	table_name '.' field_name COMPARISON STRING
		{
			strcpy(condList[condListLen].Left.TFOpd.TableName, $1);
			strcpy(condList[condListLen].Left.TFOpd.FieldName, $3);
			condList[condListLen].Left.TFOpd.HasTableName = 1;
			condList[condListLen].LeftType = 0;
			if (strcmp($4, "=") == 0)
				condList[condListLen].Op = EQ;
			else if (strcmp($4, ">=") == 0)
				condList[condListLen].Op = GE;
			else if (strcmp($4, "<=") == 0)
				condList[condListLen].Op = LE;
			else if (strcmp($4, ">") == 0)
				condList[condListLen].Op = GT;
			else if (strcmp($4, "<") == 0)
				condList[condListLen].Op = LT;
			else if (strcmp($4, "<>") == 0)
				condList[condListLen].Op = NE;
			strcpy(condList[condListLen].Right.StrOpd, $5);
			condList[condListLen].RightType = 1;
		}
	;
order_clause:
		/* empty */
		{
			orderListLen = 0;
		}
	|	ORDER BY order_list
	;
order_list:
		order_list_item
		{
			orderListLen ++;
		}
	|	order_list ',' order_list_item
		{
			orderListLen ++;
		}
	;
order_list_item:
		field_name
		{
			strcpy(orderList[orderListLen].FieldName, $1);
			orderList[orderListLen].HasTableName = 0;
			orderList[orderListLen].IsASC = 1;
		}
	|	field_name ASC
		{
			strcpy(orderList[orderListLen].FieldName, $1);
			orderList[orderListLen].HasTableName = 0;
			orderList[orderListLen].IsASC = 1;
		}
	|	field_name DESC
		{
			strcpy(orderList[orderListLen].FieldName, $1);
			orderList[orderListLen].HasTableName = 0;
			orderList[orderListLen].IsASC = 0;
		}
	|
		table_name '.' field_name
		{
			strcpy(orderList[orderListLen].TableName, $1);
			strcpy(orderList[orderListLen].FieldName, $3);
			orderList[orderListLen].HasTableName = 1;
			orderList[orderListLen].IsASC = 1;
		}
	|	table_name '.' field_name ASC
		{
			strcpy(orderList[orderListLen].TableName, $1);
			strcpy(orderList[orderListLen].FieldName, $3);
			orderList[orderListLen].HasTableName = 1;
			orderList[orderListLen].IsASC = 1;
		}
	|	table_name '.' field_name DESC
		{
			strcpy(orderList[orderListLen].TableName, $1);
			strcpy(orderList[orderListLen].FieldName, $3);
			orderList[orderListLen].HasTableName = 1;
			orderList[orderListLen].IsASC = 0;
		}
	;
	/* insert statements */
insert_stat:
		INSERT INTO table_name VALUES '(' value_list ')' ';'
		{
			start = clock();
			strcpy(insertSyntaxTree.TableName, $3);
			insertSyntaxTree.FieldListLen = 0;
			insertSyntaxTree.FieldList = NULL;
			insertSyntaxTree.ValueListLen = valueListLen;
			insertSyntaxTree.ValueList = valueList;
			insertSyntaxTree.ValueTypeList = valueTypeList;
			InsertIntoTable(insertSyntaxTree);
			end = clock();
			yyPrintRunTime(start, end);
		}
	|	INSERT INTO table_name '(' field_list ')' VALUES '(' value_list ')' ';'
		{
			start = clock();
			strcpy(insertSyntaxTree.TableName, $3);
			insertSyntaxTree.FieldListLen = fieldListLen;
			insertSyntaxTree.FieldList = fieldList;
			insertSyntaxTree.ValueListLen = valueListLen;
			insertSyntaxTree.ValueList = valueList;
			insertSyntaxTree.ValueTypeList = valueTypeList;
			InsertIntoTable(insertSyntaxTree);
			end = clock();
			yyPrintRunTime(start, end);
		}
	;
value_list:
		NUMBER
		{
			valueList[valueListLen].Integer = $1;
			valueTypeList[valueListLen] = INT;
			valueListLen ++;
		}
	|	STRING
		{
			strcpy(valueList[valueListLen].String, $1);
			valueTypeList[valueListLen] = STR;
			valueListLen ++;
		}
	|	value_list ',' NUMBER
		{
			valueList[valueListLen].Integer = $3;
			valueTypeList[valueListLen] = INT;
			valueListLen ++;
		}
	|	value_list ',' STRING
		{
			strcpy(valueList[valueListLen].String, $3);
			valueTypeList[valueListLen] = STR;
			valueListLen ++;
		}
	;
field_list:
		field_name
		{
			strcpy(fieldList[fieldListLen].FieldName, $1);
			fieldListLen ++;
		}
	|	field_list ',' field_name
		{
			strcpy(fieldList[fieldListLen].FieldName, $3);
			fieldListLen ++;
		}
	;
	/* delete statement */
delete_stat:
		DELETE FROM table_name where_clause ';'
		{
			start = clock();
			strcpy(deleteSyntaxTree.TableName, $3);
			deleteSyntaxTree.CondList = condList;
			deleteSyntaxTree.CondListLen = condListLen;
			DeleteFromTable(deleteSyntaxTree);
			end = clock();
			yyPrintRunTime(start, end);
		}
	;
load_data:
		LOAD DATA FROM file_name INTO TABLE table_name ';'
		{
			start = clock();
			LoadData($4, $7);
			end = clock();
			yyPrintRunTime(start, end);
		}
	;
file_name:
		STRING
		{
			$$ = $1;
		}
%%
int parserInit()
{
	start = end = 0;
	fieldNum = 0;
	memset(fields, 0, sizeof(Field)*MAX_FIELDS);

	memset(&selectSyntaxTree, 0, sizeof(SelectSyntaxTree));
	memset(&insertSyntaxTree, 0, sizeof(InsertSyntaxTree));
	memset(&deleteSyntaxTree, 0, sizeof(DeleteSyntaxTree));

	memset(condList, 0, sizeof(CondListItem)*MAX_COND_LIST_LEN);
	condListLen = 0;
	memset(orderList, 0, sizeof(OrderListItem)*MAX_ORDER_LIST_LEN);
	orderListLen = 0;
	memset(fromList, 0, sizeof(FromListItem)*MAX_FROM_LIST_LEN);
	fromListLen = 0;
	memset(selectList, 0, sizeof(SelectListItem)*MAX_SELECT_LIST_LEN);
	selectListLen = 0;

	fieldListLen = 0;
	valueListLen = 0;
	memset(fieldList, 0, sizeof(FieldListItem)*MAX_FIELD_LIST_LEN);
	memset(valueList, 0, sizeof(ValueListItem)*MAX_VALUE_LIST_LEN);
	memset(valueTypeList, 0, sizeof(unsigned char)*MAX_VALUE_LIST_LEN);
	return 0;
}

