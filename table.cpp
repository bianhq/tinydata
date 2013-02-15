/**
表：
哈希索引、B+树索引的创建和维护
基于索引的等值查询、单元组插入、删除
只支持单个整型key，key上不能有重复值
*/

#include "table.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

Record::Record(): key(0), fields(NULL) {}
Record::Record(Key k): key(k), fields(NULL) {}
Record::~Record() {delete []fields; }

OFBlock::OFBlock(): RecNum(0), Next(-1) {}

ResultSet::ResultSet(): RecNum(0), List(NULL) {}
void ResultSet::Clear()
{
    uint i;
    if (List != NULL)
    {
        for (i = 0; i < RecNum; ++i)
        {
            if (List[i] != NULL)
            {
                delete []List[i];
            }
        }
        delete []List;
        List = NULL;
    }
}

char *ResultSet::operator [](uint i)
{
    if (i >= RecNum)
    {
        return NULL;
    }
    return List[i];
}

ResultSet::~ResultSet()
{
    Clear();
}

uint GetRecordSize(TableInfo *tinfo)
{
    uint size = 0, i, num = tinfo->FieldNum;
    for (i = 0; i < num; ++i)
    {
        size += tinfo->Fields[i].Length;
    }
    return size;
}

BufferedTable::BufferedTable(TableInfo *tinfo)
:StorageManager(tinfo->DataPath, tinfo->MetaPath)
{
    strcpy(this->MetaPath, tinfo->MetaPath);
    this->RecSize = GetRecordSize(tinfo);
    this->top = 0;
    this->BPTInited = false;
    this->HashInited = false;
    this->TabScanPageId = 0;
    this->TabScanRecId = 0;
    this->IScanNodeId = 0;
    this->IScanOFBId = 0;
    this->IScanInOFB = false;
    this->end = -1;
}

BufferedTable::~BufferedTable()
{
    this->Close();
}

void BufferedTable::Close()
{
    FILE *fp = fopen(MetaPath, "rb+");
    if (BPTInited == true)
    {
        fwrite(&(this->bpTree), sizeof(BPTree), 1, fp);
    }
    else
    {
        fseek(fp, sizeof(BPTree), SEEK_SET);
    }
    if (HashInited == true)
    {
        fwrite(&(this->g_HashIndex), sizeof(Hash_Index), 1, fp);
        fwrite(this->g_HashIndex.Directory.Items, sizeof(DirItem)*Hash_GetItemNum(), 1, fp);
    }

    fclose(fp);
    this->StorageManager.Flush();
}

void BufferedTable::Open(bool create)
{
    if (create == true)
    {
        this->StorageManager.Create();
        return;
    }
    this->StorageManager.Init();
}

bool BufferedTable::BPT_StackPush(BPT_Node *node)
{
    if (top < 20)
    {
        stack[top++] = *node;
        return true;
    }
    return false;
}

BPT_Node *BufferedTable::BPT_StackPop()
{
    if (top > 0)
    {
        return &stack[--top];
    }
    exit(0);
}

BPT_Node *BufferedTable::BPT_StackGetTop()
{
    if (top > 0)
    {
        return &stack[top-1];
    }
    exit(0);
}

void BufferedTable::BPT_ClearStack()
{
    top = 0;
}

void BufferedTable::BPT_Init(bool create)
{
    if (create == true)
    {
        bpTree.DataSegId = 0;
        bpTree.BPTSegId = 1;
        BPT_Node bptNode;
        bptNode.Role = LEAF;
        bptNode.KeyNum = 0;
        bptNode.Childern[0] = -1;
        bptNode.Self = BPT_AllocBlock(INDEX_FILE);
        this->Write(bptNode.Self, &bptNode, sizeof(BPT_Node));

        bpTree.LeafStart = bptNode.Self;
        bpTree.RootNode = bptNode.Self;
    }
    else
    {
        FILE *fp = fopen(MetaPath, "rb+");
        fread(&(this->bpTree), sizeof(BPTree), 1, fp);
        fclose(fp);
    }
    this->BPTInited = true;
}

void BufferedTable::BPT_AddRecToNode(Key key, Addr child, BPT_Node *node)
{
    int i;
    if (node->Role == LEAF)
    {
        node->Childern[node->KeyNum+1] = node->Childern[node->KeyNum];
        for (i = node->KeyNum - 1; i >= 0; --i)
        {
            if (key < node->Keys[i])
            {
                node->Keys[i+1] = node->Keys[i];
                node->Childern[i+1] = node->Childern[i];
            }
            else break;
        }
        node->Keys[i+1] = key;
        node->Childern[i+1] = child;
    }
    else
    {
        for (i = node->KeyNum - 1; i >= 0; --i)
        {
            if (key < node->Keys[i])
            {
                node->Keys[i+1] = node->Keys[i];
                node->Childern[i+2] = node->Childern[i+1];
            }

            else break;
        }
        node->Keys[i+1] = key;
        node->Childern[i+2] = child;
    }
    node->KeyNum++;
}

void BufferedTable::BPT_DelRecFrNode(Key key, BPT_Node *node)
{
    uint i;
    if (node->Role == LEAF)
    {
        for (i = 1; i < node->KeyNum; ++i)
        {
            if (node->Keys[i] > key)
            {
                node->Keys[i-1] = node->Keys[i];
                node->Childern[i-1] = node->Childern[i];
            }
        }
        node->Childern[node->KeyNum-1] = node->Childern[node->KeyNum];
    }
    else
    {
        for (i = 1; i < node->KeyNum; ++i)
        {
            if (node->Keys[i] > key)
            {
                node->Keys[i-1] = node->Keys[i];
                node->Childern[i] = node->Childern[i+1];
            }
        }
    }
    node->KeyNum--;
}

/**
释放一个指定的数据块
param:
  file: 0表示索引文件，1表示数据文件
  Addr: 指向要释放的块的起始偏移量
*/
void BufferedTable::BPT_FreeBlock(int file, Addr addr)
{
    if (file == INDEX_FILE)
    {
        this->Free(addr, BLOCK_SIZE);
    }
    else
    {
        this->Free(addr, this->RecSize);
    }
}

/**
分配一个指定的数据块
param:
  file: 0表示索引文件，1表示数据文件
return:
  指向分配的块的起始偏移量
*/
Addr BufferedTable::BPT_AllocBlock(byte file)
{
    if (file == INDEX_FILE)
    {
        return this->Alloc(bpTree.BPTSegId, BLOCK_SIZE);
    }
    else
    {
        return this->Alloc(bpTree.DataSegId, this->RecSize);
    }
}
/**
获取全局的B+树元数据
return:
  全局的BPTree变量
*/
BPTree BufferedTable::BPT_GetTree()
{
    if (BPTInited == true)
    {
        return bpTree;
    }
    else
    {
        exit(1);
    }
}
/**
更新全局的B+树元数据
param:
  全局的BPTree变量
*/
void BufferedTable::BPT_UpdateTree(BPTree newTree)
{
    if (BPTInited == true)
    {
        bpTree.LeafStart = newTree.LeafStart;
        bpTree.RootNode = newTree.RootNode;
    }
    else
    {
        exit(1);
    }
}

/**
该文件中要实现B+树中记录删除相关函数
*/
void BufferedTable::deleteFromNode(Key key)
{
    int i, j, k;
    BPTree bpTree = BPT_GetTree();
    BPT_Node *node = BPT_StackPop();
    if (node->Self == bpTree.RootNode)//如果要从根结点删除
    {
        if (node->Role == LEAF|| node->KeyNum > 1)
        {
            //如果要删除的根结点也是叶子结点，则直接将其写回索引文件
            BPT_DelRecFrNode(key, node);
            this->Write(node->Self, node, sizeof(BPT_Node));
            return;
        }
        //如果根结点要被删除
        bpTree.RootNode = node->Childern[0];
        BPT_UpdateTree(bpTree);
        BPT_FreeBlock(INDEX_FILE, node->Self);
        return;

    }
    if (node->Role == LEAF)
    {
        //如果是叶子结点
        //先将要删除的记录从叶子结点中删除
        BPT_DelRecFrNode(key, node);

        //处理
        if (node->KeyNum >= (MAX_KEY_NUM+1)/2)
        {
            //无需删除叶子
            this->Write(node->Self, node, sizeof(BPT_Node));
            return;
        }
        //需要删除叶子结点
        BPT_Node *parent = BPT_StackGetTop();
        //找到node在父结点中的下标
        for (i = 0; (uint)i <= parent->KeyNum; ++i)
        {
            if (parent->Childern[i] == node->Self)
            {
                break;
            }
        }
        if (i  > 0)
        {
            //有左兄弟结点，与左兄弟一起处理
            //读取左兄弟
            BPT_Node *left = new BPT_Node();
            this->Read(parent->Childern[i-1], left, sizeof(BPT_Node));
            if (left->KeyNum > (MAX_KEY_NUM+1)/2)
            {
                //左兄弟分一个记录给当前结点
                node->Childern[node->KeyNum+1] = node->Childern[node->KeyNum];
                for (j = node->KeyNum; j > 0; --j)
                {
                    node->Keys[j] = node->Keys[j-1];
                    node->Childern[j] = node->Childern[j-1];
                }
                node->Keys[0] = left->Keys[left->KeyNum-1];
                node->Childern[0] = left->Childern[left->KeyNum-1];
                node->KeyNum++;
                left->Childern[left->KeyNum-1] = node->Self;
                left->KeyNum--;
                parent->Keys[i-1] = node->Keys[0];
                this->Write(parent->Self, parent, sizeof(BPT_Node));
                this->Write(left->Self, left, sizeof(BPT_Node));
                this->Write(node->Self, node, sizeof(BPT_Node));
                delete left;
                return;
            }
            else
            {
                //与左兄弟合并
                left->Childern[left->KeyNum+node->KeyNum] = node->Childern[node->KeyNum];
                for (j = left->KeyNum, k = 0; (uint)j < left->KeyNum+node->KeyNum; ++j, ++k)
                {
                    left->Keys[j] = node->Keys[k];
                    left->Childern[j] = node->Childern[k];
                }
                left->KeyNum += node->KeyNum;
                BPT_FreeBlock(INDEX_FILE, node->Self);
                this->Write(left->Self, left, sizeof(BPT_Node));
                delete left;
                deleteFromNode(parent->Keys[i-1]);
            }
        }
        else
        {
            //与右兄弟一起处理
            //读取右兄弟
            BPT_Node *right = new BPT_Node();
            this->Read(parent->Childern[i+1], right, sizeof(BPT_Node));
            if (right->KeyNum > (MAX_KEY_NUM+1)/2)
            {
                //右兄弟分一个记录给当前结点
                node->Childern[node->KeyNum+1] = right->Self;
                node->Keys[node->KeyNum] = right->Keys[0];
                node->Childern[node->KeyNum] = right->Childern[0];
                node->KeyNum++;
                for (j = 0; (uint)j < right->KeyNum; ++j)
                {
                    right->Keys[j] = right->Keys[j+1];
                    right->Childern[j] = right->Childern[j+1];
                }
                right->KeyNum--;
                parent->Keys[i] = right->Keys[0];
                this->Write(parent->Self, parent, sizeof(BPT_Node));
                this->Write(right->Self, right, sizeof(BPT_Node));
                this->Write(node->Self, node, sizeof(BPT_Node));
                delete right;
                return;
            }
            else
            {
                //与右兄弟合并
                node->Childern[node->KeyNum+right->KeyNum] = right->Childern[right->KeyNum];
                for (j = node->KeyNum, k = 0; (uint)j < node->KeyNum+right->KeyNum; ++j, ++k)
                {
                    node->Keys[j] = right->Keys[k];
                    node->Childern[j] = right->Childern[k];
                }
                node->KeyNum += right->KeyNum;
                BPT_FreeBlock(INDEX_FILE, right->Self);
                this->Write(node->Self, node, sizeof(BPT_Node));
                delete right;
                deleteFromNode(parent->Keys[i]);
            }
        }
    }
    else
    {
        //非叶子结点
        //先将要删除的记录从该结点中删除
        BPT_DelRecFrNode(key, node);

        //处理
        if (node->KeyNum >= (MAX_KEY_NUM+1)/2-1)
        {
            //无需删除该结点
            this->Write(node->Self, node, sizeof(BPT_Node));
            return;
        }
        //需要删除该结点
        BPT_Node *parent = BPT_StackGetTop();
        //找到node在父结点中的下标
        for (i = 0; (uint)i <= parent->KeyNum; ++i)
        {
            if (parent->Childern[i] == node->Self)
            {
                break;
            }
        }
        if (i  > 0)
        {
            //该非叶子结点有左兄弟结点，与左兄弟一起处理
            BPT_Node *left = new BPT_Node();
            this->Read(parent->Childern[i-1], left, sizeof(BPT_Node));
            if (left->KeyNum > (MAX_KEY_NUM+1)/2-1)
            {
                //左兄弟分一个记录给当前结点
                node->Childern[node->KeyNum+1] = node->Childern[node->KeyNum];
                for (j = node->KeyNum; j > 0; --j)
                {
                    node->Keys[j] = node->Keys[j-1];
                    node->Childern[j] = node->Childern[j-1];
                }
                node->Keys[0] = parent->Keys[i-1];
                node->Childern[0] = left->Childern[left->KeyNum];
                node->KeyNum++;
                left->KeyNum--;
                parent->Keys[i-1] = left->Keys[left->KeyNum];
                this->Write(parent->Self, parent, sizeof(BPT_Node));
                this->Write(left->Self, left, sizeof(BPT_Node));
                this->Write(node->Self, node, sizeof(BPT_Node));
                delete left;
                return;
            }
            else
            {
                //与左兄弟合并
                left->Keys[left->KeyNum] = parent->Keys[i-1];
                left->KeyNum++;
                for (j = left->KeyNum, k = 0; (uint)j < left->KeyNum+node->KeyNum; ++j, ++k)
                {
                    left->Keys[j] = node->Keys[k];
                    left->Childern[j] = node->Childern[k];
                }
                left->KeyNum += node->KeyNum;
                left->Childern[left->KeyNum] = node->Childern[node->KeyNum];
                BPT_FreeBlock(INDEX_FILE, node->Self);
                this->Write(left->Self, left, sizeof(BPT_Node));
                delete left;
                deleteFromNode(parent->Keys[i-1]);
            }
        }
        else
        {
            //与右兄弟一起处理
            //读取右兄弟
            BPT_Node *right = new BPT_Node();
            this->Read(parent->Childern[i+1], right, sizeof(BPT_Node));
            if (right->KeyNum > (MAX_KEY_NUM+1)/2-1)
            {
                //从右兄弟拿一个到当前的node
                node->Keys[node->KeyNum] = parent->Keys[i];//把父结点原来的key拿下来，放到当前结点的末尾
                node->Childern[node->KeyNum+1] = right->Childern[0];//把右兄弟的第一个指针拿到当前结点末尾
                parent->Keys[i] = right->Keys[0];//把右兄弟的第一key上传到父结点

                node->KeyNum++;
                for (j = 0; (uint)j < right->KeyNum; ++j)
                {
                    right->Keys[j] = right->Keys[j+1];
                    right->Childern[j] = right->Childern[j+1];
                }
                right->KeyNum--;
                this->Write(parent->Self, parent, sizeof(BPT_Node));
                this->Write(right->Self, right, sizeof(BPT_Node));
                this->Write(node->Self, node, sizeof(BPT_Node));
                delete right;
                return;
            }
            else
            {
                //与右兄弟合并
                //把父结点的要删除的key拿下来，放到当前结点的末尾
                node->Keys[node->KeyNum] = parent->Keys[i];
                node->KeyNum++;
                for (j = node->KeyNum, k = 0; (uint)j < node->KeyNum+right->KeyNum; ++j, ++k)
                {
                    node->Keys[j] = right->Keys[k];
                    node->Childern[j] = right->Childern[k];
                }
                node->KeyNum += right->KeyNum;
                node->Childern[node->KeyNum] = right->Childern[right->KeyNum];
                BPT_FreeBlock(INDEX_FILE, right->Self);
                this->Write(node->Self, node, sizeof(BPT_Node));
                delete right;
                deleteFromNode(parent->Keys[i]);
            }
        }
    }
}

/**
删除记录
*/
int BufferedTable::BPT_Delete(Key key)
{
    BPTree bpTree = BPT_GetTree();
    Addr root = bpTree.RootNode;
    Addr res = seekInNode(true, root, key);
    uint num;
    //key不存在
    if (res < 0)
    {
        BPT_ClearStack();
        return 0;
    }
    if (IS_OFB_ADDR(res))
    {
        num = DeleteOFB(res);
    }
    else
    {
        BPT_FreeBlock(DATA_FILE, res);
        num = 1;
    }
    deleteFromNode(key);
    BPT_ClearStack();
    return num;
}

/**
该文件中要实现B+树中记录插入相关函数
*/
void BufferedTable::insertToNode(Key key, Addr addr)
{
    uint i, j;
    BPTree bpTree = BPT_GetTree();
    BPT_Node *node = BPT_StackPop();
    if (node->Role == LEAF)
    {
        //叶子结点
        BPT_AddRecToNode(key, addr, node);
        //插入工作已经完成
        if (node->KeyNum <= MAX_KEY_NUM)
        {
            //可以直接插入
            this->Write(node->Self, node, sizeof(BPT_Node));
            return;
        }
        //申请一个新的索引块
        BPT_Node *newNode = new BPT_Node();
        //初始化新的索引块
        newNode->KeyNum = (MAX_KEY_NUM+1)/2;
        newNode->Childern[newNode->KeyNum] = node->Childern[node->KeyNum];
        for (i = node->KeyNum - newNode->KeyNum, j = 0; i < node->KeyNum; ++i, ++j)
        {
            newNode->Keys[j] = node->Keys[i];
            newNode->Childern[j] = node->Childern[i];
        }
        newNode->Role = LEAF;
        newNode->Self = BPT_AllocBlock(INDEX_FILE);
        //更新老索引块
        node->KeyNum -= newNode->KeyNum;
        node->Childern[node->KeyNum] = newNode->Self;
        if (node->Self == bpTree.RootNode)
        {
            //根结点分裂
            BPT_Node *newRoot = new BPT_Node();
            newRoot->KeyNum = 1;
            newRoot->Keys[0] = newNode->Keys[0];
            newRoot->Role = NON_LEAF;
            newRoot->Self = BPT_AllocBlock(INDEX_FILE);
            newRoot->Childern[0] = node->Self;
            newRoot->Childern[1] = newNode->Self;
            bpTree.RootNode = newRoot->Self;
            BPT_UpdateTree(bpTree);
            //写入磁盘
            this->Write(node->Self, node, sizeof(BPT_Node));
            this->Write(newNode->Self, newNode, sizeof(BPT_Node));
            this->Write(newRoot->Self, newRoot, sizeof(BPT_Node));
            delete newNode;
            delete newRoot;
            return;
        }
        //一般分裂
        //写入磁盘
        this->Write(node->Self, node, sizeof(BPT_Node));
        this->Write(newNode->Self, newNode, sizeof(BPT_Node));
        //递归
        insertToNode(newNode->Keys[0], newNode->Self);
        delete newNode;
    }
    else
    {
        //非叶子结点
        //将要插入的记录先插入，之后再调整
        BPT_AddRecToNode(key, addr, node);
        if (node->KeyNum <= MAX_KEY_NUM)
        {
            //不用分裂
            this->Write(node->Self, node, sizeof(BPT_Node));
            return;
        }
        //要分裂
        //申请一个新的索引块
        BPT_Node *newNode = new BPT_Node();
        //初始化新的索引块
        newNode->KeyNum = (MAX_KEY_NUM+1)/2 - 1;
        newNode->Childern[newNode->KeyNum] = node->Childern[node->KeyNum];//拷贝
        for (i = node->KeyNum - newNode->KeyNum, j = 0; i < node->KeyNum; ++i, ++j)
        {
            newNode->Keys[j] = node->Keys[i];
            newNode->Childern[j] = node->Childern[i];
        }
        newNode->Role = NON_LEAF;
        newNode->Self = BPT_AllocBlock(INDEX_FILE);
        //更新老索引块
        node->KeyNum = node->KeyNum -1 - newNode->KeyNum;
        if (node->Self == bpTree.RootNode)
        {
            //根结点分裂
            BPT_Node *newRoot = new BPT_Node();
            newRoot->KeyNum = 1;
            newRoot->Keys[0] = node->Keys[node->KeyNum];
            newRoot->Role = NON_LEAF;
            newRoot->Self = BPT_AllocBlock(INDEX_FILE);
            newRoot->Childern[0] = node->Self;
            newRoot->Childern[1] = newNode->Self;
            bpTree.RootNode = newRoot->Self;
            BPT_UpdateTree(bpTree);
            //写入磁盘
            this->Write(node->Self, node, sizeof(BPT_Node));
            this->Write(newNode->Self, newNode, sizeof(BPT_Node));
            this->Write(newRoot->Self, newRoot, sizeof(BPT_Node));
            delete newRoot;
            delete newNode;
            return;
        }
        //一般分裂
        //写入磁盘
        this->Write(node->Self, node, sizeof(BPT_Node));
        this->Write(newNode->Self, newNode, sizeof(BPT_Node));
        //递归
        insertToNode(node->Keys[node->KeyNum], newNode->Self);
        delete newNode;
    }
}

/**
插入记录
*/
bool BufferedTable::BPT_Insert(Record *rec)
{
    uint i;
    BPTree bpTree = BPT_GetTree();
    Addr root = bpTree.RootNode;
    Addr res = seekInNode(true, root, rec->key);
    Addr p = BPT_AllocBlock(DATA_FILE);
    this->Write(p, rec->fields, RecSize);
    if (res >= 0)
    {//key已存在
        OFBlock *block = new OFBlock();
        if (IS_OFB_ADDR(res))
        {
            this->Read(res, block, sizeof(OFBlock));
            InsertToOFB(block, p);
            this->Write(res, block, sizeof(OFBlock));
        }
        else
        {
            Addr blockAddr = this->Alloc(OFB_SEGID, sizeof(OFBlock));
            InsertToOFB(block, res);
            InsertToOFB(block, p);
            BPT_Node *leafNode = BPT_StackGetTop();
            for (i = 0; i < leafNode->KeyNum; ++i)
            {
                if (leafNode->Childern[i] == res)
                {
                    leafNode->Childern[i] = blockAddr;
                }
            }
            this->Write(blockAddr, block, sizeof(OFBlock));
            this->Write(leafNode->Self, leafNode, sizeof(BPT_Node));
        }
        delete block;
        BPT_ClearStack();
    }
    else
    {
        insertToNode(rec->key, p);
        BPT_ClearStack();
    }
    return true;
}

/**
显示索引，深度优先遍历
*/
void BufferedTable::printNode(Addr p, uint level)
{
    uint i;
    BPT_Node node;
    this->Read(p, &node, sizeof(BPT_Node));
    for (i = 0; i < level; ++i)
    {
        printf("   |");
    }
    printf("---|");
    for (i = 0; i < node.KeyNum; ++i)
    {
        printf("%d|", node.Keys[i]);
    }
    printf("\n");
    if (node.Role != LEAF)
    {
        for (i = 0; i <= node.KeyNum; ++i)
        {
            printNode(node.Childern[i], level+1);
        }
    }
}

void BufferedTable::BPT_Print()
{
    BPTree bpTree = BPT_GetTree();
    Addr root = bpTree.RootNode;
    printNode(root, 0);
}

Addr BufferedTable::seekInNode(bool buildStack, Addr node, Key key)
{
    uint i;
    Addr tmp;
    BPT_Node *bptNode = new BPT_Node();
    this->Read(node, bptNode, sizeof(BPT_Node));
    if (buildStack)
    {
        BPT_StackPush(bptNode);
    }
    if (bptNode->Role == LEAF)
    {
        for (i = 0; i < bptNode->KeyNum; ++i)
        {
            if ((bptNode->Keys)[i] == key)
            {
                break;
            }
        }
        if (i < bptNode->KeyNum)
        {
            tmp = bptNode->Childern[i];
            delete bptNode;
            return tmp;
        }
        delete bptNode;
        return -1;
    }
    for (i = 0; i < bptNode->KeyNum; ++i)
    {
        if ((bptNode->Keys)[i] > key)
        {
            break;
        }
    }
    tmp = seekInNode(buildStack, bptNode->Childern[i], key);
    delete bptNode;
    return tmp;
}

/**
查找记录
*/
ResultSet *BufferedTable::BPT_Seek(Key key, ResultSet *recList)
{
    if (recList->List != NULL)
    {
        recList->Clear();
    }
    recList->RecNum = 0;
    BPTree bpTree = BPT_GetTree();
    Addr root = bpTree.RootNode;
    Addr addr = seekInNode(false, root, key);
    if (addr >= 0)
    {
        if (IS_OFB_ADDR(addr))
        {
            SeekFrmOFB(addr, recList);
        }
        else
        {
            recList->RecNum = 1;
            recList->List = (char **)malloc(sizeof(char*));
            recList->List[0] = new char[RecSize];
            this->Read(addr, recList->List[0], RecSize);
        }
        return recList;
    }
    return NULL;
}
/**
实现哈希索引的公共函数
*/

/***************函数实现************************/
//将一个键值转换位对应的哈希值
HValue BufferedTable::Hash_GetHValue(Key key)
{
    register uint a,b,c;
    a = b = c = 0x9e3779b9 + (uint)sizeof(HValue) + 3923095;
    a += key;
    final(a, b, c);
    return (HValue)(c & MAX_UI32);
}
//通过键值查询对应的桶的文件偏移
inline Addr BufferedTable::Hash_GetBuckEntry(Key key)
{
    return g_HashIndex.Directory.Items[Hash_GetItemIndex(key)].BuckEntry;
}
//通过键值查询对应的目录下标
inline uint BufferedTable::Hash_GetItemIndex(Key key)
{
    return Hash_GetHValue(key) << (32 - g_HashIndex.Directory.ValidBits) >> (32 - g_HashIndex.Directory.ValidBits);
}
//获取目录的目录项总数
inline uint BufferedTable::Hash_GetItemNum()
{
    return 1<<g_HashIndex.Directory.ValidBits;
}
//初始化哈希索引
void BufferedTable::Hash_Init(bool create)
{
    if (create == true)
    {
        g_HashIndex.DataSegId = 0;
        g_HashIndex.HashSegId = 2;
        Hash_Bucket bucket;
        uint DirRecNum;
        g_HashIndex.Directory.ValidBits = 1;
        DirRecNum = Hash_GetItemNum();
        g_HashIndex.Directory.Items = (DirItem*)malloc(DirRecNum*sizeof(DirItem));
        g_HashIndex.Directory.Items[0].BuckEntry = Hash_AllocBucket();
        g_HashIndex.Directory.Items[1].BuckEntry = Hash_AllocBucket();

        memset(&bucket, 0, sizeof(Hash_Bucket));
        bucket.ValidBits = 1;
        this->Write(g_HashIndex.Directory.Items[0].BuckEntry, &bucket, sizeof(Hash_Bucket));
        this->Write(g_HashIndex.Directory.Items[1].BuckEntry, &bucket, sizeof(Hash_Bucket));
    }
    else
    {
        FILE *fp = fopen(MetaPath, "rb+");
        fseek(fp, sizeof(BPTree), SEEK_SET);
        fread(&(this->g_HashIndex), sizeof(Hash_Index), 1, fp);
        this->g_HashIndex.Directory.Items = new DirItem[Hash_GetItemNum()];
        fread(this->g_HashIndex.Directory.Items, sizeof(DirItem)*Hash_GetItemNum(), 1, fp);
        fclose(fp);
    }
    this->HashInited = true;
}
//在索引文件上分配用于存放一个新桶的空间
Addr BufferedTable::Hash_AllocBucket()
{
    return this->Alloc(g_HashIndex.HashSegId, sizeof(Hash_Bucket));
}
//在数据文件上分配用于存放一条新数据的空间
Addr BufferedTable::Hash_AllocRecord()
{
    return this->Alloc(g_HashIndex.DataSegId, this->RecSize);
}

/**
**************************************************************
hash: seek.c
**************************************************************
*/
/* 在数组中用二分法查找,找到返回所在下标，找不到返回-1*/
int BufferedTable::Hash_BinSearch(Key x, Key a[], int n)
{
    int low, high, mid;
    low = 0;
    high = n-1;
    //注意，这里必须用<=, 用<不对，一直返回-1
    while(low <= high)
    {
        mid = (low + high) / 2;
        if(x < a[mid])
            high = mid - 1;
        else if(x > a[mid])
            low = mid + 1;
        else
            return mid;
    }
    return -1;
}

ResultSet *BufferedTable::Hash_Seek(Key key, ResultSet *recList)
{
    if (recList->List != NULL)
    {
        recList->Clear();
    }
    recList->RecNum = 0;
    Addr bucketAddr=Hash_GetBuckEntry(key);
    Hash_Bucket *bucket = new Hash_Bucket();
    this->Read(bucketAddr,bucket,sizeof(Hash_Bucket));
    int i;
    Addr addr;
    i = Hash_BinSearch(key,bucket->Keys,bucket->KeyNum);
    if (i < 0)
    {
        delete bucket;
        return NULL;
    }
    addr = bucket->Addrs[i];
    if (IS_OFB_ADDR(addr))
    {
        SeekFrmOFB(addr, recList);
    }
    else
    {
        recList->RecNum = 1;
        recList->List = (char **)malloc(sizeof(char*));
        recList->List[0] = new char[RecSize];
        this->Read(addr, recList->List[0], RecSize);
    }
    delete bucket;
    return recList;
}

/**
**************************************************************
hash: insert.c
**************************************************************
*/
//向桶中插入一个键值，并将数据写入数据文件
void BufferedTable::InsertToBucket(Key key, Addr addr, Hash_Bucket *bucket)
{
    register int i;
    for (i = bucket->KeyNum-1; i >= 0; --i)
    {
        if (bucket->Keys[i] > key)
        {
            bucket->Keys[i+1] = bucket->Keys[i];
            bucket->Addrs[i+1] = bucket->Addrs[i];
        }
        else break;
    }
    bucket->Keys[i+1] = key;
    bucket->Addrs[i+1] = addr;
    bucket->KeyNum++;
}
//计算一个键值在validBits下的有效值
inline uint BufferedTable::ValidValue(Key key, uint validBits)
{
    return Hash_GetHValue(key) << (32-validBits) >> (32-validBits);
}
//更新目录
void BufferedTable::UpdateDir(uint b, Addr pb, uint validBits)
{
    register uint i;
    for (i = 0; i < Hash_GetItemNum(); ++i)
    {
        //由于指向原桶的目录项无需变动，所以只更新指向新桶的目录项
        if ((i << (32-validBits) >> (32-validBits)) == b)
            g_HashIndex.Directory.Items[i].BuckEntry = pb;
    }
}

void BufferedTable::DoInsert(Key key, Addr addr, Addr entry, Hash_Bucket *bucket)
{
    if (bucket->KeyNum < MAX_BUCKET_SIZE)
    {
        //如果桶中记录个数小于最大记录个数，则直接插入
        InsertToBucket(key, addr, bucket);
        this->Write(entry, bucket, sizeof(Hash_Bucket));
    }
    else if (bucket->ValidBits < g_HashIndex.Directory.ValidBits)
    {
        //如果不能直接插入，但桶的有效位数小于目录的有效位数，则将桶的有效位数提高一位、尝试分裂成两个桶并插入
        bucket->ValidBits++;
        uint h = ValidValue(key, bucket->ValidBits);//提高一位后，键值的有效值
        uint a = ValidValue(key, bucket->ValidBits-1);//提高一位、并分裂后，原来桶中的有效值
        uint b = a | (0x1<<(bucket->ValidBits-1));//提高一位、并分裂后，分裂出的新桶中的有效值
        uint i, j, k;
        //测试分裂后原桶中剩余的记录个数
        for (i = j = 0; i < bucket->KeyNum; ++i)
            if (ValidValue(bucket->Keys[i], bucket->ValidBits) == a)
                j++;
        Hash_Bucket *newBucket = new Hash_Bucket();
        Addr newEntry = Hash_AllocBucket();
        if (h == a && j == bucket->KeyNum)
        {
            //如果新记录需要插入到原来的桶中，但是原来的桶仍然是满的，将目录中对应与新分裂的桶的目录项指向一个空桶，并继续尝试插入到原桶中（递归）
            newBucket->KeyNum = 0;
            newBucket->ValidBits = bucket->ValidBits;
            this->Write(newEntry, newBucket, sizeof(Hash_Bucket));
            UpdateDir(b , newEntry, bucket->ValidBits);//更新目录
            DoInsert(key, addr, entry, bucket);
            return;
        }
        if (h == b && j == 0)
        {
            //如果新记录需要插入到新分裂的桶中，但新分裂的桶是满的，则将原桶中的记录移入新桶，原桶清空，继续尝试插入到新桶中（递归）
            memcpy(&newBucket, &bucket, sizeof(Hash_Bucket));
            bucket->KeyNum = 0;
            this->Write(entry, &bucket, sizeof(Hash_Bucket));
            this->Write(newEntry, &newBucket, sizeof(Hash_Bucket));
            UpdateDir(b , newEntry, bucket->ValidBits);//更新目录
            DoInsert(key, addr, newEntry, newBucket);
            delete newBucket;
            return;
        }
        //其他情况下，分裂后的两个桶（原桶与新桶）可以装下新记录，于是直接插入
        newBucket->ValidBits = bucket->ValidBits;
        for (i = j = k = 0; i < bucket->KeyNum; ++i)
        {
            //将不属于原桶的记录移入新桶
            if (ValidValue(bucket->Keys[i], bucket->ValidBits) == a)
            {
                bucket->Keys[j] = bucket->Keys[i];
                bucket->Addrs[j] = bucket->Addrs[i];
                j++;
            }
            else
            {
                newBucket->Keys[k] = bucket->Keys[i];
                newBucket->Addrs[k] = bucket->Addrs[i];
                k++;
            }
        }
        newBucket->KeyNum = k;
        bucket->KeyNum = j;
        //将新记录插入
        if (h == a)
            InsertToBucket(key ,addr, bucket);
        else
            InsertToBucket(key, addr, newBucket);
        this->Write(entry, bucket, sizeof(Hash_Bucket));
        this->Write(newEntry, newBucket, sizeof(Hash_Bucket));
        UpdateDir(b , newEntry, bucket->ValidBits);//更新目录
        delete newBucket;
    }
    else
    {
        //如果目录有效位数等于桶的有效位数，则将目录扩展一位（长度增加一倍），将原来的目录项复制到新增的后半部分，再次尝试插入
        g_HashIndex.Directory.Items = (DirItem *)realloc(g_HashIndex.Directory.Items,
                                                         Hash_GetItemNum()*2*sizeof(DirItem));
        memcpy(g_HashIndex.Directory.Items+Hash_GetItemNum(),
               g_HashIndex.Directory.Items, Hash_GetItemNum()*sizeof(DirItem));
        g_HashIndex.Directory.ValidBits++;
        DoInsert(key, addr, entry, bucket);
    }
}

bool BufferedTable::Hash_Insert(Record *rec)
{
    int i;
    Addr entry = Hash_GetBuckEntry(rec->key), res;
    Hash_Bucket *bucket = new Hash_Bucket();
    this->Read(entry, bucket, sizeof(Hash_Bucket));
    Addr p = Hash_AllocRecord();
    this->Write(p, rec->fields, RecSize);
    i = Hash_BinSearch(rec->key, bucket->Keys, bucket->KeyNum);
    if (i >= 0)
    {
        res = bucket->Addrs[i];
        OFBlock *block = new OFBlock();
        if (IS_OFB_ADDR(res))
        {
            this->Read(res, block, sizeof(OFBlock));
            InsertToOFB(block, p);
            this->Write(res, block, sizeof(OFBlock));
        }
        else
        {
            Addr blockAddr = this->Alloc(OFB_SEGID, sizeof(OFBlock));
            InsertToOFB(block, res);
            InsertToOFB(block, p);
            bucket->Addrs[i] = blockAddr;
            this->Write(blockAddr, block, sizeof(OFBlock));
            this->Write(entry, bucket, sizeof(Hash_Bucket));
        }
        delete block;
    }
    else
    {
        DoInsert(rec->key, p, entry, bucket);
    }
    delete bucket;
    return true;
}

/**
**************************************************************
hash: delete.c
**************************************************************
*/
//删除
int BufferedTable::Hash_Delete(Key key)
{
    //查找key值所在桶,读取桶的内容到bucket
    Hash_Bucket *bucket = new Hash_Bucket();
    Addr buckentry = Hash_GetBuckEntry(key), addr;
    this->Read(buckentry, bucket, sizeof(Hash_Bucket));
    int i = Hash_BinSearch(key, bucket->Keys, bucket->KeyNum);
    if (i < 0)
    {
        delete bucket;
        return 0;
    }
    addr = bucket->Addrs[i];
    int num;
    if (IS_OFB_ADDR(addr))
    {
        num = DeleteOFB(addr);
    }
    else
    {
        this->Free(addr, RecSize);
        num = 1;
    }
    uint k;

    //在桶中删除该key，key之后的值顺序前移
    for(k=i+1; k < bucket->KeyNum; k++)
    {
        if(bucket->Keys[k] > key)
        {
            bucket->Keys[k-1] = bucket->Keys[k];
            bucket->Addrs[k-1] = bucket->Addrs[k];
        }
    }
    bucket->KeyNum--;
    this->Write(buckentry, bucket, sizeof(Hash_Bucket));//将删除key后的桶的内容写入索引文件
    delete bucket;
    return num;
}

Addr BufferedTable::Alloc(uint segId, uint length, bool useBuf)
{
    if (useBuf == true)
    {
        return StorageManager.Alloc(segId, length);
    }
    return -1;
}
int BufferedTable::Free(Addr pageEnt, uint length, bool useBuf)
{
    if (useBuf == true)
    {
        return StorageManager.Free(pageEnt, length);
    }
    return -1;
}
int BufferedTable::Read(Addr pageEnt, void *buf, uint length, bool useBuf)
{
    if (useBuf == true)
    {
        return StorageManager.Read(pageEnt, buf, length);
    }
    return -1;
}

int BufferedTable::Write(Addr pageEnt, const void *buf, uint length, bool useBuf)
{
    if (useBuf == true)
    {
        return StorageManager.Write(pageEnt, buf, length);
    }
    return -1;
}

void BufferedTable::TabScan_Init()
{
    this->StorageManager.Flush();
    this->TabScanPageId = 0;
    this->TabScanRecId = 0;
}
//get next record's field. return the length of the field.
int BufferedTable::TabScan_Next(char *fields)
{
    int res = 0;
    if (this->TabScanRecId == 0)
    {
        while ((res=this->StorageManager.ReadDataPage(this->TabScanPageId, this->TmpFrame.FilePage)) == 0)
        {
            //该页无效
            this->TabScanPageId++;
        }
        if (res < 0)
        {
            //页表已经越界
            return -1;
        }
        this->StorageManager.PageToFrame(this->TmpFrame.FilePage, &this->TmpFrame);
    }
    memcpy(fields,
           this->TmpFrame.FilePage+this->TmpFrame.USTable[this->TabScanRecId].Pos,
           this->TmpFrame.USTable[this->TabScanRecId].Length);
    if (this->TabScanRecId == this->TmpFrame.USNum-1u)
    {
        this->TabScanRecId = 0;
        this->TabScanPageId ++;
    }
    else
    {
        this->TabScanRecId++;
    }
    return this->TmpFrame.USTable[this->TabScanRecId].Length;
}

//参数分别为要插入的溢出块地址，要插入的键值，要插入的地址，返回值为OFB地址
void BufferedTable::InsertToOFB(OFBlock *block, Addr recAddr)
{
    uint i, j;
    if (block->RecNum < MAX_OFB_REC_NUM)
    {
        for (i = 0; i < block->RecNum; ++i)
        {
            if (block->RecordAddrs[i] > recAddr)
            {
                break;
            }
        }
        if (i < block->RecNum)
        {
            for (j = block->RecNum; j > i; --j)
            {
                block->RecordAddrs[j] = block->RecordAddrs[j-1];
            }
        }
        block->RecordAddrs[i] = recAddr;
        block->RecNum++;
        return;
    }
    OFBlock *newBlock = new OFBlock();
    if (block->Next != -1)
    {
        this->StorageManager.Read(block->Next, newBlock, sizeof(OFBlock));
        InsertToOFB(newBlock, recAddr);
        this->StorageManager.Write(block->Next, newBlock, sizeof(OFBlock));
        delete newBlock;
        return;
    }
    Addr newOfbAddr = this->StorageManager.Alloc(OFB_SEGID, sizeof(OFBlock));
    block->Next = newOfbAddr;
    InsertToOFB(newBlock, recAddr);
    this->StorageManager.Write(newOfbAddr, newBlock, sizeof(OFBlock));
    delete newBlock;
}

//返回值为负表示删除失败
int BufferedTable::DeleteOFB(Addr ofbAddr)
{
    uint i, sum = 0;
    OFBlock *block = new OFBlock();
    this->StorageManager.Read(ofbAddr, block, sizeof(OFBlock));
    if (block->Next != -1)
    {
        sum = DeleteOFB(block->Next);
    }
    for (i = 0; i < block->RecNum; ++i)
    {
        this->StorageManager.Free(block->RecordAddrs[i], RecSize);
        sum++;
    }
    this->StorageManager.Free(ofbAddr, sizeof(OFBlock));
    delete block;
    return sum;
}

void BufferedTable::SeekFrmOFB(Addr ofbAddr, ResultSet* recList)
{
    OFBlock *block = new OFBlock();
    uint i, j;
    this->StorageManager.Read(ofbAddr, block, sizeof(OFBlock));
    i = recList->RecNum;
    recList->RecNum += block->RecNum;
    recList->List = (char **)realloc(recList->List, sizeof(char*)*recList->RecNum);
    for (j = 0; i < recList->RecNum; ++i, ++j)
    {
        recList->List[i] = new char[RecSize];
        this->StorageManager.Read(block->RecordAddrs[j], recList->List[i], RecSize);
    }
    if (block->Next != -1)
    {
        SeekFrmOFB(block->Next, recList);
    }
    delete block;
}

//index scan
//init for index scan
bool BufferedTable::IndexScan_Init(Key begin, Key end)
{
    uint i;
    if (begin > end)
    {
        return false;
    }
    this->StorageManager.Flush();
    BPTree bpTree = BPT_GetTree();
    Addr root = bpTree.RootNode;
    Addr addr;
    BPT_ClearStack();
    while ((addr = seekInNode(true, root, begin)) < 0)//找到begin所对应的桶和记录的地址
    {
        begin ++;
        BPT_ClearStack();
        if (begin > end)
        {
            return false;
        }
    }

    this->IScanNode = *this->BPT_StackGetTop();
    for (i = 0; i < this->IScanNode.KeyNum; ++i)
    {
        if (this->IScanNode.Childern[i] == addr)
        {
            break;
        }
    }
    this->IScanNodeId = i;
    this->IScanOFBId = 0;
    if (IS_OFB_ADDR(addr))
    {//如果是溢出块的地址
        this->IScanInOFB = true;
        this->StorageManager.Read(addr, &(this->IScanOFB), sizeof(OFBlock));
    }
    else
    {
        this->IScanInOFB = false;
    }
    this->end = end;
    return true;
}

//get next record's field. return the length of the field.
int BufferedTable::IndexScan_Next(char *fields)
{
    if (this->IScanInOFB == true && (this->IScanOFBId != this->IScanOFB.RecNum || this->IScanOFB.Next != -1))
    {
        if (this->IScanOFBId == this->IScanOFB.RecNum)
        {
            this->StorageManager.Read(this->IScanOFB.Next, &(this->IScanOFB), sizeof(OFBlock));
            this->IScanOFBId = 0;
        }
        this->StorageManager.Read(this->IScanOFB.RecordAddrs[this->IScanOFBId], fields, RecSize);
        this->IScanOFBId++;
    }
    else
    {
        this->IScanInOFB = false;
        if (this->IScanNode.Childern[this->IScanNodeId] == -1)
        {
            return -1;
        }
        if (this->IScanNodeId == this->IScanNode.KeyNum)
        {
            this->StorageManager.Read(this->IScanNode.Childern[this->IScanNodeId], &(this->IScanNode), sizeof(BPT_Node));
            this->IScanNodeId = 0;
        }
        if (this->IScanNode.Keys[this->IScanNodeId] > this->end)
        {
            return -1;
        }
        if (IS_OFB_ADDR(this->IScanNode.Childern[this->IScanNodeId]))
        {
            this->IScanInOFB = true;
            this->StorageManager.Read(this->IScanNode.Childern[this->IScanNodeId], &(this->IScanOFB), sizeof(OFBlock));
            this->IScanOFBId = 0;
            this->StorageManager.Read(this->IScanOFB.RecordAddrs[this->IScanOFBId], fields, RecSize);
            this->IScanOFBId++;
        }
        else
        {
            this->StorageManager.Read(this->IScanNode.Childern[this->IScanNodeId], fields, RecSize);
        }
        this->IScanNodeId++;
    }
    return RecSize;
}
