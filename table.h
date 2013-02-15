#ifndef TABLE_H_INCLUDED
#define TABLE_H_INCLUDED

#include <stdio.h>
#include "util.h"
#include "storage.h"
#include "dict.h"

uint GetRecordSize(TableInfo *tinfo);

#define IS_OFB_ADDR(a) (a&0x200000000)
//#define MAX_OFB_REC_NUM (BLOCK_SIZE-16)/8
#define MAX_OFB_REC_NUM 10
#define OFB_SEGID 3

struct Record
{
    Key key;
    char *fields;
    Record();
    Record(Key k);
    ~Record();
};

struct ResultSet
{
    uint RecNum;
    char **List;
    void Clear();
    char *operator [](uint i);
    ResultSet();
    ~ResultSet();
};

struct OFBlock
{
    uint RecNum;
    Addr Next;
    Addr RecordAddrs[MAX_OFB_REC_NUM];
    OFBlock();
};

/**
hash structs and macros definition
*/
//桶的最大容量
#define MAX_BUCKET_SIZE ((BLOCK_SIZE-8)/12-1)
//#define MAX_BUCKET_SIZE 3
//hash宏定义
// Rotate a uint32 value left by k bits - note multiple evaluation!
#define rot(x,k) (((x)<<(k)) | ((x)>>(32-(k))))
//final -- final mixing of 3 32-bit values (a,b,c) into c
#define final(a,b,c){ c ^= b; c -= rot(b,14); a ^= c; a -= rot(c,11); b ^= a; b -= rot(a,25); c ^= b; c -= rot(b,16); a ^= c; a -= rot(c, 4); b ^= a; b -= rot(a,14); c ^= b; c -= rot(b,24); }

//哈希桶
struct Hash_Bucket
{
    uint ValidBits;//有效位个数
    uint KeyNum;//键值个数
    Key Keys[MAX_BUCKET_SIZE];//桶内的键值
    Addr Addrs[MAX_BUCKET_SIZE];//对应与键值的、指向数据文件中记录的起始文件偏移量
};
//目录项
struct DirItem
{
    Addr BuckEntry;//指向索引文件中桶的起始文件偏移
};
//目录
struct Dir
{
    uint ValidBits;//有效位个数
    DirItem *Items;//目录项数组
};
//哈希索引
struct Hash_Index
{
    uint DataSegId;
    uint HashSegId;
    Dir Directory;
};

/**
bptree structs and macros definition
*/

#define NON_LEAF 0
#define LEAF 1
#define INDEX_FILE 0
#define DATA_FILE 1
#define MAX_KEY_NUM ((BLOCK_SIZE-36)/12)
//#define MAX_KEY_NUM 3

//B+树的结点结构
struct BPT_Node
{
    uint KeyNum;//有效的key个数
    uint Role;//0表示非叶子结点，1表示叶子结点
    Addr Self;
    Key Keys[MAX_KEY_NUM+1];//有效的key
    Addr Childern[MAX_KEY_NUM + 2];//作为叶子结点时最后一个指针指向下一个兄弟结点
};

//B+树的元数据结构
struct BPTree
{
    uint DataSegId;//数据文件的文件描述符
    uint BPTSegId;//索引文件的文件描述符
    Addr LeafStart;//第一个叶子结点在数据文件中的文件偏移量
    Addr RootNode;//根结点在索引文件中的文件偏移量
};

/**
Buffered Table difinition
*/
class BufferedTable
{
private:
    char MetaPath[MAX_PATH_LEN];
    Frame TmpFrame;
    uint RecSize;
    Storage StorageManager;
    bool BPTInited;
    bool HashInited;
    uint TabScanPageId;
    uint TabScanRecId;

    uint IScanNodeId;
    uint IScanOFBId;
    BPT_Node IScanNode;
    OFBlock IScanOFB;
    bool IScanInOFB;
    Key end;
    //参数分别为要插入的溢出块地址，要插入的键值，要插入的地址，返回值为OFB地址
    void InsertToOFB(OFBlock *block, Addr recAddr);
    //返回值为负表示删除失败
    int DeleteOFB(Addr ofbAddr);
    void SeekFrmOFB(Addr ofbAddr, ResultSet* recList);

    Addr Alloc(uint segId, uint length, bool useBuf=true);
    int Free(Addr pageEnt, uint length, bool useBuf=true);
    int Read(Addr pageEnt, void *buf, uint length, bool useBuf=true);
    int Write(Addr pageEnt, const void *buf, uint length, bool useBuf=true);
    //bptree
    BPTree bpTree;
    BPT_Node stack[20];
    int top;
    BPTree BPT_GetTree();
    bool BPT_StackPush(BPT_Node *node);
    void BPT_ClearStack();
    BPT_Node *BPT_StackPop();
    BPT_Node *BPT_StackGetTop();
    void BPT_UpdateTree(BPTree);
    void BPT_FreeBlock(int file, Addr addr);
    void BPT_AddRecToNode(Key key, Addr addr, BPT_Node *node);
    void BPT_DelRecFrNode(Key key, BPT_Node *node);
    Addr BPT_AllocBlock(byte file);

    Addr seekInNode(bool buildStack, Addr node, Key key);
    void deleteFromNode(Key key);
    void insertToNode(Key key, Addr addr);
    void printNode(Addr p, uint level);

    //hash
    Hash_Index g_HashIndex;
    //将一个键值转换位对应的哈希值
    HValue Hash_GetHValue(Key key);
    //通过键值查询对应的桶的文件偏移
    inline Addr Hash_GetBuckEntry(Key key);
    //获取目录的目录项总数
    inline uint Hash_GetItemNum();
    //通过键值查询对应的目录下标
    inline uint Hash_GetItemIndex(Key key);
    //在索引文件上分配用于存放一个新桶的空间
    Addr Hash_AllocBucket();
    //在数据文件上分配用于存放一条新数据的空间
    Addr Hash_AllocRecord();
    int Hash_BinSearch(Key x, Key a[], int n);

    void InsertToBucket(Key key, Addr addr, Hash_Bucket *bucket);
    inline uint ValidValue(Key key, uint validBits);
    void UpdateDir(uint b, Addr pb, uint validBits);
    void DoInsert(Key key, Addr addr, Addr entry, Hash_Bucket *bucket);

public:
    BufferedTable(TableInfo *tinfo);
    ~BufferedTable();
    void Close();
    void Open(bool create=false);
    //bptree
    void BPT_Init(bool create=false);
    //插入记录
    bool BPT_Insert(Record *rec);
    //删除记录
    int BPT_Delete(Key key);
    //查找记录
    ResultSet *BPT_Seek(Key key, ResultSet *recList);
    //显示索引
    void BPT_Print();

    //hash
    //初始化哈希索引
    void Hash_Init(bool create=false);
    //插入
    bool Hash_Insert(Record *rec);
    //查询
    ResultSet *Hash_Seek(Key key, ResultSet *recList);
    //删除
    int Hash_Delete(Key key);

    //table scan
    //init for table scan
    void TabScan_Init();
    //get next record's field. return the length of the field.
    int TabScan_Next(char *fields);

    //index scan
    //init for index scan
    bool IndexScan_Init(Key begin, Key end);//begin要比end小
    //get next record's field. return the length of the field.
    int IndexScan_Next(char *fields);
};

#endif // TABLE_H_INCLUDED
