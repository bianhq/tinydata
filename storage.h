/**
存储管理
段页式文件块管理
基于Clock算法的请求分页式缓冲区管理
段页表用了定长的数组，之后要改成动态数组或者链表
采用64位逻辑地址：段表号32位，页表号20位，页内偏移12位
*/

#ifndef STORAGE_H_INCLUDED
#define STORAGE_H_INCLUDED

#include "util.h"
#include <stdio.h>

#define BLOCK_SIZE 3692u
#define META_HEAD_SIZE 1024u

#define SEG_NUM 4

/**
file_manager
*/
#define MAX_PAGE_TAB_LEN 1048576
#define INIT_PAGE_TAB_LEN 51200
#define MAX_US_NUM 100
#define PAGE_SIZE 4096u
#define PAGE_HEAD_SIZE 404u

/**
buf_manager
*/
#define BUFFED_FRAMES 100u

struct PageTabItem
{
    bool Used;
    Addr PageEnt;
    PageTabItem();
};

struct SegTabItem
{
    bool Used;
    uint PageTableLength;
    PageTabItem *PageTable;
    SegTabItem();
};
struct UsedSpace
{
    PageAddr Pos;
    ushort Length;
};

struct Frame
{
    bool Valid;
    ushort UsedBytes;
    ushort USNum;
    UsedSpace USTable[MAX_US_NUM];
    byte FilePage[PAGE_SIZE];
};

struct FastTabItem
{
    Addr PageEnt;
    uint FrameId;
    bool A;//访问位，在Clock置换算法中使用
    bool M;//修改位，在换出是，决定是否需要写回文件
    bool P;//Pin位
    bool SLock;//共享锁
    bool Xlock;//排它锁
};

struct FastTable
{
    uint length;
    FastTabItem Items[BUFFED_FRAMES];
};

class Storage
{
private:
    Addr p;
    uint index[SEG_NUM];//标记上一次查询到的、快表中的下标
    Frame Buffer[BUFFED_FRAMES];
    FastTable FastTables[SEG_NUM];
    SegTabItem SegPage[SEG_NUM];
    FILE *Data, *Meta;// = NULL = NULL
    char MetaPath[MAX_PATH_LEN];
    char DataPath[MAX_PATH_LEN];

    Addr FM_AllocPage(uint segId);
    int FM_FreePage(Addr pageEnt);
    int FM_ReadPage(Addr pageEnt, void *buf);
    int FM_WritePage(Addr pageEnt, const void *buf);
    void FM_LoadSegPage();
    void FM_FlushSegPage();
    inline uint FM_GetSegId(Addr addr);
    inline uint FM_GetPageId(Addr addr);

    FastTabItem* ClockAlg(uint segId);
    void CloseFileStream();
    int HitBuffer(Addr addr);
    void OpenFileStream();
    int ReadFrmFile(char f, uint offset, void *buf, size_t nbytes);
    int WriteToFile(char f, uint offset, const void *buf, size_t nbytes);

public:
    Storage(char *dataPath, char *metaPath);
    ~Storage();
    Addr Alloc(uint segId, uint length);
    int Free(Addr pageEnt, uint length);
    int Read(Addr pageEnt, void *buf, uint length);
    int ReadDataPage(uint pageId, void *page);
    int Write(Addr pageEnt, const void *buf, uint length);
    void PageToFrame(byte *filePage, Frame *frame);
    void FrameToPage(Frame *frame, byte *filePage);
    void Create();
    void Init();
    void Flush();
};

#endif // STORAGE_H_INCLUDED
