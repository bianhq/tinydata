#include "storage.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

PageTabItem::PageTabItem(): Used(false), PageEnt(-1) {}
SegTabItem::SegTabItem(): Used(false), PageTableLength(0), PageTable(NULL) {}

Storage::Storage(char *dataPath, char *metaPath)
{
    strcpy(this->DataPath, dataPath);
    strcpy(this->MetaPath, metaPath);
    this->Data = NULL;
    this->Meta = NULL;
    this->p = 0;
    memset(this->index, 0, sizeof(this->index));
}

Storage::~Storage()
{
    CloseFileStream();
    uint i;
    for (i = 0; i < SEG_NUM; ++i)
    {
        delete []SegPage[i].PageTable;
    }
}

//Clock置换算法
//在缓冲区初始化之后的第一轮置换中，允许一个段抢占所有的缓冲区块
//在之后的置换中，允许一个段最多抢占50%的缓冲区块，以防止某个段占用过多的缓冲块、使得其他段频繁的换进换出
FastTabItem* Storage::ClockAlg(uint segId)
{
    uint i, j, len;

    uint sumLen = 0, maxLen = 0, maxLenSegId = 0, maxLenFrameId = 0;
    for (i = 0; i < SEG_NUM; ++i)
    {
        if (FastTables[i].length > maxLen)
        {
            maxLen = FastTables[i].length;
            maxLenSegId = i;
        }
        sumLen += FastTables[i].length;
    }
    //如果缓冲区未满，则抢占空的缓冲区
    if (sumLen < BUFFED_FRAMES)
    {
        for (i = 0; i < BUFFED_FRAMES; ++i)
        {
            if (Buffer[i].Valid == false)
            {
                break;
            }
        }
        len = FastTables[segId].length;
        memset(&FastTables[segId].Items[len], 0, sizeof(FastTabItem));
        FastTables[segId].Items[len].FrameId = i;
        FastTables[segId].length++;
        Buffer[i].Valid = true;
        return &FastTables[segId].Items[len];
    }
    //如果缓冲区已满，则抢占占有缓冲区较多的段的缓冲区
    len = FastTables[maxLenSegId].length;
    //对快表做循环的扫描，必然会找到一个可以换出的页框
    for (i = index[maxLenSegId]; true; ++i)
    {
        i %= len;
        if (FastTables[maxLenSegId].Items[i].A == false)
        {
            //如果页框可以换出
            if (FastTables[maxLenSegId].Items[i].M == true)
            {
                if (Buffer[FastTables[maxLenSegId].Items[i].FrameId].USNum == 0)
                {
                    FM_FreePage(FastTables[maxLenSegId].Items[i].PageEnt);
                }
                else
                {
                    FrameToPage(&Buffer[FastTables[maxLenSegId].Items[i].FrameId],
                                Buffer[FastTables[maxLenSegId].Items[i].FrameId].FilePage);
                    FM_WritePage(FastTables[maxLenSegId].Items[i].PageEnt,
                                Buffer[FastTables[maxLenSegId].Items[i].FrameId].FilePage);
                }
            }
            break;
        }
        else
        {
            FastTables[maxLenSegId].Items[i].A = false;
        }
    }
    if (maxLenSegId != segId)
    {
        FastTables[maxLenSegId].length--;
        len--;
        index[maxLenSegId] = i%len;
        maxLenFrameId = FastTables[maxLenSegId].Items[i].FrameId;
        for (j = i; j < len; ++j)
        {
            FastTables[maxLenSegId].Items[j] = FastTables[maxLenSegId].Items[j+1];
        }
        len = FastTables[segId].length;
        FastTables[segId].length++;
        FastTables[segId].Items[len].PageEnt = -1;
        FastTables[segId].Items[len].A = false;
        FastTables[segId].Items[len].M = false;
        FastTables[segId].Items[len].P = false;
        FastTables[segId].Items[len].SLock = false;
        FastTables[segId].Items[len].Xlock = false;
        FastTables[segId].Items[len].FrameId = maxLenFrameId;
        Buffer[FastTables[segId].Items[len].FrameId].Valid = true;
        return &FastTables[segId].Items[len];//返回换出页框对应的快表表项
    }
    index[segId] = (i+1)%len;
    FastTables[segId].Items[i].PageEnt = -1;
    //将快表项重新初始化，只保留页框号
    FastTables[segId].Items[i].A = false;
    FastTables[segId].Items[i].M = false;
    FastTables[segId].Items[i].P = false;
    FastTables[segId].Items[i].SLock = false;
    FastTables[segId].Items[i].Xlock = false;
    Buffer[FastTables[segId].Items[i].FrameId].Valid = true;
    return &FastTables[segId].Items[i];//返回换出页框对应的快表表项
}

//-1表示未命中，大于等于0为页框对应的快表的index
int Storage::HitBuffer(Addr addr)
{
    uint segId = FM_GetSegId(addr);
    uint i, len = FastTables[segId].length;
    for (i = 0; i < len; ++i)
    {
        if ((FastTables[segId].Items[i].PageEnt | INTERPAGE_ADDR) == (addr | INTERPAGE_ADDR))
        {
            break;
        }
    }
    if (i < len)
    {
        //命中
        return i;
    }
    //未命中
    return -1;
}

void Storage::PageToFrame(byte *filePage, Frame *frame)
{
    memcpy(frame->FilePage, filePage, PAGE_SIZE);
    memcpy(&frame->UsedBytes, filePage, sizeof(ushort));
    memcpy(&frame->USNum, filePage+sizeof(ushort), sizeof(ushort));
    uint i;
    for (i = 0; i < MAX_US_NUM; ++i)
    {
        memcpy(&frame->USTable[i], filePage+2*sizeof(ushort)+i*sizeof(UsedSpace), sizeof(UsedSpace));
    }
}

void Storage::FrameToPage(Frame *frame, byte *filePage)
{
    memcpy(filePage, frame->FilePage, PAGE_SIZE);
    memcpy(filePage, &frame->UsedBytes, sizeof(ushort));
    memcpy(filePage+sizeof(ushort), &frame->USNum, sizeof(ushort));
    uint i;
    for (i = 0; i < MAX_US_NUM; ++i)
    {
        memcpy(filePage+2*sizeof(ushort)+i*sizeof(UsedSpace), &frame->USTable[i], sizeof(UsedSpace));
    }
}

void Storage::Init()
{
    FM_LoadSegPage();
    memset(Buffer, 0, sizeof(Buffer));
    memset(FastTables, 0, sizeof(FastTables));
}

void Storage::Flush()
{
    uint i, j, len;
    for (i = 0; i < SEG_NUM; ++i)
    {
        len = FastTables[i].length;
        for (j = 0; j < len; ++j)
        {
            if (FastTables[i].Items[j].M == true)
            {
                if (Buffer[FastTables[i].Items[j].FrameId].USNum == 0)
                {
                    FM_FreePage(FastTables[i].Items[j].PageEnt);
                }
                else
                {
                    FrameToPage(&Buffer[FastTables[i].Items[j].FrameId],
                                Buffer[FastTables[i].Items[j].FrameId].FilePage);
                    FM_WritePage(FastTables[i].Items[j].PageEnt,
                                 Buffer[FastTables[i].Items[j].FrameId].FilePage);
                }
            }
        }
    }
    FM_FlushSegPage();
}

Addr Storage::Alloc(uint segId, uint length)
{
    uint len = FastTables[segId].length, i, j, k;
    Addr tmpAddr;
    PageAddr pageAddr;
    if (PAGE_SIZE < PAGE_HEAD_SIZE + length)
    {
        //不能分配超过一个页中最大可用空间长度的空间
        return -1;
    }
    Frame *tmpFrame;
    for (i = 0; i < len; ++i)
    {
        tmpFrame = &Buffer[FastTables[segId].Items[i].FrameId];
        if (PAGE_SIZE - PAGE_HEAD_SIZE - tmpFrame->UsedBytes >= length && tmpFrame->USNum < MAX_US_NUM)
        {
            tmpAddr = FastTables[segId].Items[i].PageEnt & ADDR_WOL12;
            pageAddr = PAGE_HEAD_SIZE;
            for (j = 0; j < tmpFrame->USNum; ++j)
            {
                if (tmpFrame->USTable[j].Pos - pageAddr >= (int)length)
                {
                    break;
                }
                pageAddr = tmpFrame->USTable[j].Pos + tmpFrame->USTable[j].Length;
            }
            if (j < tmpFrame->USNum)
            {
                for (k = tmpFrame->USNum-1; k >= j; --k)
                {
                    tmpFrame->USTable[k+1] = tmpFrame->USTable[k];
                }
                tmpFrame->USTable[j].Pos = pageAddr;
                tmpFrame->USTable[j].Length = length;
                tmpFrame->USNum++;
                tmpFrame->UsedBytes += length;
                tmpAddr += pageAddr;
                return tmpAddr;
            }
            else if (PAGE_SIZE - pageAddr >= length)
            {
                tmpFrame->USTable[tmpFrame->USNum].Pos = pageAddr;
                tmpFrame->USTable[tmpFrame->USNum].Length = length;
                tmpFrame->USNum++;
                tmpFrame->UsedBytes += length;
                tmpAddr += pageAddr;
                return tmpAddr;
            }
        }
    }
    FastTabItem *item= ClockAlg(segId);
    item->PageEnt = FM_AllocPage(segId);
    memset(&Buffer[item->FrameId], 0, sizeof(Frame));
    Buffer[item->FrameId].Valid = true;
    Buffer[item->FrameId].UsedBytes = length;
    Buffer[item->FrameId].USNum = 1;
    Buffer[item->FrameId].USTable[0].Length = length;
    Buffer[item->FrameId].USTable[0].Pos = PAGE_HEAD_SIZE;
    tmpAddr = item->PageEnt & ADDR_WOL12;
    tmpAddr += PAGE_HEAD_SIZE;
    return tmpAddr;
}

int Storage::Free(Addr pageEnt, uint length)
{
    uint segId = FM_GetSegId(pageEnt);
    int i;
    uint j, usNum, frameId;
    i = HitBuffer(pageEnt);
    FastTabItem *item;
    if (i >= 0)
    {

        frameId = FastTables[segId].Items[i].FrameId;
        usNum = Buffer[frameId].USNum;
        for (j = 0; j < usNum; ++j)
        {
            if ((pageEnt & INTERPAGE_ADDR) == Buffer[frameId].USTable[j].Pos)
            {
                break;
            }
        }
        if (j < usNum)
        {
            for (; j < usNum-1; ++j)
            {
                Buffer[frameId].USTable[j] = Buffer[frameId].USTable[j+1];
            }
            Buffer[frameId].USNum--;
            Buffer[frameId].UsedBytes -= length;
            FastTables[segId].Items[i].A = true;
            FastTables[segId].Items[i].M = true;
            return length;
        }
        return -1;
    }
    else
    {
        //要释放的空间不在缓冲区中
        item = ClockAlg(segId);
        item->PageEnt = pageEnt & ADDR_WOL12;
        FM_ReadPage(pageEnt, Buffer[item->FrameId].FilePage);
        PageToFrame(Buffer[item->FrameId].FilePage, &Buffer[item->FrameId]);
        return this->Free(pageEnt, length);
    }
}

int Storage::Read(Addr pageEnt, void *buf, uint length)
{
    uint segId = FM_GetSegId(pageEnt);
    int i = HitBuffer(pageEnt);
    uint frameId;
    FastTabItem *item = NULL;
    if (i >= 0)
    {
        frameId = FastTables[segId].Items[i].FrameId;
        memcpy(buf, Buffer[frameId].FilePage + (pageEnt&INTERPAGE_ADDR), length);
        FastTables[FM_GetSegId(pageEnt)].Items[i].A = true;
        return length;
    }
    else
    {
        //没命中
        item = ClockAlg(segId);
        item->PageEnt = pageEnt & ADDR_WOL12;
        FM_ReadPage(pageEnt, Buffer[item->FrameId].FilePage);
        PageToFrame(Buffer[item->FrameId].FilePage, &Buffer[item->FrameId]);
        return this->Read(pageEnt, buf, length);
    }
}

int Storage::Write(Addr pageEnt, const void *buf, uint length)
{
    uint segId = FM_GetSegId(pageEnt);
    int i = HitBuffer(pageEnt);
    FastTabItem *item = NULL;
    if (i >= 0)
    {
        //将buf的内容写入缓冲区
        memcpy(Buffer[FastTables[segId].Items[i].FrameId].FilePage + (pageEnt&INTERPAGE_ADDR), buf, length);
        FastTables[FM_GetSegId(pageEnt)].Items[i].A = true;
        FastTables[FM_GetSegId(pageEnt)].Items[i].M = true;
        return length;
    }
    else
    {
        //缓冲区已满
        item = ClockAlg(segId);
        item->PageEnt = pageEnt & ADDR_WOL12;
        FM_ReadPage(pageEnt, Buffer[item->FrameId].FilePage);
        PageToFrame(Buffer[item->FrameId].FilePage, &Buffer[item->FrameId]);
        return this->Write(pageEnt, buf, length);
    }
}

/**
file_manager.c
*/

void Storage::OpenFileStream()
{
    if (Meta == NULL)
    {
        Meta = fopen(this->MetaPath, "rb+");
        setbuf(Meta, NULL);
    }
    if (Data == NULL)
    {
        Data = fopen(this->DataPath, "rb+");
        setbuf(Data, NULL);
    }
}

void Storage::CloseFileStream()
{
    if (Meta != NULL)
    {
        fclose(Meta);
        Meta = NULL;
    }
    if (Data != NULL)
    {
        fclose(Data);
        Data = NULL;
    }
}

int Storage::ReadFrmFile(char f, uint offset, void *buf, size_t nbytes)
{
    int i;
    FILE *fp = NULL;
    if (f ==  'm')
    {
        fp = Meta;
    }
    else
    {
        fp=Data;
    }
    fseek(fp, offset, SEEK_SET);
    i = fread(buf, nbytes, 1, fp);
    return i;
}

int Storage::WriteToFile(char f, uint offset, const void *buf, size_t nbytes)
{
    int i;
    FILE *fp = NULL;
    if (f == 'm')
    {
        fp = Meta;
    }
    else
    {
        fp = Data;
    }
    fseek(fp, offset, SEEK_SET);
    i = fwrite(buf, nbytes, 1, fp);
    return i;
}

inline uint Storage::FM_GetSegId(Addr addr)
{
    return (addr >> 32);
}

inline uint Storage::FM_GetPageId(Addr addr)
{
    return (addr >> 12) & 0xFFFFF;
}
char ppp[PAGE_SIZE];
Addr Storage::FM_AllocPage(uint segId)
{
    uint i, j;
    Addr tmpAddr;
    for (i = 0; i < SegPage[segId].PageTableLength; ++i)
    {
        if (SegPage[segId].PageTable[i].Used == false)
        {
            SegPage[segId].PageTable[i].Used = true;
            break;
        }
    }
    if (i < SegPage[segId].PageTableLength)
    {
        if (SegPage[segId].PageTable[i].PageEnt == -1)
        {
            SegPage[segId].PageTable[i].PageEnt = p;
            p += PAGE_SIZE;
        }
    }
    else if (i <= MAX_PAGE_TAB_LEN - INIT_PAGE_TAB_LEN)
    {
        SegPage[segId].PageTable = (PageTabItem *)realloc(SegPage[segId].PageTable,
                                                          (SegPage[segId].PageTableLength+INIT_PAGE_TAB_LEN)*sizeof(PageTabItem));
        SegPage[segId].PageTableLength += INIT_PAGE_TAB_LEN;
        SegPage[segId].PageTable[i].PageEnt = p;
        p += PAGE_SIZE;
        for (j = i+1; j < SegPage[segId].PageTableLength; ++j)
        {
            SegPage[segId].PageTable[j].Used = false;
            SegPage[segId].PageTable[j].PageEnt = -1;
        }

    }
    else
    {
        return -1;
    }
    tmpAddr = segId;
    tmpAddr <<= 32;
    tmpAddr += (i<<12);
    return tmpAddr;
}

int Storage::FM_FreePage(Addr pageEnt)
{
    if (pageEnt < 0)
    {
        return -1;
    }
    SegPage[FM_GetSegId(pageEnt)].PageTable[FM_GetPageId(pageEnt)].Used = false;
    return 0;
}

int Storage::FM_ReadPage(Addr pageEnt, void *buf)
{
    if (pageEnt < 0)
    {
        return -1;
    }
    if (SegPage[FM_GetSegId(pageEnt)].PageTable[FM_GetPageId(pageEnt)].Used == false)
    {
        return -2;
    }
    return ReadFrmFile('d', SegPage[FM_GetSegId(pageEnt)].PageTable[FM_GetPageId(pageEnt)].PageEnt, buf, PAGE_SIZE);
}

int Storage::FM_WritePage(Addr pageEnt, const void *buf)
{
    if (pageEnt < 0)
    {
        return -1;
    }
    if (SegPage[FM_GetSegId(pageEnt)].PageTable[FM_GetPageId(pageEnt)].Used == false)
    {
        return -2;
    }
    return WriteToFile('d', SegPage[FM_GetSegId(pageEnt)].PageTable[FM_GetPageId(pageEnt)].PageEnt, buf, PAGE_SIZE);
}

void Storage::FM_FlushSegPage()
{
    CloseFileStream();
    OpenFileStream();
    WriteToFile('m', META_HEAD_SIZE, SegPage, sizeof(SegPage));
    uint i, pos = META_HEAD_SIZE + sizeof(SegPage);
    for (i = 0; i < SEG_NUM; ++i)
    {
        WriteToFile('m', pos, SegPage[i].PageTable, sizeof(PageTabItem)*SegPage[i].PageTableLength);
        pos += sizeof(PageTabItem)*SegPage[i].PageTableLength;
    }
    WriteToFile('m', pos, &p, sizeof(Addr));
}

void Storage::Create()
{
    memset(SegPage, 0, sizeof(SegPage));
    memset(Buffer, 0, sizeof(Buffer));
    memset(FastTables, 0, sizeof(FastTables));
    uint i;
    for (i = 0; i < SEG_NUM; ++i)
    {
        SegPage[i].PageTable = new PageTabItem[INIT_PAGE_TAB_LEN];
        SegPage[i].PageTableLength = INIT_PAGE_TAB_LEN;
        SegPage[i].Used = true;
    }
    FILE *meta = fopen(this->MetaPath, "wb");
    FILE *data = fopen(this->DataPath, "wb");
    fseek(meta, META_HEAD_SIZE, SEEK_SET);
    fclose(meta);
    fclose(data);
    CloseFileStream();
    OpenFileStream();
}

void Storage::FM_LoadSegPage()
{
    CloseFileStream();
    OpenFileStream();
    uint i;
    for (i = 0; i < SEG_NUM; ++i)
    {
        if (SegPage[i].PageTable != NULL)
        {
            delete []SegPage[i].PageTable;
        }
    }
    ReadFrmFile('m', META_HEAD_SIZE, SegPage, sizeof(SegPage));
    uint pos = META_HEAD_SIZE + sizeof(SegPage);
    for (i = 0; i < SEG_NUM; ++i)
    {
        SegPage[i].Used = true;
        SegPage[i].PageTable = new PageTabItem[SegPage[i].PageTableLength];
        ReadFrmFile('m', pos, SegPage[i].PageTable, sizeof(PageTabItem)*SegPage[i].PageTableLength);
        pos += sizeof(PageTabItem)*SegPage[i].PageTableLength;
    }
    ReadFrmFile('m', pos, &p, sizeof(Addr));
}

int Storage::ReadDataPage(uint pageId, void *page)
{
    if (pageId < SegPage[0].PageTableLength)
    {
        if (SegPage[0].PageTable[pageId].Used == true)
        {
            ReadFrmFile('d', SegPage[0].PageTable[pageId].PageEnt, page, PAGE_SIZE);
            return PAGE_SIZE;
        }
        return 0;
    }
    return -1;
}
