
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "tbl.h"
#include "codec.h"
#include "../pflayer/pf.h"

#define SLOT_COUNT_OFFSET 2
#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(EXIT_FAILURE);}}
#define reterr(err) {if(err<0) {PF_PrintError(); return err;}}

// int  getLen(int slot, byte *pageBuf); UNIMPLEMENTED;
// int  getNumSlots(byte *pageBuf); UNIMPLEMENTED;
// void setNumSlots(byte *pageBuf, int nslots); UNIMPLEMENTED;
// int  getNthSlotOffset(int slot, char* pageBuf); UNIMPLEMENTED;

void parsePageBuf(char* pageBuf,PageBuffer* parsedPageBuffer){
    parsedPageBuffer->numRecords = pageBuf;
    parsedPageBuffer->freeSpace = pageBuf + 4;
    parsedPageBuffer->offsets = pageBuf + 8;
}

int isFree(char* pageBuf,int len){
    PageBuffer parsedPageBuffer;
    parsePageBuf(pageBuf,&parsedPageBuffer); 
    int numRecords = *(parsedPageBuffer.numRecords);
    int start = (parsedPageBuffer.freeSpace - pageBuf);
    int end = parsedPageBuffer.offsets[numRecords];
    return (end - start) >= (len+2);  
}

int allocPageIfNeeded(Table* table,int len){
    if(table->lastPage == -1){
        reterr(PF_AllocPage(table->fd,&table->lastPage,&table->lastPageBuf));
    }
    else if(!isFree(table->lastPageBuf,len)){
        reterr(PF_UnfixPage(table->fd,table->lastPage,table->lastPageDirty));
        reterr(PF_AllocPage(table->fd,&table->lastPage,&table->lastPageBuf));
    }
    else{
        return DBE_OK;
    }

    PageBuffer parsedPageBuffer;
    parsePageBuf(table->lastPageBuf,&parsedPageBuffer);

    *parsedPageBuffer.numRecords = 0;
    *parsedPageBuffer.freeSpace = table->lastPageBuf + 10;
    parsedPageBuffer.offsets[0] = PF_PAGE_SIZE;

    table->lastPageDirty = 1;
}

int getLastPage(int fd,int* pageNum,char **pageBuf){
    // Get last page
    int err = PF_GetFirstPage(fd,pageNum,pageBuf);
    if(err == PFE_EOF){
        *pageNum = -1;
        *pageBuf = NULL;
        return DBE_OK;
    }
    reterr(err);
    
    while(1){
        int tempNum = *pageNum;
        err = PF_GetNextPage(fd,pageNum,pageBuf);
        if(err == PFE_OK){
            reterr(PF_UnfixPage(fd,tempNum,0));
        }
        if(err == PFE_EOF){
            break;
        }
        reterr(err);
    }

    return DBE_OK;
}
/**
   Opens a paged file, creating one if it doesn't exist, and optionally
   overwriting it.
   Returns 0 on success and a negative error code otherwise.
   If successful, it returns an initialized Table*.
 */
int
Table_Open(char *dbname, Schema *schema, bool overwrite, Table **ptable)
{
    // Initialize PF, create PF file,
    // allocate Table structure  and initialize and return via ptable
    // The Table structure only stores the schema. The current functionality
    // does not really need the schema, because we are only concentrating
    // on record storage. 

    int err;
    int fd;

    Table* table = malloc(sizeof(Table));

    PF_Init();
    if(overwrite){
        err = PF_DestroyFile(dbname);
        if(err == PFE_FILEOPEN)
            reterr(err);
        
        err = PF_CreateFile(dbname);
        reterr(err);

    }
    else{
        PF_CreateFile(dbname);
    }

    fd = PF_OpenFile(dbname);
    reterr(fd);

    reterr(getLastPage(table->fd,&table->lastPage,&table->lastPageBuf));
    table->lastPageDirty = 0;    

    table->schema = malloc(sizeof(Schema));
    memcpy(table->schema,schema,sizeof(Schema));
    table->fd = fd;

    *ptable = table;
    return DBE_OK;
}

void
Table_Close(Table *tbl) {
    Pagelist *openPage = tbl->openPage;
    while(openPage != NULL){
        checkerr(PF_UnfixPage(tbl->fd,openPage->pageNum, openPage->dirty));
        openPage = openPage->next;
    }
    if(tbl->lastPage != -1){
        checkerr(PF_UnfixPage(tbl->fd,tbl->lastPage,tbl->lastPageDirty));
    }
    checkerr(PF_CloseFile(tbl->fd));
    free(tbl);
    // Unfix any dirty pages, close file.
}


int
Table_Insert(Table *tbl, byte *record, int len, RecId *rid) {
    // Allocate a fresh page if len is not enough for remaining space
    // Get the next free slot on page, and copy record in the free
    // space
    // Update slot and free space index information on top of page.
    reterr(allocPageIfNeeded(tbl,len));
    PageBuffer parsedPageBuffer;
    parsePageBuf(tbl->lastPageBuf,&parsedPageBuffer);
    short recordOffset = parsedPageBuffer.offsets[*parsedPageBuffer.numRecords] - len;
    (*parsedPageBuffer.numRecords)++;
    parsedPageBuffer.offsets[*parsedPageBuffer.numRecords] = recordOffset;
    *parsedPageBuffer.freeSpace += 2;
    memcpy(tbl->lastPageBuf+recordOffset,record,len); 
    
    *rid = tbl->lastPage << 16 | (*parsedPageBuffer.numRecords-1);
    return DBE_OK;
    
}

#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(EXIT_FAILURE);}}

/*
  Given an rid, fill in the record (but at most maxlen bytes).
  Returns the number of bytes copied.
 */
int
Table_Get(Table *tbl, RecId rid, byte *record, int maxlen) {
    int slot = rid & 0xFFFF;
    int pageNum = rid >> 16;

    char* pageBuf;
    if(pageNum != tbl->lastPage){
        reterr(PF_GetThisPage(tbl->fd,pageNum,&pageBuf));
    }
    else{
        pageBuf = tbl->lastPageBuf;
    }
    PageBuffer parsedPageBuffer;
    parsePageBuf(pageBuf,&parsedPageBuffer);
    if(slot > *parsedPageBuffer.numRecords || slot < 0){
        return -1;
    }
    short recordOffset = parsedPageBuffer.offsets[slot+1];
    int len = parsedPageBuffer.offsets[slot] - recordOffset;
    len = len > maxlen ? maxlen : len;
    memcpy(record,pageBuf+recordOffset,len);
    if(pageNum != tbl->lastPage){
        reterr(PF_UnfixPage(tbl->fd,pageNum,0));
    }
    return len; // return size of record
}

void
Table_Scan(Table *tbl, void *callbackObj, ReadFunc callbackfn) {

    
    int pageNum;
    char* pageBuf;

    int err = PF_GetFirstPage(tbl->fd,&pageNum,&pageBuf);

    if(err == PFE_EOF){
        return;
    }

    do{
        checkerr(err);
        PageBuffer parsedPageBuffer;
        parsePageBuf(pageBuf,&parsedPageBuffer);
        for(int i = 0;i < *parsedPageBuffer.numRecords;i++){
            RecId rid = pageNum << 16 | i;
            short recordOffset = parsedPageBuffer.offsets[i+1];
            int recordLen = parsedPageBuffer.offsets[i] - recordOffset;
            byte* record = pageBuf + recordOffset;
            callbackfn(callbackObj,rid,record,recordLen);
        }
        if(pageNum != tbl->lastPage){
            checkerr(PF_UnfixPage(tbl->fd,pageNum,0));
        }
    }
    while((err = PF_GetNextPage(tbl->fd,&pageNum,&pageBuf)) != PFE_EOF);

}


