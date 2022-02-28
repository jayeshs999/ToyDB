
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "tbl.h"
#include "codec.h"
#include "../pflayer/pf.h"

#define SLOT_COUNT_OFFSET 2

/*
   Parses a page buffer and stores the metadata information
   in the PageBuffer struct
*/
void parsePageBuf(char *pageBuf, PageBuffer *parsedPageBuffer)
{
    parsedPageBuffer->numRecords = pageBuf;    // Pointer to the location where number of records are stored
    parsedPageBuffer->freeSpace = pageBuf + 4; // Pointer to the location where free space starts
    parsedPageBuffer->offsets = pageBuf + 8;   // Pointer to the location where offsets array starts
}

/*
   Helper functions
*/
int getLen(int slot, byte *pageBuf)
{
    PageBuffer parsedPageBuffer;
    parsePageBuf(pageBuf, &parsedPageBuffer); // Parse the buffer
    return parsedPageBuffer.offsets[slot] - parsedPageBuffer.offsets[slot + 1];
}
int getNumSlots(byte *pageBuf)
{
    PageBuffer parsedPageBuffer;
    parsePageBuf(pageBuf, &parsedPageBuffer); // Parse the buffer
    return *parsedPageBuffer.numRecords;
}
void setNumSlots(byte *pageBuf, int nslots)
{
    PageBuffer parsedPageBuffer;
    parsePageBuf(pageBuf, &parsedPageBuffer); // Parse the buffer
    *parsedPageBuffer.numRecords = nslots;
}

int getNthSlotOffset(int slot, char *pageBuf)
{
    PageBuffer parsedPageBuffer;
    parsePageBuf(pageBuf, &parsedPageBuffer); // Parse the buffer
    return parsedPageBuffer.offsets[slot + 1];
}

void setNthSlotOffset(int slot, short offset, char *pageBuf)
{
    PageBuffer parsedPageBuffer;
    parsePageBuf(pageBuf, &parsedPageBuffer); // Parse the buffer
    parsedPageBuffer.offsets[slot + 1] = offset;
}

char *getFreeSpace(char *pageBuf)
{
    PageBuffer parsedPageBuffer;
    parsePageBuf(pageBuf, &parsedPageBuffer); // Parse the buffer
    return *parsedPageBuffer.freeSpace;
}

void setFreeSpace(char *pageBuf, char *pointer)
{
    PageBuffer parsedPageBuffer;
    parsePageBuf(pageBuf, &parsedPageBuffer); // Parse the buffer
    *parsedPageBuffer.freeSpace = pointer;
}

/*
   Checks if the page buffer has enough space to store a
   record of length len.
*/
int isFree(char *pageBuf, int len)
{
    int numRecords = getNumSlots(pageBuf);
    int start = (getFreeSpace(pageBuf) - pageBuf);       // Offset of free space
    int end = getNthSlotOffset(numRecords - 1, pageBuf); // Offset of last record
    return (end - start) >= (len + 2);                   // Check if free space is enough to store the record and its offset
}

/*
  Allocates and initializes a new page if there is no page
  in the table or there is not enough space in the last page
*/
int allocPageIfNeeded(Table *table, int len)
{
    if (table->lastPage == -1)
    {
        // Allocate a new page if there is no page in the table
        reterr(PF_AllocPage(table->fd, &table->lastPage, &table->lastPageBuf));
    }
    else if (!isFree(table->lastPageBuf, len)) // Check if the last page has enough space
    {
        // Unfix the last page
        reterr(PF_UnfixPage(table->fd, table->lastPage, table->lastPageDirty));
        // Allocate a new last page
        reterr(PF_AllocPage(table->fd, &table->lastPage, &table->lastPageBuf));
    }
    else
    {
        return DBE_OK;
    }

    PageBuffer parsedPageBuffer;
    parsePageBuf(table->lastPageBuf, &parsedPageBuffer); // Parse the buffer

    // Initialize the metadata values
    *parsedPageBuffer.numRecords = 0;
    *parsedPageBuffer.freeSpace = table->lastPageBuf + 10;
    parsedPageBuffer.offsets[0] = PF_PAGE_SIZE;

    table->lastPageDirty = 1; // Set the last page to be dirty
    return DBE_OK;
}

/*
    It gets the last page of file into memory and sets pageBuf to point to it.
    It also sets the value in pageNum to the page number of the last page.
*/
int getLastPage(int fd, int *pageNum, char **pageBuf)
{
    // Get first page
    int err = PF_GetFirstPage(fd, pageNum, pageBuf);

    // Return if there is no last page
    if (err == PFE_EOF)
    {
        *pageNum = -1;
        *pageBuf = NULL;
        return DBE_OK;
    }
    reterr(err);

    while (1)
    {
        int tempNum = *pageNum;
        // Get the next page
        err = PF_GetNextPage(fd, pageNum, pageBuf);

        // Close the previous page if there is a next page
        if (err == PFE_OK)
        {
            reterr(PF_UnfixPage(fd, tempNum, 0));
        }

        // End of table reached
        if (err == PFE_EOF)
        {
            break;
        }
        reterr(err);
    }

    return DBE_OK;
}

/*
   Opens a paged file, creating one if it doesn't exist, and optionally
   overwriting it.
   Returns 0 on success and a negative error code otherwise.
   If successful, it returns an initialized Table*.
*/
int Table_Open(char *dbname, Schema *schema, bool overwrite, Table **ptable)
{
    int err;
    int fd;

    Table *table = malloc(sizeof(Table)); // Allocate space for a new table struct

    if (overwrite)
    {
        err = PF_DestroyFile(dbname); // Destory the old table
        if (err == PFE_FILEOPEN)
            reterr(err);

        err = PF_CreateFile(dbname); // Create a new file
        reterr(err);
    }
    else
    {
        PF_CreateFile(dbname); // Create a new file
    }

    fd = PF_OpenFile(dbname); // Open the table file
    reterr(fd);

    // Open the last page of the table
    reterr(getLastPage(table->fd, &table->lastPage, &table->lastPageBuf));
    table->lastPageDirty = 0;

    // Copy the schema into the table struct
    table->schema = malloc(sizeof(Schema));
    memcpy(table->schema, schema, sizeof(Schema));

    // Set the file descriptor in the table struct
    table->fd = fd;

    // Set ptable to point to the table struct
    *ptable = table;
    return DBE_OK;
}

void Table_Close(Table *tbl)
{
    // Unfix the last page of the table if it exists
    if (tbl->lastPage != -1)
    {
        checkerr(PF_UnfixPage(tbl->fd, tbl->lastPage, tbl->lastPageDirty));
    }

    // Close the opened file
    checkerr(PF_CloseFile(tbl->fd));

    // Free the table struct
    free(tbl);
}

int Table_Insert(Table *tbl, byte *record, int len, RecId *rid)
{
    // Allocate a fresh page if len is not enough for remaining space
    // Get the next free slot on page, and copy record in the free space
    // Update slot and free space index information on top of page.

    // Allocate a new page if needed according to record length
    reterr(allocPageIfNeeded(tbl, len));

    // Update metadata
    int numRecords = getNumSlots(tbl->lastPageBuf);
    short recordOffset = getNthSlotOffset(numRecords - 1, tbl->lastPageBuf) - len;
    numRecords++;
    setNumSlots(tbl->lastPageBuf, numRecords);
    setNthSlotOffset(numRecords - 1, recordOffset, tbl->lastPageBuf);
    setFreeSpace(tbl->lastPageBuf, getFreeSpace(tbl->lastPageBuf) + 2);

    // Copy the record into the buffer and set it to dirty
    memcpy(tbl->lastPageBuf + recordOffset, record, len);
    tbl->lastPageDirty = 1;

    // Store the new record's id into rid
    *rid = tbl->lastPage << 16 | (numRecords - 1);
    return DBE_OK;
}

/*
  Given an rid, fill in the record (but at most maxlen bytes).
  Returns the number of bytes copied.
*/
int Table_Get(Table *tbl, RecId rid, byte *record, int maxlen)
{
    int slot = rid & 0xFFFF;
    int pageNum = rid >> 16;

    char *pageBuf;

    // Open the page if it isn't already open i.e. if it isn't the last page
    if (pageNum != tbl->lastPage)
    {
        reterr(PF_GetThisPage(tbl->fd, pageNum, &pageBuf));
    }
    else
    {
        pageBuf = tbl->lastPageBuf;
    }

    // Check if slot number is valid
    if (slot >= getNumSlots(pageBuf) || slot < 0)
    {
        return -1;
    }

    // Get the record offset and length
    short recordOffset = getNthSlotOffset(slot, pageBuf);
    int len = getLen(slot, pageBuf);
    len = len > maxlen ? maxlen : len;

    // Copy the record into the record buffer
    memcpy(record, pageBuf + recordOffset, len);

    // Unfix the page if it isn't the last page
    if (pageNum != tbl->lastPage)
    {
        reterr(PF_UnfixPage(tbl->fd, pageNum, 0));
    }
    return len; // return size of record
}

void Table_Scan(Table *tbl, void *callbackObj, ReadFunc callbackfn)
{

    int pageNum;
    char *pageBuf;

    // Unfix the last page if it exists
    if (tbl->lastPage != -1)
    {
        checkerr(PF_UnfixPage(tbl->fd, tbl->lastPage, tbl->lastPageDirty));
    }

    // Get the first page
    int err = PF_GetFirstPage(tbl->fd, &pageNum, &pageBuf);

    if (err == PFE_EOF)
    {
        return; // Return if enf of file reached
    }

    // Iterate over the pages till end of file is not reached
    do
    {
        checkerr(err);

        int numRecords = getNumSlots(pageBuf); // Get the number of records
        for (int i = 0; i < numRecords; i++)
        {
            RecId rid = pageNum << 16 | i; // Calculate record id

            // Get the record offset and length
            short recordOffset = getNthSlotOffset(i, pageBuf);
            int recordLen = getLen(i, pageBuf);

            byte *record = pageBuf + recordOffset;

            // Call the callback function
            callbackfn(callbackObj, rid, record, recordLen);
        }
        checkerr(PF_UnfixPage(tbl->fd, pageNum, 0));
    } while ((err = PF_GetNextPage(tbl->fd, &pageNum, &pageBuf)) != PFE_EOF);

    // Reopen the last page if it exists
    if (tbl->lastPage != -1)
    {
        checkerr(PF_GetThisPage(tbl->fd, tbl->lastPage, &tbl->lastPageBuf));
        tbl->lastPageDirty = 0;
    }
}
