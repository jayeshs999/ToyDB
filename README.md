# CS 387 Assignment 5

## Description of the code

### tbl.h
We created the struct **Table** as follows

```c
typedef struct {
    Schema *schema; // Store the table schema
    int fd; // File descriptor for the open table
    int lastPage; // Last page of the table, -1 if no page
    char* lastPageBuf; // Pointer to the buffer of the last[age in memory
    int lastPageDirty; // Boolean value specifying dirty status of the last page
} Table ;
```
This stores information corresponding to the currently open table like file descriptor, last page number, buffer of the last page.

We also defined some macros to use while handling error codes

```c++
#define DBE_OK  0
 
#define checkerr(err) {if (err < 0) {char info[50];sprintf(info,"%s:%d: Error in %s ",__FILE__,__LINE__,__func__);PF_PrintError(info); exit(EXIT_FAILURE);}}

#define reterr(err) {if(err<0) {char info[50];sprintf(info,"%s:%d: Error in %s\n\t",__FILE__,__LINE__,__func__);PF_PrintError(info); return err;}}

#define checkAMerr(err) {if (err < 0) {char info[50];sprintf(info,"%s:%d: Error in %s\n\t",__FILE__,__LINE__,__func__);AM_PrintError(info); exit(EXIT_FAILURE);}}
```

### tbl.c
We implemented the helper functions given and also added some new ones

```c
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
```

The function ```parsePageBuf``` parses the buffer metadata while the other functions get/set parts of the metadata.

Some other helper functions which were implemented - 

```c
/*
   Checks if the page buffer has enough space to store a
   record of length len.
*/
int isFree(char *pageBuf, int len);

/*
  Allocates and initializes a new page if there is no page
  in the table or there is not enough space in the last page
*/
int allocPageIfNeeded(Table *table, int len);

/*
    It gets the last page of file into memory and sets pageBuf to point to it.
    It also sets the value in pageNum to the page number of the last page.
*/
int getLastPage(int fd, int *pageNum, char **pageBuf);
```

In the function ```Table_Open```, we open the file with the given name, creating a new one if it doesn't exist. Then we initialize other parts of the variable ```Table table``` and assign ```ptable``` to point to ```table```.

In the function ```Table_Close```, we close the open pages, close the file and also free the memory assigned to ```Table* tbl```.

In the function ```Table_Insert```, we first check whether the last page has enough space to hold the record, allocating a new page if needed. We then update the metadata and copy the record contents into the page at a particular offset. The record ID is stored into the variable ```RecID *rid```.

In the function ```Table_Get```, we get the page corresponding to the record into memory if it isn't there. Then we copy atmost ```maxlen``` bytes from the record into the record buffer. Then, the opened page is unfixed if it isn't the last page.

In the function ```Table_Scan```, we iterate over all pages using the functions ```PF_GetFirstPage``` and ```PF_GetNextPage``` until the error code ```PF_EOF``` is returned. For each record in each page, we call the function ```callbackfn``` with the required arguments.

### loaddb.c
First, we read the CSV file and parse the first line into the ```Schema *sch```.
We then open the table using ```Table_Open```, create an integer indexing using ```AM_CreateIndex``` and open the file used to store the indexes (used in AM layer to create B+ tree).
We iterate over the CSV file row-by-row, encode the row into a byte array ```record``` and insert the record into the table using the function ```Table_Insert```. Additionally, we insert the corresponding entry in the index using the function```AM_InsertEntry```. 
Finally, we close all the open files and the open table.

### dumpdb.c
The ```printRow``` function decodes the row according to the type of entry and prints the comma separated values.
The ```index_scan``` function initializes the scan descriptor based on the operation and iterates over the record IDs returned by the AM layer, obtains the corresponding records using the function ```Table_Get``` and prints them.

## Contributors

### Jayesh Singla - 190050053
Implemented the functions in tbl.c and contributed to debugging.

### Shrey Singla - 190050114
Implemented loaddb.c and dumpdb.c and contributed to debugging.

## References

We only referred to the files ```am.pdf``` and ```pf.pdf``` and the corresponding code to understand the working of the API.