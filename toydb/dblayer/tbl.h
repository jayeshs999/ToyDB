#ifndef _TBL_H_
#define _TBL_H_
#include <stdbool.h>

#define VARCHAR 1
#define INT     2
#define LONG    3
#define DBE_OK  0 

typedef char byte;

typedef struct {
    char *name;
    int  type;  // one of VARCHAR, INT, LONG
} ColumnDesc;

typedef struct {
    int numColumns;
    ColumnDesc **columns; // array of column descriptors
} Schema;

typedef struct{
    int pageNum;
    int dirty;
    struct Pagelist* next;
} Pagelist;

typedef struct{
    int* numRecords;
    char** freeSpace;
    short* offsets;
} PageBuffer;

typedef struct {
    Schema *schema;

    int fd;
    struct PageList* openPage;
    int lastPage;
    char* lastPageBuf;
    int lastPageDirty;
    
} Table ;

typedef int RecId;

int
Table_Open(char *fname, Schema *schema, bool overwrite, Table **table);

int
Table_Insert(Table *t, byte *record, int len, RecId *rid);

int
Table_Get(Table *t, RecId rid, byte *record, int maxlen);

void
Table_Close(Table *);

typedef void (*ReadFunc)(void *callbackObj, RecId rid, byte *row, int len);

void
Table_Scan(Table *tbl, void *callbackObj, ReadFunc callbackfn);

#endif
