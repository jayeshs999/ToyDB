#include <stdio.h>
#include <stdlib.h>
#include "codec.h"
#include "tbl.h"
#include "util.h"
#include "../pflayer/pf.h"
#include "../amlayer/am.h"
#define checkerr(err) {if (err < 0) {PF_PrintError(""); exit(1);}}


void
printRow(void *callbackObj, RecId rid, byte *row, int len) {
    Schema *schema = (Schema *) callbackObj;
    byte *cursor = row;
    
    // UNIMPLEMENTED;
    // char res[MAX_LINE_LEN];
    // int pos = 0;
    for (int i=0; i<schema->numColumns; i++){
        ColumnDesc *col = schema->columns[i];
        if (col->type == VARCHAR){
            char res[MAX_LINE_LEN];
            int len = DecodeCString(cursor, res, MAX_LINE_LEN);
            // pos += len;
            // cursor += len;
            // res[pos] = ',';
            // pos+=1;
            cursor += (len + 2);
            printf("%s,", res);
        }
        else if (col->type == INT){
            int dec = DecodeInt(cursor);
            cursor+=4;
            printf("%d,", dec);
        }
        else{
            long long dec = DecodeLong(cursor);
            cursor+=8;
            printf("%lld,", dec);
        }
    }
    printf("\n");
}

#define DB_NAME "data.db"
#define INDEX_NAME "data.db.0"
	 
void
index_scan(Table *tbl, Schema *schema, int indexFD, int op, int value) {
    // UNIMPLEMENTED;
    int scanDesc = AM_OpenIndexScan(indexFD, 'i', 4, op, (char *)&value);

    while (true){
        int recID = AM_FindNextEntry(scanDesc);
        if (recID == AME_EOF) break;
        byte * record;
        int len = Table_Get(tbl, recID, record, MAX_LINE_LEN);
        printRow((void *)schema, recID, record, len);
    }

    int err = AM_CloseIndexScan(scanDesc);
    
    
    /*
    Open index ...
    while (true) {
	find next entry in index
	fetch rid from table
        printRow(...)
    }
    close index ...
    */
}

int
main(int argc, char **argv) {
    char *schemaTxt = "Country:varchar,Capital:varchar,Population:int";
    Schema *schema = parseSchema(schemaTxt);
    Table *tbl;

    // UNIMPLEMENTED;
    int err;
    err = Table_Open(DB_NAME, schema, 0, &tbl);

    if (argc == 2 && *(argv[1]) == 's') {
	// UNIMPLEMENTED;
    
    Table_Scan(tbl, (void *)schema, printRow);

	// invoke Table_Scan with printRow, which will be invoked for each row in the table.
    } else {
	// index scan by default
	int indexFD = PF_OpenFile(INDEX_NAME);
	checkerr(indexFD);

	// Ask for populations less than 100000, then more than 100000. Together they should
	// yield the complete database.
	index_scan(tbl, schema, indexFD, LESS_THAN_EQUAL, 100000);
	index_scan(tbl, schema, indexFD, GREATER_THAN, 100000);
    }
    Table_Close(tbl);
}
