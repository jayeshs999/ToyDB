#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <ctype.h>
#include "codec.h"
#include "../pflayer/pf.h"
#include "../amlayer/am.h"
#include "tbl.h"
#include "util.h"

#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(1);}}

#define MAX_PAGE_SIZE 4000


#define DB_NAME "data.db"
#define INDEX_NAME "data.db.0"
#define CSV_NAME "data.csv"


/*
Takes a schema, and an array of strings (fields), and uses the functionality
in codec.c to convert strings into compact binary representations
 */
int
encode(Schema *sch, char **fields, byte *record, int spaceLeft) {
    // UNIMPLEMENTED;
    int total = 0;
    for (int i=0; i<sch->numColumns;i++){
        ColumnDesc *col = sch->columns[i];
        if (col->type == VARCHAR){
            total += EncodeCString(fields[i], record + total, MAX_LINE_LEN);
        }
        else if (col->type == INT){
            total += EncodeInt(atoi(fields[i]), record + total);
        }
        else{
            total += EncodeLong(strtol(fields[i],NULL,10), record + total);
        }
    }
    return total;
    // for each field
    //    switch corresponding schema type is
    //        VARCHAR : EncodeCString
    //        INT : EncodeInt
    //        LONG: EncodeLong
    // return the total number of bytes encoded into record
}

Schema *
loadCSV() {
    // Open csv file, parse schema
    FILE *fp = fopen(CSV_NAME, "r");
    if (!fp) {
	perror("data.csv could not be opened");
        exit(EXIT_FAILURE);
    }

    char buf[MAX_LINE_LEN];
    char *line = fgets(buf, MAX_LINE_LEN, fp);
    if (line == NULL) {
	fprintf(stderr, "Unable to read data.csv\n");
	exit(EXIT_FAILURE);
    }

    // Open main db file
    Schema *sch = parseSchema(line);
    Table *tbl;
    // tbl = malloc(sizeof(tbl));
    int err;
    Table_Open(DB_NAME, sch, 0, &tbl);
    err = AM_CreateIndex(INDEX_NAME, 1, 'i', 4);
    checkerr(err);
    PF_Init();
    err = PF_CreateFile(INDEX_NAME);
    checkerr(err);
    int indexFD = PF_OpenFile(INDEX_NAME);

    // UNIMPLEMENTED;

    char *tokens[MAX_TOKENS];
    char record[MAX_PAGE_SIZE];

    while ((line = fgets(buf, MAX_LINE_LEN, fp)) != NULL) {
	int n = split(line, ",", tokens);
	assert (n == sch->numColumns);
	int len = encode(sch, tokens, record, sizeof(record));
	RecId rid;

	// UNIMPLEMENTED;
    rid = Table_Insert(tbl, record, len, &rid);

	printf("%d %s\n", rid, tokens[0]);

	// Indexing on the population column 
	int population = atoi(tokens[2]);

    
    err = AM_InsertEntry(indexFD, 'i', 4, (char*)&population, rid);

	// UNIMPLEMENTED;
	// Use the population field as the field to index on

	    
	checkerr(err);
    }
    fclose(fp);
    Table_Close(tbl);
    err = PF_CloseFile(indexFD);
    checkerr(err);
    return sch;
}

int
main() {
    loadCSV();
}
