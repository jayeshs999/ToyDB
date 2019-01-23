#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "pf.h"

#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(1);}}

typedef struct {
    char *name;
    char *type; // one of "string" "int" "long"
} ColumnDesc;

typedef struct {
    int numColumns;
    ColumnDesc **columns; // array of column descriptors
} TableDesc;

#define MAX_LINE_LEN   1000
#define MAX_TOKENS 100



/**
  split takes a character buf, and uses strtok to populate
  an array of token*'s. 
*/
int 
split(char *buf, char *delim, char **tokens) {
    char *token = strtok(buf, delim);
    int n = 0;
    while(token) {
	tokens[n] = token;
	token = strtok(NULL, delim);
	n++;
    }
    return n;
}

TableDesc *
parseSchema(char *buf) {
    char *tokens[MAX_TOKENS];
    int n = split(buf, ",", tokens);
    TableDesc *td = malloc(sizeof(TableDesc));
    td->columns = malloc(n * sizeof(ColumnDesc *));
    // strtok is terrible; it depends on global state.
    // Do one split based on ',".
    // Could use strtok_s for this use case
    char *descTokens[MAX_TOKENS];
    td->numColumns = n;
    for (int i = 0; i < n; i++) {
	int c = split(tokens[i],":", descTokens);
	assert(c == 2);
	ColumnDesc *cd = (ColumnDesc *) malloc(sizeof(ColumnDesc));
	cd->name = strdup(descTokens[0]);
	cd->type = strdup(descTokens[1]);
	td->columns[i] = cd;
    }
    return td;
}

void
addRow(int fd, TableDesc *td, char** fields) {
    
}

#define DB_NAME "data.db"
#define CSV_NAME "data.csv"

int
openDB(char *dbname) {
    PF_Init();
    PF_DestroyFile(dbname);
    int err = PF_CreateFile(dbname);
    checkerr(err);

    err = PF_OpenFile(dbname);
    checkerr(err);
    return err; // should be positive, and represents an fd.
}
    
int
main() {
    
    // Open csv file, parse schema
    FILE *fp = fopen(CSV_NAME, "r");
    if (!fp) {
	perror("data.csv could not be opened");
        return EXIT_FAILURE;
    }

    char buf[MAX_LINE_LEN];
    char *line = fgets(buf, MAX_LINE_LEN, fp);
    if (line == NULL) {
	fprintf(stderr, "Unable to read data.csv\n");
	return EXIT_FAILURE;
    }

    TableDesc *td = parseSchema(line);
    
    int fd = openDB(DB_NAME); // db file descriptor 

    char *tokens[MAX_TOKENS];
    while ((line = fgets(buf, MAX_LINE_LEN, fp)) != NULL) {
	printf("%s\n", line);
	int n = split(line, ",", tokens);
	assert (n == td->numColumns);
	addRow(fd, td, tokens);
    }
    fclose(fp);
    int err = PF_CloseFile(fd);
    checkerr(err);
    return EXIT_SUCCESS;
}
