# CS 387 Assignment 5

Jayesh Singla: 190050053

Shrey Singla: 190050114

# References

# Contributions

### Jayesh Singla
- Implemented first part

### Shrey Singla
- Implemented second and third part

Both of us contributed in debugging the code

# Write up of the execution

## tbl.c

## loaddb.c
Implementation algorithm - 
- First we read the csv file and parse the first line into the struct 'Schema'
- Open the table using Table_Open API, create an integer indexing and open a file to store the indexes (used in AM layer to create B+ tree)
- Iterate over the database row-by-row. Encode the row into a byte array record and insert the record into the Table using the Table_Insert API. Additionally, insert the index using the AM_InsertEntry API call. 
- Finally, close all the open files and the open table.
## dumpdb.c
Implementation algorithm - 
- The printRow function decodes the row according to the type of entry and prints the comma separated values.
- The indexScan function initialises the scan descriptor based on the operation and iterates over the id's returned by the AM layer and prints the rows based on the corresponding recID's

