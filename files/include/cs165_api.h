

/* BREAK APART THIS API (TODO MYSELF) */
/* PLEASE UPPERCASE ALL THE STUCTS */

/*
Copyright (c) 2015 Harvard University - Data Systems Laboratory (DASLab)
Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#ifndef CS165_H
#define CS165_H

#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <pthread.h>

// Limits the size of a name in our database to 64 characters
#define MAX_SIZE_NAME 64
#define HANDLE_MAX_SIZE 64
#define DEFAULT_DB_TABLES_CAPACITY 16
#define DEFAULT_TABLE_CAPACITY 1000000
#define DEFAULT_RESULT_SIZE 1000000
#define DEFAULT_CLIENT_HANDLES 8
#define DEFAULT_MAX_SHARED_SCANS 1
#define DEFAULT_MAX_SELECTS_IN_BATCH 10000
#define MAX_SELECT_THREADS 4
#define SELECT_VECTOR_SIZE 8096 
#define DATABASE_HOME_DIRECTORY "./databases"
#define DATABASE_HOME_LIST "./databases/all_databases"
#define MAX_BTREE_NODE_KEYS 1024

/**
 * EXTRA
 * DataType
 * Flag to mark what type of data is held in the struct.
 * You can support additional types by including this enum and using void*
 * in place of int* in db_operator simliar to the way IndexType supports
 * additional types.
 **/

typedef enum DataType {
     INT,
     LONG,
     DOUBLE
} DataType;

struct Comparator;
struct Table;


struct BtreeNode;

typedef struct BtreeInternalNodeData {
    int keys[MAX_BTREE_NODE_KEYS];
    struct BtreeNode* children[MAX_BTREE_NODE_KEYS + 1]; 
} BtreeInternalNodeData; 

typedef struct BtreeLeafData {
    int data[MAX_BTREE_NODE_KEYS];
    int indices[MAX_BTREE_NODE_KEYS];
    struct BtreeNode* next_leaf; 
} BtreeLeafData; 

typedef union BtreeKeys {
    BtreeLeafData leaf_data; 
    BtreeInternalNodeData internal_data;
} BtreeKeys;

typedef struct BtreeNode {
    bool is_leaf; 
    int num_keys; 
    BtreeKeys data; 
} BtreeNode; 

typedef struct BtreeIndex {
    BtreeNode* btree_root; 
} BtreeIndex; 


typedef struct SortedIndex {
    int* data;     
    int* indices; 
} SortedIndex; 

typedef union IndexFields {
    struct BtreeIndex btree_index; 
    SortedIndex sorted_index;
} IndexFields;

typedef enum IndexType {
    BTREE,
    SORTED
} IndexType;

typedef struct ColumnIndex {
    IndexType type; 
    IndexFields index_fields; 
} ColumnIndex; 

typedef struct Column {
    char name[MAX_SIZE_NAME]; 
    int* data;
    struct Table* table;
    // You will implement column indexes later. 
    struct ColumnIndex *index;
    bool clustered;
} Column;


/**
 * table
 * Defines a table structure, which is composed of multiple columns.
 * We do not require you to dynamically manage the size of your tables,
 * although you are free to append to the struct if you would like to (i.e.,
 * include a size_t table_size).
 * name, the name associated with the table. table names must be unique
 *     within a database, but tables from different databases can have the same
 *     name.
 * - col_count, the number of columns in the table
 * - col,umns this is the pointer to an array of columns contained in the table.
 * - table_length, the size of the columns in the table.
 **/

typedef struct Table {
    char name [MAX_SIZE_NAME];
    Column** columns;
    size_t col_count;
    size_t col_capacity;
    size_t table_length;
    size_t table_capacity; 
    size_t index_column; 
} Table;

/**
 * db
 * Defines a database structure, which is composed of multiple tables.
 * - name: the name of the associated database.
 * - tables: the pointer to the array of tables contained in the db.
 * - tables_size: the size of the array holding table objects
 * - tables_capacity: the amount of pointers that can be held in the currently allocated memory slot
 **/

typedef struct Db {
    char name[MAX_SIZE_NAME]; 
    Table** tables;
    size_t tables_size;
    size_t tables_capacity;
} Db;

/**
 * Error codes used to indicate the outcome of an API call
 **/
typedef enum StatusCode {
  /* The operation completed successfully */
  OK,
  /* There was an error with the call. */
  ERROR,
} StatusCode;

// status declares an error code and associated message
typedef struct Status {
    StatusCode code;
    char* error_message;
} Status;

// Defines a comparator flag between two values.
typedef enum ComparatorType {
    NO_COMPARISON = 0,
    LESS_THAN = 1,
    GREATER_THAN = 2,
    EQUAL = 4,
    LESS_THAN_OR_EQUAL = 5,
    GREATER_THAN_OR_EQUAL = 6
} ComparatorType;

/*
 * Declares the type of a result column, 
 which includes the number of tuples in the result, the data type of the result, and a pointer to the result data
 */
typedef struct Result {
    size_t num_tuples;
    DataType data_type;
    void *payload;
} Result;

/*
 * an enum which allows us to differentiate between columns and results
 */
typedef enum GeneralizedColumnType {
    RESULT,
    COLUMN
} GeneralizedColumnType;
/*
 * a union type holding either a column or a result struct
 */
typedef union GeneralizedColumnPointer {
    Result* result;
    Column* column;
} GeneralizedColumnPointer;

/*
 * unifying type holding either a column or a result
 */
typedef struct GeneralizedColumn {
    GeneralizedColumnType column_type;
    GeneralizedColumnPointer column_pointer;
} GeneralizedColumn;

/*
 * used to refer to a column in our client context
 */

typedef struct GeneralizedColumnHandle {
    char name[HANDLE_MAX_SIZE];
    GeneralizedColumn generalized_column;
} GeneralizedColumnHandle;


typedef struct ResultHandle {
    char name[HANDLE_MAX_SIZE];
    Result* result; 
} ResultHandle; 

/*
 * holds the information necessary to refer to generalized columns (results or columns)
 */
typedef struct ClientContext {
    ResultHandle* chandle_table;
    int chandles_in_use;
    int chandle_slots;
    pthread_mutex_t mutex;
} ClientContext;


/**
 * comparator
 * A comparator defines a comparison operation over a column. 
 **/
typedef struct Comparator {
    long int p_low; // used in equality and ranges.
    long int p_high; // used in range compares. 
    GeneralizedColumn* gen_col;
    GeneralizedColumn* vec_pos; 
    ComparatorType type1;
    ComparatorType type2;
    char handle[HANDLE_MAX_SIZE];
} Comparator;

/*
 * tells the databaase what type of operator this is
 */
typedef enum OperatorType {
    CREATE,
    INSERT,
    OPEN,
    SELECT,
    FETCH,
    SHUTDOWN,
    AGGREGATE,
    PRINT,
    BATCH_QUERIES,
    JOIN
} OperatorType;
/*
 * necessary fields for insertion
 */
typedef struct InsertOperator {
    Table* table;
    int* values;
} InsertOperator;
/*
 * necessary fields for insertion
 */
typedef struct OpenOperator {
    char* db_name;
} OpenOperator;

/* typedef struct SelectOperator { */
/*     int low_val; */
/*     int high_val; */ 
/*     GeneralizedColumn* col1; */ 
/*     GeneralizedColumn* col2; */ 
/* } SelectOperator; */

typedef struct SelectOperator {
    Comparator** comparators;
    size_t comparators_length; 
    size_t comparators_capacity;
} SelectOperator;

typedef struct BatchOperator { 
    SelectOperator** selects; 
    size_t selects_length; 
    size_t selects_capacity; 
} BatchOperator; 

typedef struct PrintOperator {
    GeneralizedColumn** columns; 
    size_t num_columns;
} PrintOperator; 

typedef struct FetchOperator {
    GeneralizedColumn* col1; 
    GeneralizedColumn* col2; 
    char handle[HANDLE_MAX_SIZE]; 
} FetchOperator;

typedef struct ThreadSelect {
    SelectOperator* select; 
    ClientContext* context;
} ThreadSelect;


/*
 * Structs for aggregate operations
 */
typedef enum AggregateType{
    MIN,
    MAX,
    SUM,
    AVG,
    ADD,
    SUB
} AggregateType;

typedef struct AggregateOperator {
    GeneralizedColumn* col1; 
    GeneralizedColumn* col2; 
    AggregateType type;
    char handle[HANDLE_MAX_SIZE];
} AggregateOperator;

typedef enum JoinType {
    HASH,
    NESTED
} JoinType; 

typedef struct JoinOperator {
    JoinType type;     
    Result* val_vec1; 
    Result* pos_vec1;
    Result* val_vec2; 
    Result* pos_vec2; 
    char handle1[HANDLE_MAX_SIZE];
    char handle2[HANDLE_MAX_SIZE];
} JoinOperator; 

/*
 * union type holding the fields of any operator
 */
typedef union OperatorFields {
    InsertOperator insert_operator;
    OpenOperator open_operator;
    SelectOperator select_operator; 
    FetchOperator fetch_operator; 
    AggregateOperator aggregate_operator; 
    PrintOperator print_operator;
    BatchOperator batch_operator; 
    JoinOperator join_operator; 
} OperatorFields;
/*
 * DbOperator holds the following fields:
 * type: the type of operator to perform (i.e. insert, select, ...)
 * operator fields: the fields of the operator in question
 * client_fd: the file descriptor of the client that this operator will return to
 * context: the context of the operator in question. This context holds the local results of the client in question.
 */
typedef struct DbOperator {
    OperatorType type;
    OperatorFields operator_fields;
    int client_fd;
    ClientContext* context;
} DbOperator;


extern Db *current_db;
extern int* shutdown_flag;



/**
 * sync_db(db)
 * Saves the current status of the database to disk.
 *
 * db       : the database to sync.
 * returns  : the status of the operation.
 **/

// initialization functions
Table* init_table();
Column* init_column();
Db* init_db();

Status sync_db(Db* db);

Status add_db(const char* db_name);

Table* create_table(Db* db, const char* name, size_t num_columns, Status *status);

Column* create_column(char *name, Table *table, bool sorted, Status *ret_status);

void execute_insert(DbOperator* query); 

void execute_select(SelectOperator* select_operator, ClientContext* context);

void execute_fetch(DbOperator* query);

void execute_load(char* path_name);

void execute_aggregate(DbOperator* query); 

void execute_shutdown(DbOperator* query);

void execute_batch_queries(DbOperator* query);

// shutdown operations
Status shutdown_server();

void shutdown_db(); 

void write_db_to_disk(Db* db);

void write_table_to_disk(Table* table, char* db_dir); 

void write_column_to_disk(Column* column, Table* table, char* dir); 

/* Status shutdown_database(Db* db); */

char* execute_db_operator(DbOperator* query);
/* char** execute_db_operator(DbOperator* query); */
void free_db_operator(DbOperator* query);

void free_db(Db* db);
void free_table(Table* table);
void free_column(Column* column);
void free_client_context(ClientContext* context); 
void free_result(Result* result);

// startup operations
Status db_startup();

Db* load_db_from_disk(char* db_name); 

Table* load_table_from_disk(char* table_name, char* dir);

Column* load_column_from_disk(char* column_name, char* dir, Table* table); 

//index operations
BtreeNode* btree_create();
void select_from_btree_index(BtreeIndex* index, Comparator* comparator, Result* result); 
void insert_to_unclustered_btree_index(BtreeIndex* index, int val, size_t orig_pos, bool last_val); 
size_t insert_to_clustered_btree_index(BtreeIndex* index, int val);
void get_btree_values(BtreeIndex* index, int* ret);
void free_btree(BtreeNode* root); 

#endif /* CS165_H */

