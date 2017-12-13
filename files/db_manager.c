#define _BSD_SOURCE
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <limits.h>
#include <linux/limits.h>
#include <stdio.h>
#include <pthread.h>
#include <time.h>

#include "include/cs165_api.h"
#include "include/utils.h"
#include "include/client_context.h"
#include "include/hashmap.h"


// In this class, there will always be only one active database at a time
Db *current_db;

Db* init_db() {
    Db* new_db = malloc(sizeof(Db));
    if(new_db == NULL) {
        return NULL;
    }
    new_db->tables = NULL;
    new_db->tables_size = 0;
    new_db->tables_capacity = 0;
    return new_db;
}

Table* init_table() {
    Table* new_table = malloc(sizeof(Table));
    if(new_table == NULL) {
        return NULL;
    }
    new_table->columns = NULL;
    new_table->col_count = 0;
    new_table->col_capacity = 0;
    new_table->table_length = 0;
    new_table->table_capacity = 0;
    return new_table; 
}

Column* init_column() {
    Column* new_column = malloc(sizeof(Column));
    if(new_column == NULL) {
        return NULL;
    }
    new_column->clustered = false;
    new_column->data = NULL;
    new_column->table = NULL;
    new_column->index = NULL;
    return new_column;
}

void free_db_operator(DbOperator* dbo) {
    switch(dbo->type) {
        case CREATE:
            break;
        case INSERT:
        {
            free(dbo->operator_fields.insert_operator.values); 
            break;
        }
        case SELECT:
        {
            for(size_t i = 0; i < dbo->operator_fields.select_operator.comparators_length; i++) {
                free(dbo->operator_fields.select_operator.comparators[i]->gen_col);
                if (dbo->operator_fields.select_operator.comparators[i]->vec_pos != NULL) {
                    free(dbo->operator_fields.select_operator.comparators[i]->vec_pos);
                }
                free(dbo->operator_fields.select_operator.comparators[i]);
            }
            free(dbo->operator_fields.select_operator.comparators);
            break;
        }    
        case FETCH:
        {
            GeneralizedColumn* col1 = dbo->operator_fields.fetch_operator.col1;
            GeneralizedColumn* col2 = dbo->operator_fields.fetch_operator.col2;
            if(col1 != NULL) {
                free(col1);
            }
            if(col2 != NULL) {
                free(col2);
            }
            break;
        } 
        case SHUTDOWN:
            break;
        case OPEN:
            break;
        case PRINT:
            for(size_t i = 0; i < dbo->operator_fields.print_operator.num_columns; i++) {
                free(dbo->operator_fields.print_operator.columns[i]);
            }
            free(dbo->operator_fields.print_operator.columns);
            break;
        case AGGREGATE:
        {
            GeneralizedColumn* col1 = dbo->operator_fields.aggregate_operator.col1;
            GeneralizedColumn* col2 = dbo->operator_fields.aggregate_operator.col2;
            if(col1 != NULL) {
                free(col1);
            }
            if(col2 != NULL) {
                free(col2);
            }
            break;
        }
        case BATCH_QUERIES:
        {
            BatchOperator* batch_operator = &dbo->operator_fields.batch_operator;
            for(size_t i = 0; i < batch_operator->selects_length; i++) {
                SelectOperator* cur_select = batch_operator->selects[i];
                for(size_t j = 0; j < cur_select->comparators_length; j++) {
                    free(cur_select->comparators[j]->gen_col);
                    if (cur_select->comparators[j]->vec_pos != NULL) {
                        free(cur_select->comparators[j]->vec_pos);
                    }
                    free(cur_select->comparators[j]);
                }
                free(cur_select->comparators);
            }
            free(batch_operator->selects);
            break;
        }
        case JOIN:
            break;
    }        

    // free client_context
    free(dbo);
}

/* 
 * Similarly, this method is meant to create a database.
 * As an implementation choice, one can use the same method
 * for creating a new database and for loading a database 
 * from disk, or one can divide the two into two different
 * methods.
 */
Status add_db(const char* db_name) {
	Status ret_status;
    if(strlen(db_name) >= MAX_SIZE_NAME) {
        ret_status.code = ERROR; 
        ret_status.error_message = "DB name is too long! Please insert name with less characters";  
        return ret_status;
    }
    if(current_db != NULL) {
        free_db(current_db);
    }
    Db* new_db = init_db();
    strcpy(new_db->name, db_name);
    new_db->name[strlen(db_name)] = '\0';
    new_db->tables_size = 0;
    new_db->tables_capacity = DEFAULT_DB_TABLES_CAPACITY;
    new_db->tables = (Table**) malloc(sizeof(Table*) * new_db->tables_capacity);
    if(!new_db->tables) {
        free(new_db);
        ret_status.code = ERROR;
        ret_status.error_message = "Failed to create tables array in database";
        return ret_status;
    }

	ret_status.code = OK;
    cs165_log(stdout, "DB created with name: %s\n", db_name);
    current_db = new_db;
	return ret_status;
}

/* 
 * Here you will create a table object. The Status object can be used to return
 * to the caller that there was an error in table creation
 */
Table* create_table(Db* db, const char* name, size_t num_columns, Status *ret_status) {
    // QUESTION: are you allowed to create a table when you are not current_db is not db.name?
    cs165_log(stdout, "create_table");
    if(strlen(name) >= MAX_SIZE_NAME) {
        ret_status->code = ERROR; 
        ret_status->error_message = "Table name is too long! Please insert name with less characters";  
        return NULL;
    }
    Table* new_table = init_table();
    if(!new_table) {
        ret_status->code = ERROR;
        ret_status->error_message = "Failed to allocate memory for the table";
    }
    strcpy(new_table->name, name);
    new_table->name[strlen(name)] = '\0';
    new_table->columns = (Column**) malloc(sizeof(Column*) * num_columns);
    if(!new_table->columns) {
        free(new_table);
        ret_status->code = ERROR; 
        ret_status->error_message = "Failed to allocate memory for columns array in the table.\n";
        return NULL;
    }
    new_table->col_count = 0;
    new_table->col_capacity = num_columns;
    new_table->table_length = 0;
    new_table->table_capacity = DEFAULT_TABLE_CAPACITY;
    new_table->index_column = -1;
    ret_status->code=OK;
    db->tables[db->tables_size] = new_table;
    db->tables_size++;
    cs165_log(stdout, "Table created in DB: %s with name: %s\n", db->name, name);
    return new_table;
}

Column* create_column(char* name, Table* table, bool sorted, Status *ret_status) {
    (void)sorted;
    // QUESTION: when to check if all columns are inserted?
    if(table->col_count == table->col_capacity) {
        ret_status->code = ERROR;
        ret_status->error_message = "Trying to add column, but the maximum number of columns in the table already reached.";
        return NULL;
    } 
    
    if(strlen(name) >= MAX_SIZE_NAME) {
        ret_status->code = ERROR; 
        ret_status->error_message = "Column name is too long! Please insert name with less characters";  
        return NULL;
    }

    Column* new_column = init_column();
    if(!new_column) {
        ret_status->code = ERROR;
        ret_status->error_message = "Failed to allocate memory for the column";
        return NULL;
    }
     
    strcpy(new_column->name, name);
    new_column->name[strlen(name)] = '\0';
    new_column->data = (int*) malloc(sizeof(int) * DEFAULT_TABLE_CAPACITY);
    if(!new_column->data) {
        free(new_column);
        ret_status->code = ERROR;
        ret_status->error_message = "Failed to allocate memory for data in column";
        return NULL;
    }
    new_column->table = table;
    table->columns[table->col_count] = new_column;
    table->col_count++;

    return new_column;    
}

void increase_column_size(Column* column, size_t old_size, size_t new_size) {
    int* new_data = malloc(sizeof(int) * new_size);
    for(size_t i = 0; i < old_size; i++) {
        new_data[i] = column->data[i]; 
    }
    free(column->data);
    column->data = new_data;

    if(column->index != NULL) {
        if(column->index->type == SORTED) {
            int* new_data = malloc(sizeof(int) * new_size);
            int* new_indices = malloc(sizeof(int) * new_size); 
            for(size_t i = 0; i < old_size; i++) {
                new_data[i] = column->index->index_fields.sorted_index.data[i];
                new_indices[i] = column->index->index_fields.sorted_index.indices[i];
            }
            free(column->index->index_fields.sorted_index.data);
            free(column->index->index_fields.sorted_index.indices);
            column->index->index_fields.sorted_index.data = new_data; 
            column->index->index_fields.sorted_index.indices = new_indices; 
        }   
    }
}

void insert_to_array_in_position(int* data,  int val, size_t pos, size_t data_len) {
    size_t i = data_len; 
    while (i > pos) {
        data[i] = data[i-1];
        i--;
    }
    data[i] = val; 
}

void insert_to_sorted_data(int* data, size_t len, int val) {
    size_t i; 
    for(i = len; i > 0 && data[i-1] > val; i--) {
        data[i] = data[i - 1];
    }
    data[i] = val; 
}

// this function inserts the value to an unclustered index on a column.
// it take as input orig_pos, the position in the original column data in which the new value was stored
// The function needs to iterate on all positions and update the index of any value whose
// position in the original column is greater than the position in which the value was inserted in the original column. 
void insert_to_unclustered_index(ColumnIndex* index, int val, size_t orig_pos, size_t col_len) {
    if(index->type == SORTED) {
        // if we inserted to the last position in the column, we just shift all the values one step up
        // to make room to the new values and there is no need to update any position value in the index
        if(orig_pos == col_len) {
            SortedIndex ind = index->index_fields.sorted_index;
            size_t i; 
            for(i = col_len; i > 0 && ind.data[i-1] > val; i--) {
                ind.data[i] = ind.data[i - 1];
                ind.indices[i] = ind.indices[i-1];
            }
            ind.data[i] = val; 
            ind.indices[i] = orig_pos; 
        }
        // Otherwise we need to shift all the values, and for every position that is after the one in the original 
        // column where the value was inserted we need to increase the position by one 
        else {
            SortedIndex ind = index->index_fields.sorted_index;
            size_t i; 
            for(i = col_len; i > 0 && ind.data[i-1] > val; i--) {
                ind.data[i] = ind.data[i - 1];
                ind.indices[i] = ind.indices[i - 1];
                if((size_t)ind.indices[i] >= (size_t)orig_pos) {
                    ind.indices[i] += 1;
                }
            }
            ind.data[i] = val; 
            ind.indices[i] = orig_pos; 
            // update the positions of the other values
            for(size_t j = 0; j < i; j++) {
                if((size_t)ind.indices[j] >= orig_pos) {
                    ind.indices[j] += 1;
                }
            }
        }
    }
    else {
        if(orig_pos == col_len) {
            // if the value inserted was the last in the column, we call the function with "true"
            // so that we don't iterate over all values in the btree to update positions
            insert_to_unclustered_btree_index(&index->index_fields.btree_index, val, orig_pos, true); 
        }
        else {
            insert_to_unclustered_btree_index(&index->index_fields.btree_index, val, orig_pos, false); 
        }
    }
}

// this function inserts the value to the appropriate place in the index and returns the position. 
// called to insert a value to the leading column in a clustered index. 
size_t insert_to_clustered_index(ColumnIndex* index, int val, size_t col_len) {
    if(index->type == SORTED) {
        SortedIndex ind = index->index_fields.sorted_index;
        size_t i; 
        for(i = col_len; i > 0 && ind.data[i-1] > val; i--) {
            ind.data[i] = ind.data[i - 1];
            ind.indices[i] = i;
        }
        ind.data[i] = val; 
        ind.indices[i] = i; 
        return i; 
    }
    else {
        return insert_to_clustered_btree_index(&index->index_fields.btree_index, val); 
    }
}

void insert_to_clustered_column(Column* column, int val, size_t* pos, bool insert_pos) {
    size_t col_len = column->table->table_length; 
    // in this case we insert to column that is the main one in a clusteted index, 
    // we need to insert and get the position so we can insert in the right location in the other columns
    // in the clustered table
    if(!insert_pos) {
        column->data[col_len] = val;  
        *pos = insert_to_clustered_index(column->index, val, col_len);
    }
    // in this case we need to insert to the column in a specific location 
    else {
        insert_to_array_in_position(column->data, val, *pos, col_len);
        if(column->index != NULL) {
            insert_to_unclustered_index(column->index, val, *pos, col_len);
        }
    }
}

// In this case we just insert to column in the last available location and its index if exists
void insert_to_unclustered_column(Column* column, int val) {
    size_t col_len = column->table->table_length; 

    column->data[col_len] = val;  
    if(column->index != NULL) {
        // pass col_len as the position to which the value was inserted in the column
        insert_to_unclustered_index(column->index, col_len, val, col_len);
    }
}

// insert to table without clustered index
void insert_to_unclustered_table(Table* table, int* values) {
    for(size_t i = 0; i < table->col_count; i++) {
        insert_to_unclustered_column(table->columns[i], values[i]);
    }
    table->table_length++;
}

// insert to table with clustered index
void insert_to_clustered_table(Table* table, int* values) {
    size_t pos; 
    // pass false to let function know we insert to the leading column in a clustered index, we need
    // to write the position to which the value was inserted into pos variable
    insert_to_clustered_column(table->columns[table->index_column], values[table->index_column], &pos, false);
    for(size_t i = 0; i < table->col_count; i++) {
        // for all the columns that are not the leading one, we pass the position to which the value 
        // should be inserted
        if(i != table->index_column) {
            insert_to_clustered_column(table->columns[i], values[i], &pos, true);
        }
    }   
    table->table_length++;
}

void execute_insert(DbOperator* query) {
    Table* table = query->operator_fields.insert_operator.table;
    int* values = query->operator_fields.insert_operator.values;

    // reallocate the columns if needed
    if(table->table_length == table->table_capacity) {
        for(size_t i = 0; i < table->col_count; i++) {
            increase_column_size(table->columns[i], table->table_capacity, table->table_capacity * 2);
        }
    }

    if(table->index_column != (size_t)(-1)) {
        insert_to_clustered_table(table, values); 
    }
    else {
        insert_to_unclustered_table(table, values);
    }
}

// loads the passed in data into the table. Does not handle bulk loading at the moment, 
// inserts values one by one.
void execute_load(char* data) {
    char* all_columns = strsep(&data, "\n"); 
    int num_cols = count_commas(all_columns) + 1;
    
    /* Column* all_cols[num_cols]; */
    Table* load_table; 
    /* char* column_name; */
    char* full_column_name = strsep(&all_columns, ",");
    strsep(&full_column_name, ".");
    char* table_name = strsep(&full_column_name, ".");
    load_table = lookup_table(table_name);

    char* cur_val;
    int i;
    char* line = NULL;
    int insert_values[num_cols];
    while((line = strsep(&data, "\n"))) {
        if(strcmp(line, "") != 0) {
            for(i = 0; i < num_cols - 1; i++) {
                cur_val = strsep(&line, ",");
                insert_values[i] = atoi(cur_val); 
            }
            cur_val = strsep(&line, "\n");
            insert_values[i] = atoi(cur_val);
            if(load_table->index_column != (size_t)(-1)) {
                insert_to_clustered_table(load_table, insert_values); 
            }
            else {
                insert_to_unclustered_table(load_table, insert_values);
            }
        }
    }
}

/* Free operators to be used on shutdown */ 
void free_result(Result* result) {
    free(result->payload);
    free(result); 
}

void free_client_context(ClientContext* context) {
    for(int i = 0; i < context->chandles_in_use; i++) {
        free_result(context->chandle_table[i].result); 
    }
    free(context->chandle_table);
    free(context);
}

void free_column(Column* column) {
    free(column->data);    
    if(column->index != NULL) {
        if(column->index->type == SORTED) {
            free(column->index->index_fields.sorted_index.data); 
            free(column->index->index_fields.sorted_index.indices); 
            free(column->index);
        }
        else {
            free_btree(column->index->index_fields.btree_index.btree_root);
            free(column->index);
        }
    }
    free(column);
}

void free_table(Table* table) {
    for(size_t i = 0; i < table->col_count; i++) {
        free_column(table->columns[i]);
    }
    free(table->columns);
    free(table);
}

void free_db(Db* db) {
    for(size_t i = 0; i < db->tables_size; i++) {
        free_table(db->tables[i]);
    }
    free(db->tables); 
    free(db);
}

void execute_shutdown(DbOperator* query) {
    shutdown_db();
    free_client_context(query->context);
}

void shutdown_db() {
    /* int result = mkdir("/home/dorbaruch/databases", 0777); */
    mkdir(DATABASE_HOME_DIRECTORY, 0777);
    
    FILE* fp = NULL;
    fp = fopen(DATABASE_HOME_LIST, "w");
    fwrite(current_db->name, MAX_SIZE_NAME, 1, fp);
    fclose(fp); 

    write_db_to_disk(current_db);
    free_db(current_db); 
}

void write_db_to_disk(Db* db) {
    char full_file_name[PATH_MAX];
    strcpy(full_file_name, DATABASE_HOME_DIRECTORY); 
    strcat(full_file_name, "/");
    strcat(full_file_name, db->name);
    FILE* fp = fopen(full_file_name, "w");

    int to_write[] = {db->tables_size, db->tables_capacity};
    fwrite(to_write, sizeof(int), 2, fp);
    for(unsigned int i = 0; i < db->tables_size; i++) {
        fwrite(db->tables[i]->name, MAX_SIZE_NAME, 1, fp);
    }
    fclose(fp);
    for(unsigned int i = 0; i < db->tables_size; i++) {
        write_table_to_disk(db->tables[i], full_file_name);
    }
}

void write_table_to_disk(Table* table, char* dir) {
    char full_file_name[PATH_MAX];
    strcpy(full_file_name, dir);
    strcat(full_file_name, ".");
    strcat(full_file_name, table->name);
    FILE* fp = fopen(full_file_name, "w");

    size_t to_write[] = {table->col_count, table->col_capacity, table->table_length, table->index_column};
    fwrite(to_write, sizeof(size_t), 4, fp);
    for(size_t i = 0; i < table->col_count; i++) {
        fwrite(table->columns[i]->name, MAX_SIZE_NAME, 1, fp);
    }
    fclose(fp);
    for(size_t i = 0; i < table->col_count; i++) {
        write_column_to_disk(table->columns[i], table, full_file_name);
    }
}

void write_index_to_disk(Column* column, char* dir) {
    char full_file_name[PATH_MAX];
    strcpy(full_file_name, dir);
    strcat(full_file_name, ".");
    strcat(full_file_name, "index");
    FILE* fp = fopen(full_file_name, "w");

    char index_type[MAX_SIZE_NAME];
    if(column->index->type == SORTED) {
        strcpy(index_type, "sorted");
        fwrite(index_type, MAX_SIZE_NAME, 1, fp);
        fwrite(column->index->index_fields.sorted_index.data, sizeof(int), column->table->table_length, fp);
        fwrite(column->index->index_fields.sorted_index.indices, sizeof(int), column->table->table_length, fp);
    }
    else {
        strcpy(index_type, "btree");
        fwrite(index_type, MAX_SIZE_NAME, 1, fp);
        int* btree_data = malloc(sizeof(int) * column->table->table_length);
        get_btree_values(&column->index->index_fields.btree_index, btree_data);
        fwrite(btree_data, sizeof(int), column->table->table_length, fp);
        free(btree_data);
    }
    fclose(fp);
}

void write_column_to_disk(Column* column, Table* table, char* dir) {
    char full_file_name[PATH_MAX];
    strcpy(full_file_name, dir);
    strcat(full_file_name, ".");
    strcat(full_file_name, column->name);
    FILE* fp = fopen(full_file_name, "w");
    char clustered[MAX_SIZE_NAME];
    if(column->clustered) {
        strcpy(clustered, "clustered");
        fwrite(clustered, MAX_SIZE_NAME, 1, fp);
    }
    else {
        strcpy(clustered, "unclustered");
        fwrite(clustered, MAX_SIZE_NAME, 1, fp);
    }
    fwrite(column->data, sizeof(int), table->table_length, fp);
    fclose(fp);
    if(column->index != NULL) {
        write_index_to_disk(column, full_file_name);
    }
}


Status db_startup() {
    FILE* fp = fopen(DATABASE_HOME_LIST, "r");
    if(fp != NULL) {
        char db_name[MAX_SIZE_NAME];
        fread(db_name, sizeof(db_name), 1, fp);

        current_db = load_db_from_disk(db_name);
        fclose(fp);
    }
    struct Status ret_status; 
    ret_status.code = OK;
    return ret_status;
}

Db* load_db_from_disk(char* db_name) {
    char full_file_name[PATH_MAX];
    strcpy(full_file_name, DATABASE_HOME_DIRECTORY); 
    strcat(full_file_name, "/");
    strcat(full_file_name, db_name);
    FILE* fp = fopen(full_file_name, "r");
    
    Db* new_db = init_db();
    strcpy(new_db->name, db_name); 
    fread(&new_db->tables_size, sizeof(int), 1, fp); 
    fread(&new_db->tables_capacity, sizeof(int), 1, fp);   
    new_db->tables = (Table**) malloc(sizeof(Table*) * new_db->tables_capacity);
    if(!new_db->tables) {
        free(new_db);
        /* ret_status.code = ERROR; */
        /* ret_status.error_message = "Failed to create tables array in database"; */
        return NULL;
    }
    
    char tables_names[MAX_SIZE_NAME * new_db->tables_size]; 
    /* char* tables_names = malloc(MAX_SIZE_NAME * new_db->tables_size); */
    fread(tables_names, MAX_SIZE_NAME, new_db->tables_size, fp); 
    for(unsigned int i = 0, j = 0; i < new_db->tables_size; i++, j = j + MAX_SIZE_NAME) {
        char* table_name = (char*)(&tables_names[j]);
        new_db->tables[i] = load_table_from_disk(table_name, full_file_name);
    }
    fclose(fp);
    return new_db;
}

Table* load_table_from_disk(char* table_name, char* dir) {
    char full_file_name[PATH_MAX];
    strcpy(full_file_name, dir); 
    strcat(full_file_name, ".");
    strcat(full_file_name, table_name); 
    FILE* fp = fopen(full_file_name, "r");

    Table* new_table = init_table();
    strcpy(new_table->name, table_name);
    fread(&new_table->col_count, sizeof(size_t), 1, fp);
    fread(&new_table->col_capacity, sizeof(size_t), 1, fp);
    fread(&new_table->table_length, sizeof(size_t), 1, fp);
    fread(&new_table->index_column, sizeof(size_t), 1, fp);
    new_table->columns = (Column**) malloc(sizeof(Column*) * new_table->col_capacity);
    if(!new_table->columns) {
        free(new_table);
        return NULL;
    }

    char column_names[MAX_SIZE_NAME * new_table->col_count];
    fread(column_names, MAX_SIZE_NAME, new_table->col_count, fp); 
    for(unsigned int i = 0, j = 0; i < new_table->col_count; i++, j+=MAX_SIZE_NAME) {
        char* column_name = (char*)(&column_names[j]);
        new_table->columns[i] = load_column_from_disk(column_name, full_file_name, new_table);
        if(new_table->columns[i]->clustered) {
            new_table->index_column = i;
        }
    }
    fclose(fp);
    return new_table; 
}

ColumnIndex* load_index_from_disk(char* dir, Column* column) {
    char full_file_name[PATH_MAX];
    strcpy(full_file_name, dir); 
    strcat(full_file_name, ".");
    strcat(full_file_name, "index"); 
    FILE* fp = fopen(full_file_name, "r");
    if(fp == NULL) {
        return NULL;
    }
    
    size_t col_len = column->table->table_length;
    ColumnIndex* index = malloc(sizeof(ColumnIndex));
    char index_type[MAX_SIZE_NAME];
    fread(index_type, MAX_SIZE_NAME, 1, fp); 
    if(strcmp(index_type, "sorted") == 0) {
        index->type = SORTED;
        index->index_fields.sorted_index.data = malloc(sizeof(int) * col_len);
        fread(index->index_fields.sorted_index.data, sizeof(int), col_len, fp); 
        index->index_fields.sorted_index.indices = malloc(sizeof(int) * col_len);
        fread(index->index_fields.sorted_index.indices, sizeof(int), col_len, fp); 
    }
    else {
        index->type = BTREE; 
        index->index_fields.btree_index.btree_root = malloc(sizeof(BtreeNode));
        index->index_fields.btree_index.btree_root->is_leaf = true; 
        index->index_fields.btree_index.btree_root->num_keys = 0; 
        index->index_fields.btree_index.btree_root->data.leaf_data.next_leaf = NULL; 
        if(column->clustered) {
            int* btree_data = malloc(sizeof(int) * column->table->table_length);
            fread(btree_data, sizeof(int), column->table->table_length, fp);
            for(size_t i = 0; i < col_len; i++) { 
                insert_to_clustered_btree_index(&index->index_fields.btree_index, btree_data[i]); 
            }
            free(btree_data);
        }
        else {
            // TODO: probably need to change the writing of btree to disk, we need to also write the 
            // original positions and then use them when loading (instead of i). 
            for(size_t i = 0; i < col_len; i++) { 
                insert_to_unclustered_btree_index(&index->index_fields.btree_index, column->data[i], i, true); 
            }
        }
    }
    return index;
}

Column* load_column_from_disk(char* column_name, char* dir, Table* table) {
    char full_file_name[PATH_MAX];
    strcpy(full_file_name, dir); 
    strcat(full_file_name, ".");
    strcat(full_file_name, column_name); 
    FILE* fp = fopen(full_file_name, "r");

    Column* new_column = init_column();
    new_column->table = table;
    strcpy(new_column->name, column_name);

    char clustered[MAX_SIZE_NAME];
    fread(clustered, MAX_SIZE_NAME, 1, fp); 
    if(strcmp(clustered, "clustered") == 0) {
        new_column->clustered = true;
    }
    else {
        new_column->clustered = false; 
    }

    new_column->data = (int*) malloc(sizeof(int) * table->table_length);
    if(!new_column->data) {
        free(new_column);
        return NULL;
    }
    fread(new_column->data, sizeof(int), table->table_length, fp); 
    fclose(fp);

    new_column->index = load_index_from_disk(full_file_name, new_column);

    return new_column;
}

void* thread_select(void* args) {
    ThreadSelect* cast_arg = (ThreadSelect*) args;
    execute_select(cast_arg->select, cast_arg->context);
    return (void*)args;
}

void execute_batch_queries(DbOperator* query) {
    BatchOperator batch_operator = query->operator_fields.batch_operator;
    size_t num_selects = batch_operator.selects_length;
    pthread_t threads[num_selects];
    ThreadSelect args[num_selects];

    for(size_t j = 0; j < num_selects; j+=MAX_SELECT_THREADS) {
        for(size_t i = j; i < j + MAX_SELECT_THREADS && i < num_selects; i++) {
            args[i].select = batch_operator.selects[i];
            args[i].context = query->context; 
            pthread_create(&threads[i], NULL, thread_select, (void*)(&args[i]));
        }
        for(size_t i = j; i < j + MAX_SELECT_THREADS && i < num_selects; i++) {
            pthread_join(threads[i], NULL);
        }
    }
}

void execute_join(DbOperator* query) {
    ClientContext* context = query->context; 
    JoinOperator join = query->operator_fields.join_operator;
    Result* result1 = malloc(sizeof(Result)); 
    result1->payload = malloc(sizeof(int) * join.val_vec1->num_tuples);
    result1->num_tuples = 0; 
    Result* result2 = malloc(sizeof(Result)); 
    result2->payload = malloc(sizeof(int) * join.val_vec2->num_tuples);
    result2->num_tuples = 0; 

    if(join.type == HASH) {
        Hashmap* hashmap = hashmap_create(); 
        // insert all the values from the first key-pos pair into the hash table
        for(size_t i = 0; i < join.val_vec1->num_tuples; i++) {
            hashmap_put(hashmap, ((int*)join.val_vec1->payload)[i], ((int*)join.pos_vec1->payload)[i]); 
        }
        // probe the hashtable and update results when found match 
        for(size_t i = 0; i < join.val_vec2->num_tuples; i++) {
            int pos1 = hashmap_get(hashmap, ((int*)join.val_vec2->payload)[i]);  
            if(pos1 != -1) {
                insert_to_sorted_data((int*)result1->payload, result1->num_tuples, pos1); 
                result1->num_tuples++; 
                insert_to_sorted_data((int*)result2->payload, result2->num_tuples, ((int*)join.pos_vec2->payload)[i]); 
                result2->num_tuples++; 
            } 
        }
    }
    else if (join.type == NESTED) {
        size_t vector_size = 1024; 
        for(size_t i = 0; i < join.val_vec1->num_tuples; i+=vector_size) {
            for(size_t j = 0; j < join.val_vec2->num_tuples; j+=vector_size) {
                for(size_t r = i; r < i + vector_size && r < join.val_vec1->num_tuples; r++) {
                    for(size_t m = j; m < j + vector_size && m < join.val_vec2->num_tuples; m++) {
                        if(((int*)join.val_vec1->payload)[r] == ((int*)join.val_vec2->payload)[m]) {
                            insert_to_sorted_data((int*)result1->payload, result1->num_tuples, ((int*)join.pos_vec1->payload)[r]); 
                            result1->num_tuples++; 
                            insert_to_sorted_data((int*)result2->payload, result2->num_tuples, ((int*)join.pos_vec2->payload)[m]); 
                            result2->num_tuples++; 
                        }

                    }
                
                }
            }
        }
    }
    add_result_to_context(context, join.handle1, result1);
    add_result_to_context(context, join.handle2, result2);
}

char* execute_db_operator(DbOperator* query) {
    if(query == NULL) {
        return NULL;
    }
    switch(query->type) {
        case CREATE:
            break;
            /* return "Create operation was successful"; */
        case INSERT:
        {
            execute_insert(query); 
            break;
            /* return "Insert operation was successful"; */
        }
        case SELECT:
        {
            clock_t start, end;
            double cpu_time_used;
            start = clock();
            execute_select(&query->operator_fields.select_operator, query->context);
            end = clock(); 
            cpu_time_used = ((double) (end-start)) / CLOCKS_PER_SEC;
            /* log_info("Batched queries took %f seconds.\n", cpu_time_used); */
            cs165_log(stdout, "Batched queries took %f seconds. \n", cpu_time_used);
            break;
            /* return "Select operation was successful"; */
        }    
        case FETCH:
        {
            execute_fetch(query);     
            break;
            /* return "Fetch operation was successful"; */ 
        } 
        case SHUTDOWN:
            execute_shutdown(query);
            break;
            /* return "Shutdown operation was successful"; */
        case OPEN:
            break;
        case PRINT:
            break;
        case AGGREGATE:
            execute_aggregate(query);
            break;
        case BATCH_QUERIES: 
        {
            clock_t start, end;
            double cpu_time_used;
            start = clock();
            execute_batch_queries(query); 
            end = clock(); 
            cpu_time_used = ((double) (end-start)) / CLOCKS_PER_SEC;
            /* log_info("Batched queries took %f seconds.\n", cpu_time_used); */
            cs165_log(stdout, "Batched queries took %f seconds. \n", cpu_time_used);
            break;
        }
        case JOIN:
            execute_join(query);
            break;
    }
    free_db_operator(query);
    return NULL;
}

// this is used when selecting with shared scans so running a few comparators in parallel on one column 
void select_unsorted_data_shared(int* data, Comparator* comparator, Result* result, size_t cur_loc, size_t vector_size, size_t data_length) {
    int p_low = comparator->p_low;
    int p_high = comparator->p_high;
    size_t i;
    if(comparator->type1 == NO_COMPARISON && comparator->type2 == NO_COMPARISON) {
        for(i = cur_loc; i < cur_loc + vector_size && i < data_length; i++) {
            ((int*)result->payload)[result->num_tuples++] = i; 
        }
    } else if (comparator->type1 == NO_COMPARISON && comparator->type2 != NO_COMPARISON) {
        for(i = cur_loc; i < cur_loc + vector_size && i < data_length; i++) {
            if(data[i] < p_high) {
                ((int*)result->payload)[result->num_tuples++] = i;
            }
        }
    } else if (comparator->type1 != NO_COMPARISON && comparator->type2 == NO_COMPARISON) {
        for(i = cur_loc; i < cur_loc + vector_size && i < data_length; i++) {
            if(data[i] >= p_low) {
                ((int*)result->payload)[result->num_tuples++] = i;
            }
        }
    } else {
        for(i = cur_loc; i < cur_loc + vector_size && i < data_length; i++) {
            if(data[i] >= p_low && data[i] < p_high) {
                ((int*)result->payload)[result->num_tuples++] = i;
            }
        }
    }
}

// used in regular select without shared scans
void select_unsorted_data(int* data, Comparator* comparator, Result* result, size_t data_length) {
    int p_low = comparator->p_low;
    int p_high = comparator->p_high;
    size_t i;
    //TODO: Figure out a better way because this is too much memory!
    if(comparator->type1 == NO_COMPARISON && comparator->type2 == NO_COMPARISON) {
        for(i = 0; i < data_length; i++) {
            ((int*)result->payload)[result->num_tuples++] = i; 
        }
    } else if (comparator->type1 == NO_COMPARISON && comparator->type2 != NO_COMPARISON) {
        for(i = 0; i < data_length; i++) {
            if(data[i] < p_high) {
                ((int*)result->payload)[result->num_tuples++] = i;
            }
        }
    } else if (comparator->type1 != NO_COMPARISON && comparator->type2 == NO_COMPARISON) {
        for(i = 0; i < data_length; i++) {
            if(data[i] >= p_low) {
                ((int*)result->payload)[result->num_tuples++] = i;
            }
        }
    } else {
        for(i = 0; i < data_length; i++) {
            if(data[i] >= p_low && data[i] < p_high) {
                ((int*)result->payload)[result->num_tuples++] = i;
            }
        }
    }
}

// shared scans in the case of 4 arguments to select 
void select_unsorted_data_with_pos_vec_shared(int* data, int* pos_vec, Comparator* comparator, Result* result, size_t cur_loc, size_t vector_size, size_t data_length) {
    int p_low = comparator->p_low;
    int p_high = comparator->p_high;
    size_t i;
    if(comparator->type1 == NO_COMPARISON && comparator->type2 == NO_COMPARISON) {
        for(i = cur_loc; i < cur_loc + vector_size && i < data_length; i++) {
            ((int*)result->payload)[result->num_tuples++] = pos_vec[i]; 
        }
    } else if (comparator->type1 == NO_COMPARISON && comparator->type2 != NO_COMPARISON) {
        for(i = cur_loc; i < cur_loc + vector_size && i < data_length; i++) {
            if(data[i] < p_high) {
                ((int*)result->payload)[result->num_tuples++] = pos_vec[i];
            }
        }
    } else if (comparator->type1 != NO_COMPARISON && comparator->type2 == NO_COMPARISON) {
        for(i = cur_loc; i < cur_loc + vector_size && i < data_length; i++) {
            if(data[i] >= p_low) {
                ((int*)result->payload)[result->num_tuples++] = pos_vec[i];
            }
        }
    } else {
        for(i = cur_loc; i < cur_loc + vector_size && i < data_length; i++) {
            if(data[i] >= p_low && data[i] < p_high) {
                ((int*)result->payload)[result->num_tuples++] = pos_vec[i];
            }
        }
    }
}

void select_unsorted_data_with_pos_vec(int* data, int* pos_vec, Comparator* comparator, Result* result, size_t data_length) {
    int p_low = comparator->p_low;
    int p_high = comparator->p_high;
    size_t i;
    if(comparator->type1 == NO_COMPARISON && comparator->type2 == NO_COMPARISON) {
        for(i = 0; i < data_length; i++) {
            ((int*)result->payload)[result->num_tuples++] = pos_vec[i]; 
        }
    } else if (comparator->type1 == NO_COMPARISON && comparator->type2 != NO_COMPARISON) {
        for(i = 0; i < data_length; i++) {
            if(data[i] < p_high) {
                ((int*)result->payload)[result->num_tuples++] = pos_vec[i];
            }
        }
    } else if (comparator->type1 != NO_COMPARISON && comparator->type2 == NO_COMPARISON) {
        for(i = 0; i < data_length; i++) {
            if(data[i] >= p_low) {
                ((int*)result->payload)[result->num_tuples++] = pos_vec[i];
            }
        }
    } else {
        for(i = 0; i < data_length; i++) {
            if(data[i] >= p_low && data[i] < p_high) {
                ((int*)result->payload)[result->num_tuples++] = pos_vec[i];
            }
        }
    }
}
void select_sorted_data_with_pos_vec(int* data, int* pos_vec, Comparator* comparator, Result* result, size_t start_loc, size_t data_length) {
    int p_low = comparator->p_low;
    int p_high = comparator->p_high;
    size_t i;
    if(comparator->type1 == NO_COMPARISON && comparator->type2 == NO_COMPARISON) {
        for(i = start_loc; i < data_length; i++) {
            ((int*)result->payload)[result->num_tuples++] = pos_vec[i]; 
        }
    } else if (comparator->type1 == NO_COMPARISON && comparator->type2 != NO_COMPARISON) {
        for(i = start_loc; i < data_length; i++) {
            if(data[i] < p_high) {
                ((int*)result->payload)[result->num_tuples++] = pos_vec[i];
            }
            else {
                break;
            }
        }
    } else if (comparator->type1 != NO_COMPARISON && comparator->type2 == NO_COMPARISON) {
        for(i = start_loc; i < data_length; i++) {
            if(data[i] >= p_low) {
                ((int*)result->payload)[result->num_tuples++] = pos_vec[i];
            }
        }
    } else {
        for(i = start_loc; i < data_length; i++) {
            if(data[i] >= p_low) {
                if (data[i] < p_high) {
                    ((int*)result->payload)[result->num_tuples++] = pos_vec[i];
                }
                else {
                    break;
                }
            }
        }
    }
}

size_t search_in_array(int* data, int val, size_t data_len) {
    size_t low = 0; 
    size_t high = data_len - 1; 
    size_t mid = 0;
    while (low != high) {
        mid = low + ((high - low) / 2);
        if (data[mid] == val) {
            high = mid; 
            break; 
        }
        else if (data[mid] < val) {
            low = mid + 1;
        }
        else {
            high = mid;
        }
    }
    
    // in case of duplicate values in the array
    for(int i = high - 1; i >= 0; i--) {
        if(data[i] == data[high]) {
            high--; 
        }
    }
    return high; 
}

void select_from_sorted_index(SortedIndex* index, Comparator* comparator, Result* result, size_t data_len) {
    size_t start_pos = 0;
    if(comparator->type1 != NO_COMPARISON) {
        start_pos = search_in_array(index->data, comparator->p_low, data_len);
    }

    select_sorted_data_with_pos_vec(index->data, index->indices, comparator, result, start_pos, data_len);
}

void select_from_index(ColumnIndex* index, Comparator* comparator, Result* result, size_t data_len) {
    if(index->type == SORTED) {
        select_from_sorted_index(&index->index_fields.sorted_index, comparator, result, data_len); 
    }
    else if (index->type == BTREE) {
        select_from_btree_index(&index->index_fields.btree_index, comparator, result);     
    } 
}

void execute_select(SelectOperator* select_operator, ClientContext* context) {
    Comparator** comparators = select_operator->comparators;
    size_t num_comparators = select_operator->comparators_length;
    
    // only handle selects from index when we don't have shared scans
    if(num_comparators == 1) {
        Comparator* comparator = comparators[0]; 
        GeneralizedColumn* col_vec = comparator->gen_col; 

        int* pos_vec_data = NULL; 
        GeneralizedColumn* pos_vec = comparator->vec_pos;
        if(pos_vec != NULL) {
            if(pos_vec->column_type == COLUMN) {
                pos_vec_data = pos_vec->column_pointer.column->data;
            }
            else {
                pos_vec_data = pos_vec->column_pointer.result->payload;
            }
        }

        Result* result =  malloc(sizeof(Result));
        result->num_tuples = 0;
        result->data_type = INT;

        if(col_vec->column_type == COLUMN) {
            Column* column = col_vec->column_pointer.column;

            size_t data_len = column->table->table_length; 
            result->payload = malloc(sizeof(int) * data_len);

            if(pos_vec_data != NULL) {
                select_unsorted_data_with_pos_vec(column->data, pos_vec_data, comparator, result, data_len);
            }
            else {
                if(column->index != NULL) {
                    select_from_index(column->index, comparator, result, data_len);
                }
                else {
                    select_unsorted_data(column->data, comparator, result, data_len);
                }
            }
        }
        else {
            Result* res = col_vec->column_pointer.result;

            size_t data_len = res->num_tuples; 
            result->payload = malloc(sizeof(int) * data_len);

            if(pos_vec_data != NULL) {
                select_unsorted_data_with_pos_vec(res->payload, pos_vec_data, comparator, result, data_len); 
            }
            else {
                select_unsorted_data(res->payload, comparator, result, data_len);
            }
        }
        add_result_to_context(context, comparator->handle, result);
    }
    // shared scans case
    else {
        int* col_vec_data;
        int* pos_vec_data = NULL;
        size_t data_length = 0;
        // create arrays to hold data per comparator
        char* handles[num_comparators];
        Result* results[num_comparators];

        GeneralizedColumn* col_vec = comparators[0]->gen_col;
        if(col_vec->column_type == COLUMN) {
            col_vec_data = col_vec->column_pointer.column->data;
            data_length = col_vec->column_pointer.column->table->table_length; 
        }
        else {
            col_vec_data = col_vec->column_pointer.result->payload;
            data_length = col_vec->column_pointer.result->num_tuples;
        }

        GeneralizedColumn* pos_vec = comparators[0]->vec_pos;
        if(pos_vec != NULL) {
            if(pos_vec->column_type == COLUMN) {
                pos_vec_data = pos_vec->column_pointer.column->data;
            }
            else {
                pos_vec_data = pos_vec->column_pointer.result->payload;
            }
        }

        for(size_t ind = 0; ind < num_comparators; ind++) {
            handles[ind] = comparators[ind]->handle;

            results[ind] = malloc(sizeof(Result));
            results[ind]->payload = malloc(sizeof(int) * data_length);
            results[ind]->num_tuples = 0;
            results[ind]->data_type = INT;
        }

        int vector_size = SELECT_VECTOR_SIZE; 
        // 3 arguments case
        if(pos_vec_data == NULL) {
            for(size_t cur_loc = 0; cur_loc < data_length; cur_loc += vector_size) {
                for(size_t ind = 0; ind < num_comparators; ind++) {
                    select_unsorted_data_shared(col_vec_data, comparators[ind], results[ind], cur_loc, vector_size, data_length);
                }
            }
        }
        // four arguments case
        else {
            for(size_t cur_loc = 0; cur_loc < data_length; cur_loc += vector_size) {
                for(size_t ind = 0; ind < num_comparators; ind++) {
                    select_unsorted_data_with_pos_vec_shared(col_vec_data, pos_vec_data, comparators[ind], results[ind], cur_loc, vector_size, data_length);
                }
            }
        }
        for(size_t ind = 0; ind < num_comparators; ind++) {
            add_result_to_context(context, handles[ind], results[ind]);
        }
    }
}

void execute_fetch(DbOperator* query) {
    ClientContext* context = query->context;
    Column* val_vec = query->operator_fields.fetch_operator.col1->column_pointer.column;
    Result* pos_vec = query->operator_fields.fetch_operator.col2->column_pointer.result; 
    char* handle = query->operator_fields.fetch_operator.handle;

    Result* result = (Result*) malloc(sizeof(Result));
    result->num_tuples = pos_vec->num_tuples;
    result->payload = malloc(sizeof(int) * result->num_tuples);
    result->data_type = INT;
    for(size_t i = 0; i < pos_vec->num_tuples; i++) {
        ((int*)result->payload)[i] = val_vec->data[((int*)pos_vec->payload)[i]];
    }

    add_result_to_context(context, handle, result); 
}

void execute_sum_avg(DbOperator* query, bool sum) {
    ClientContext* context = query->context;
    GeneralizedColumn* col = query->operator_fields.aggregate_operator.col1;
    char* handle = query->operator_fields.aggregate_operator.handle;

    int* data = NULL;
    size_t data_length = 0;

    if(col->column_type == COLUMN) {
        Column* vec = col->column_pointer.column;
        data = vec->data;
        data_length = vec->table->table_length;
    }
    else {
        Result* vec = col->column_pointer.result; 
        data = vec->payload; 
        data_length = vec->num_tuples;
    }

    Result* result = (Result*) malloc(sizeof(Result));
    if(sum) {
        result->payload = malloc(sizeof(long)); 
        result->num_tuples = 1;
        result->data_type = LONG;
        long col_sum = 0; 
        for(size_t i = 0; i < data_length; i++) {
            col_sum += data[i];
        }
        ((long*)result->payload)[0] = col_sum; 
    }
    else {
        result->payload = malloc(sizeof(double)); 
        result->num_tuples = 1;
        result->data_type = DOUBLE;
        double col_sum = 0; 
        for(size_t i = 0; i < data_length; i++) {
            col_sum += data[i];
        }
        ((double*)result->payload)[0] = col_sum / data_length; 
    }

    add_result_to_context(context, handle, result); 
}

void execute_sub_add(DbOperator* query, bool sub) {
    ClientContext* context = query->context;
    GeneralizedColumn* col1 = query->operator_fields.aggregate_operator.col1;
    GeneralizedColumn* col2 = query->operator_fields.aggregate_operator.col2; 
    char* handle = query->operator_fields.aggregate_operator.handle;

    int* data1 = NULL;
    int* data2 = NULL; 
    size_t data_length = 0;


    if(col1->column_type == COLUMN) {
        Column* vec1 = col1->column_pointer.column;
        data1 = vec1->data;
        data_length = vec1->table->table_length;
    }
    else {
        Result* vec1 = col1->column_pointer.result; 
        data1 = vec1->payload; 
        data_length = vec1->num_tuples;
    }

    if(col2->column_type == COLUMN) {
        Column* vec2 = col2->column_pointer.column;
        data2 = vec2->data;
    }
    else {
        Result* vec2 = col2->column_pointer.result; 
        data2 = vec2->payload; 
    }

    Result* result = (Result*) malloc(sizeof(Result));
    // TODO: maybe change to LONG?
    result->payload = malloc(sizeof(int) * data_length); 
    result->num_tuples = data_length;
    result->data_type = INT;

    if(sub) {
        for(size_t i = 0; i < data_length; i++) {
            ((int*)result->payload)[i] = data1[i] - data2[i];
        }
    }
    else {
        for(size_t i = 0; i < data_length; i++) {
            ((int*)result->payload)[i] = data1[i] + data2[i];
        }
    }

    add_result_to_context(context, handle, result); 
}

void execute_min_max(DbOperator* query, bool min) {
    ClientContext* context = query->context;
    GeneralizedColumn* col1 = query->operator_fields.aggregate_operator.col1;
    GeneralizedColumn* col2 = query->operator_fields.aggregate_operator.col2; 
    char* handle = query->operator_fields.aggregate_operator.handle;

    int* data1 = NULL;
    size_t data1_length = 0;
    int* data2 = NULL; 
    int result; 
    if(min) {
        result = INT_MAX;
    }
    else {
        result = INT_MIN;
    }

    if(col2 != NULL) {
        Result* vec1 = col1->column_pointer.result; 
        data1 = vec1->payload; 
        data1_length = vec1->num_tuples;
        
        if(col2->column_type == COLUMN) {
            Column* vec2 = col2->column_pointer.column;
            data2 = vec2->data;
        }
        else {
            Result* vec2 = col2->column_pointer.result; 
            data2 = vec2->payload; 
        }
        if(min) {
            for(size_t i = 0; i < data1_length; i++) {
                if(data2[data1[i]] < result) {
                    result = data2[data1[i]];
                }
            }
        }
        else {
            for(size_t i = 0; i < data1_length; i++) {
                if(data2[data1[i]] > result) {
                    result = data2[data1[i]];
                }
            }
        }
    
    }
    else {
        if(col1->column_type == COLUMN) {
            Column* vec1 = col1->column_pointer.column;
            data1 = vec1->data;
            data1_length = vec1->table->table_length;
        }
        else {
            Result* vec1 = col1->column_pointer.result; 
            data1 = vec1->payload; 
            data1_length = vec1->num_tuples;
        }
        if(min) {
            for(size_t i = 0; i < data1_length; i++) {
                if(data1[i] < result) {
                    result = data1[i];
                }
            }
        }
        else {
            for(size_t i = 0; i < data1_length; i++) {
                if(data1[i] > result) {
                    result = data1[i];
                }
            }
        }
    }

    // place in handle
    Result* result_vec = (Result*) malloc(sizeof(Result));
    result_vec->num_tuples = 1;
    result_vec->payload = malloc(sizeof(int));
    result_vec->data_type = INT;
    ((int*)result_vec->payload)[0] = result;

    add_result_to_context(context, handle, result_vec); 

}

void execute_aggregate(DbOperator* query) {
    
    switch(query->operator_fields.aggregate_operator.type) {
        case MIN:
            execute_min_max(query, true); 
            break;
        case MAX:
            execute_min_max(query, false);     
            break;
        case SUM:
            execute_sum_avg(query, true); 
            break;
        case AVG:
            execute_sum_avg(query, false); 
            break; 
        case SUB:
            execute_sub_add(query, true); 
            break; 
        case ADD:
            execute_sub_add(query, false); 
            break; 
    }
}
