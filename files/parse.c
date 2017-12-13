/* 
 * This file contains methods necessary to parse input from the client.
 * Mostly, functions in parse.c will take in string input and map these
 * strings into database operators. This will require checking that the
 * input from the client is in the correct format and maps to a valid
 * database operator.
 */

#define _BSD_SOURCE
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include <math.h>
#include <stdio.h>
#include <limits.h>
#include "include/cs165_api.h"
#include "include/parse.h"
#include "include/utils.h"
#include "include/client_context.h"

/**
 * Takes a pointer to a string.
 * This method returns the original string truncated to where its first comma lies.
 * In addition, the original string now points to the first character after that comma.
 * This method destroys its input.
 **/

char* next_token(char** tokenizer, message_status* status) {
    char* token = strsep(tokenizer, ",");
    if (token == NULL) {
        *status= INCORRECT_FORMAT;
    }
    return token;
}

int count_num_arguments(char* query) {
    int i, count;
    for (i=0, count=1; query[i]; i++)
        count += (query[i] == ',');
    return count;
}

void parse_open_close_parenthesis(char** query_command, message_status* status) {
    // check for leading '('
    if (strncmp(*query_command, "(", 1) == 0) {
        (*query_command)++;

        // read and chop off last char, which should be a ')'
        int last_char = strlen(*query_command) - 1;
        if ((*query_command)[last_char] != ')') {
            *status =  INCORRECT_FORMAT;
            return;
        }
        // replace the ')' with a null terminating character. 
        (*query_command)[last_char] = '\0';

    } else {
        *status = UNKNOWN_COMMAND;
        return;
    }
}

GeneralizedColumn* find_vec_by_name(char* name, ClientContext* context, bool look_only_in_context) {
    Result* result_vec = lookup_vec(context, name);
    if(result_vec != NULL) {
        GeneralizedColumn* ret = malloc(sizeof(GeneralizedColumn));
        ret->column_type = RESULT;
        ret->column_pointer.result = result_vec; 
        return ret;  
    }  
    else if(!look_only_in_context) {
        Column* col = lookup_column(name);
        if(col != NULL) {
            GeneralizedColumn* ret = malloc(sizeof(GeneralizedColumn));
            ret->column_type = COLUMN;
            ret->column_pointer.column = col; 
            return ret;
        }
    }
    return NULL;
}

/**
 * This method takes in a string representing the arguments to create a table.
 * It parses those arguments, checks that they are valid, and creates a table.
 **/

message_status parse_create_column(char* create_arguments) {
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* column_name  = next_token(create_arguments_index, &status);
    char* db_and_table_name = next_token(create_arguments_index, &status);
    char* db_name = strsep(&db_and_table_name, ".");
    char* table_name = db_and_table_name;

    // not enough arguments
    if (status == INCORRECT_FORMAT) {
        return status;
    }

    // Get the table name free of quotation marks
    column_name = trim_quotes(column_name);

    // read and chop off last char, which should be a ')'
    int last_char = strlen(table_name) - 1;
    if (table_name[last_char] != ')') {
        return INCORRECT_FORMAT;
    }
    // replace the ')' with a null terminating character. 
    table_name[last_char] = '\0';
    
    // check that the database argument is the current active database
    if (strcmp(current_db->name, db_name) != 0) {
        cs165_log(stdout, "query unsupported. Bad db name");
        return QUERY_UNSUPPORTED;
    }
    
    // lookup the table and make sure it exists. 
    Table* insert_table = lookup_table(table_name);
    if (insert_table == NULL) {
        return OBJECT_NOT_FOUND;
    }

    Status create_status;
    create_status.code = OK;
    create_status.error_message = NULL;
    create_column(column_name, insert_table, false, &create_status);
    if (create_status.code != OK) {
        cs165_log(stdout, "adding a column failed.");
        return EXECUTION_ERROR;
    }

    return status;
}

/**
 * This method takes in a string representing the arguments to create a table.
 * It parses those arguments, checks that they are valid, and creates a table.
 **/

message_status parse_create_tbl(char* create_arguments) {
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* table_name = next_token(create_arguments_index, &status);
    char* db_name = next_token(create_arguments_index, &status);
    char* col_cnt = next_token(create_arguments_index, &status);

    // not enough arguments
    if (status == INCORRECT_FORMAT) {
        return status;
    }

    // Get the table name free of quotation marks
    table_name = trim_quotes(table_name);

    // read and chop off last char, which should be a ')'
    int last_char = strlen(col_cnt) - 1;
    if (col_cnt[last_char] != ')') {
        return INCORRECT_FORMAT;
    }
    // replace the ')' with a null terminating character. 
    col_cnt[last_char] = '\0';
    
    // check that the database argument is the current active database
    if (strcmp(current_db->name, db_name) != 0) {
        cs165_log(stdout, "query unsupported. Bad db name");
        return QUERY_UNSUPPORTED;
    }

    // turn the string column count into an integer, and check that the input is valid.
    int column_cnt = atoi(col_cnt);
    if (column_cnt < 1) {
        return INCORRECT_FORMAT;
    }
    Status create_status;
    create_table(current_db, table_name, column_cnt, &create_status);
    if (create_status.code != OK) {
        cs165_log(stdout, "adding a table failed.");
        return EXECUTION_ERROR;
    }

    return status;
}

/**
 * This method takes in a string representing the arguments to create a database.
 * It parses those arguments, checks that they are valid, and creates a database.
 **/

message_status parse_create_db(char* create_arguments) {
    char *token;
    token = strsep(&create_arguments, ",");
    // not enough arguments if token is NULL
    if (token == NULL) {
        return INCORRECT_FORMAT;                    
    } else {
        // create the database with given name
        char* db_name = token;
        // trim quotes and check for finishing parenthesis.
        db_name = trim_quotes(db_name);
        int last_char = strlen(db_name) - 1;
        if (last_char < 0 || db_name[last_char] != ')') {
            return INCORRECT_FORMAT;
        }
        // replace final ')' with null-termination character.
        db_name[last_char] = '\0';

        token = strsep(&create_arguments, ",");
        if (token != NULL) {
            return INCORRECT_FORMAT;
        }
        if (add_db(db_name).code == OK) {
            return OK_DONE;
        } else {
            return EXECUTION_ERROR;
        }
    }
}

message_status parse_create_index(char* create_arguments) {
    message_status status = OK_DONE; 
    // read and chop off last char, which should be a ')'
    int last_char = strlen(create_arguments) - 1;
    if (create_arguments[last_char] != ')') {
        return INCORRECT_FORMAT;
    }
    // replace the ')' with a null terminating character. 
    create_arguments[last_char] = '\0';

    char* full_column_name = next_token(&create_arguments, &status);
    char* index_type = next_token(&create_arguments, &status);
    char* index_clustered = next_token(&create_arguments, &status);
        
    if (status == INCORRECT_FORMAT) {
        return status;
    }   

    Column* col = lookup_column(full_column_name);
    if(col == NULL) {
       return OBJECT_NOT_FOUND; 
    }

    col->index = malloc(sizeof(ColumnIndex));

    if(strcmp(index_clustered, "clustered") == 0) {
        col->clustered = true;
        col->table->index_column = lookup_column_index(col);
    }
    else if (strcmp(index_clustered, "unclustered") == 0) {
        col->clustered = false;
    }

    // check if sorted or Btree
    if(strcmp(index_type, "sorted") == 0) {
        col->index->type = SORTED;
        //TODO: If unclustered, maybe need to check if there is already data in the 
        // column and create the index now?
        col->index->index_fields.sorted_index.data = malloc(sizeof(int) * DEFAULT_TABLE_CAPACITY);
        col->index->index_fields.sorted_index.indices = malloc(sizeof(int) * DEFAULT_TABLE_CAPACITY);

        /* // if it is clustered we no longer need the data in the column, it is in the index */
        /* if(col->clustered) { */
        /*     free(col->data); */ 
        /* } */
    }
    else if (strcmp(index_type, "btree") == 0) {
        col->index->type = BTREE;
        col->index->index_fields.btree_index.btree_root = btree_create();
    }

    return OK_DONE; 
}

/**
 * parse_create parses a create statement and then passes the necessary arguments off to the next function
 **/
message_status parse_execute_create(char* create_arguments) {
    message_status mes_status = OK_WAIT_FOR_RESPONSE;
    char* tokenizer_copy = NULL;
    char* to_free = NULL;
    // Since strsep destroys input, we create a copy of our input. 
    tokenizer_copy = to_free = malloc((strlen(create_arguments)+1) * sizeof(char));
    char *token;
    strcpy(tokenizer_copy, create_arguments);
    // check for leading parenthesis after create. 
    if (strncmp(tokenizer_copy, "(", 1) == 0) {
        tokenizer_copy++;
        // token stores first argument. Tokenizer copy now points to just past first ","
        token = next_token(&tokenizer_copy, &mes_status);
        if (mes_status == INCORRECT_FORMAT) {
            return mes_status;
        } else {
            // pass off to next parse function. 
            if (strcmp(token, "db") == 0) {
                mes_status = parse_create_db(tokenizer_copy);
            } else if (strcmp(token, "tbl") == 0) {
                mes_status = parse_create_tbl(tokenizer_copy);
            } else if (strcmp(token, "col") == 0) {
                mes_status = parse_create_column(tokenizer_copy); 
            } else if (strcmp(token, "idx") == 0) {
                mes_status = parse_create_index(tokenizer_copy);
            } else {
                mes_status = UNKNOWN_COMMAND;
            }
        }
    } else {
        mes_status = UNKNOWN_COMMAND;
    }
    free(to_free);
    return mes_status;
}

DbOperator* parse_print(char* query_command, message* send_message, ClientContext* context) {
    parse_open_close_parenthesis(&query_command, &send_message->status); 
    if(send_message->status == INCORRECT_FORMAT || send_message->status == UNKNOWN_COMMAND) {
        return NULL;
    }
    
    int num_columns_to_print = count_num_arguments(query_command); 
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->context = context;
    dbo->type = PRINT; 
    dbo->operator_fields.print_operator.columns = malloc(sizeof(GeneralizedColumn*) * num_columns_to_print);

    for(int i = 0; i < num_columns_to_print; i++) {
        char* vec_name = strsep(&query_command, ",");  
        GeneralizedColumn* col = find_vec_by_name(vec_name, context, false); 
        if(col == NULL) {
            // TODO: remember to free array of GeneralizedColumn
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        dbo->operator_fields.print_operator.columns[i] = col;
    }

    dbo->operator_fields.print_operator.num_columns = (size_t)num_columns_to_print;

    return dbo;
}

DbOperator* parse_fetch(char* query_command, message* send_message, char* handle, ClientContext* context) {
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;

        char** command_index = &query_command;
        char* full_column_name = next_token(command_index, &send_message->status);
        strsep(&full_column_name, ".");
        char* table_name = strsep(&full_column_name, ".");
        char* column_name = full_column_name;
        char* vector_name = next_token(command_index, &send_message->status);
        
        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }
        
        // read and chop off last char, which should be a ')'
        int last_char = strlen(vector_name) - 1;
        if (vector_name[last_char] != ')') {
           send_message->status = INCORRECT_FORMAT;
        }
        // replace the ')' with a null terminating character. 
        vector_name[last_char] = '\0';

        Table* select_table = lookup_table(table_name);
        if (select_table == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        Column* select_column = lookup_column_in_table(select_table, column_name);
        if(select_column == NULL) {
            send_message->status = OBJECT_NOT_FOUND; 
            return NULL;
        }
        Result* pos_vec = lookup_vec(context, vector_name); 
        if(pos_vec == NULL) {
            send_message->status = OBJECT_NOT_FOUND; 
            return NULL; 
        }

        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = FETCH;
        dbo->context = context; 

        GeneralizedColumn* col1 = malloc(sizeof(GeneralizedColumn));
        col1->column_type = COLUMN; 
        col1->column_pointer.column = select_column; 
        dbo->operator_fields.fetch_operator.col1 = col1;

        GeneralizedColumn* col2 = malloc(sizeof(GeneralizedColumn));
        col2->column_type = RESULT;
        col2->column_pointer.result = pos_vec; 
        dbo->operator_fields.fetch_operator.col2 = col2; 
        
        strcpy(dbo->operator_fields.fetch_operator.handle, handle); 
        return dbo;

    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

DbOperator* parse_select(char* query_command, message* send_message, char* handle, ClientContext* context, DbOperator* operator) {
    parse_open_close_parenthesis(&query_command, &send_message->status); 
    if(send_message->status == INCORRECT_FORMAT || send_message->status == UNKNOWN_COMMAND) {
        return NULL;
    }
    int num_arguments = count_num_arguments(query_command);
    char** command_index = &query_command;
    char* full_column_name = NULL;
    char* vec_pos_name = NULL;
    char* low_value = NULL;
    char* high_value = NULL;
    GeneralizedColumn* col1 = NULL; 
    GeneralizedColumn* col2 = NULL; 
    if(num_arguments == 3) {
        full_column_name = next_token(command_index, &send_message->status);
        low_value = next_token(command_index, &send_message->status);
        high_value = next_token(command_index, &send_message->status);
        
        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }   

        col1 = find_vec_by_name(full_column_name, context, false); 
        if(col1 == NULL) {
            send_message->status = OBJECT_NOT_FOUND; 
            return NULL;
        }
    }
    else if (num_arguments == 4) {
        vec_pos_name = next_token(command_index, &send_message->status);
        full_column_name = next_token(command_index, &send_message->status);
        low_value = next_token(command_index, &send_message->status);
        high_value = next_token(command_index, &send_message->status);
        
        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }   

        col1 = find_vec_by_name(vec_pos_name, context, true); 
        if(col1 == NULL) {
            send_message->status = OBJECT_NOT_FOUND; 
            return NULL;
        }
        col2 = find_vec_by_name(full_column_name, context, false); 
        if(col2 == NULL) {
            free(col2);
            send_message->status = OBJECT_NOT_FOUND; 
            return NULL;
        }
    }

    Comparator* comparator = malloc(sizeof(Comparator));

    if(strcmp(low_value, "null") == 0) {
        comparator->p_low = -1;
        comparator->type1 = NO_COMPARISON;
    }
    else {
        comparator->p_low = atoi(low_value);
        comparator->type1 = GREATER_THAN_OR_EQUAL;
    }

    if(strcmp(high_value, "null") == 0) {
        comparator->p_high = -1;
        comparator->type2 = NO_COMPARISON;
    }
    else {
        comparator->p_high = atoi(high_value);
        comparator->type2 = LESS_THAN;
    }

    if (col2 == NULL) {
        comparator->gen_col = col1; 
        comparator->vec_pos = NULL;
    }
    else {
        comparator->gen_col = col2;
        comparator->vec_pos = col1;
    }
    strcpy(comparator->handle, handle);

    DbOperator* dbo = NULL;

    // in this case we are not batching select operators
    if(operator == NULL) {
        dbo = malloc(sizeof(DbOperator));
        dbo->type = SELECT;
        dbo->context = context; 
        dbo->operator_fields.select_operator.comparators = malloc(sizeof(Comparator*) * 1);
        dbo->operator_fields.select_operator.comparators[0] = comparator; 
        dbo->operator_fields.select_operator.comparators_capacity = 1;
        dbo->operator_fields.select_operator.comparators_length = 1; 
    }
    else {
        BatchOperator* batch_operator = &operator->operator_fields.batch_operator; 
        if(batch_operator->selects_length > 0 &&
           batch_operator->selects[batch_operator->selects_length - 1]->comparators_length < 
           batch_operator->selects[batch_operator->selects_length - 1]->comparators_capacity) {
            // in this case the last select operator can still hold more 
            // comparators for the shared scan
            SelectOperator* last_select = batch_operator->selects[batch_operator->selects_length - 1];
            last_select->comparators[last_select->comparators_length++] = comparator; 
        }
        // in this case we need to create a new select operator to insert to batch 
        else {
            SelectOperator* new_select = malloc(sizeof(SelectOperator));
            new_select->comparators = malloc(sizeof(Comparator*) * DEFAULT_MAX_SHARED_SCANS);
            new_select->comparators[0] = comparator;
            new_select->comparators_length = 1; 
            new_select->comparators_capacity = DEFAULT_MAX_SHARED_SCANS;
            batch_operator->selects[batch_operator->selects_length++] = new_select;
        }
    }

    return dbo;
}

/**
 * parse_insert reads in the arguments for a create statement and 
 * then passes these arguments to a database function to insert a row.
 **/

DbOperator* parse_insert(char* query_command, message* send_message) {
    unsigned int columns_inserted = 0;
    char* token = NULL;
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;
        // parse table input
        // changed because we lookup table by name and not database.table
        // char* table_name = next_token(command_index, &send_message->status);
        
        char* db_and_table_name = next_token(command_index, &send_message->status);
        strsep(&db_and_table_name, ".");
        char* table_name = db_and_table_name;

        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }

        // lookup the table and make sure it exists. 
        Table* insert_table = lookup_table(table_name);
        if (insert_table == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }

        // create insert operator
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = INSERT;
        dbo->operator_fields.insert_operator.table = insert_table;
        dbo->operator_fields.insert_operator.values = malloc(sizeof(int) * insert_table->col_count);

        // parse inputs until we reach the end. Turn each given string into an integer. 
        while ((token = strsep(command_index, ",")) != NULL) {
            int insert_val = atoi(token);
            dbo->operator_fields.insert_operator.values[columns_inserted++] = insert_val;
        }

        // check that we received the correct number of input values
        if (columns_inserted != insert_table->col_count) {
            send_message->status = INCORRECT_FORMAT;
            free(dbo);
            return NULL;
        } 
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

DbOperator* parse_sub_add(char* query_command, message* send_message, char* handle, ClientContext* context, bool sub) {
    parse_open_close_parenthesis(&query_command, &send_message->status); 
    if(send_message->status == INCORRECT_FORMAT || send_message->status == UNKNOWN_COMMAND) {
        return NULL;
    }

    char* col1_name = strsep(&query_command, ",");
    char* col2_name = query_command;

    if(col1_name == NULL || col2_name == NULL) {
        send_message->status = INCORRECT_FORMAT;
        return NULL;
    }

    GeneralizedColumn* col1 = NULL; 
    GeneralizedColumn* col2 = NULL; 
    col1 = find_vec_by_name(col1_name, context, false);
    if(col1 == NULL) {
        send_message->status = OBJECT_NOT_FOUND;
        return NULL;
    }
    col2 = find_vec_by_name(col2_name, context, false);
    if(col2 == NULL) {
        free(col1);
        send_message->status = OBJECT_NOT_FOUND;
        return NULL;
    }   

    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->context = context;
    dbo->type = AGGREGATE; 
    if(sub == true) {
        dbo->operator_fields.aggregate_operator.type = SUB;
    }
    else {
        dbo->operator_fields.aggregate_operator.type = ADD;
    }
    strcpy(dbo->operator_fields.aggregate_operator.handle, handle);
    dbo->operator_fields.aggregate_operator.col1 = col1;
    dbo->operator_fields.aggregate_operator.col2 = col2;
    return dbo;
}

DbOperator* parse_sum_avg(char* query_command, message* send_message, char* handle, ClientContext* context, bool sum) {
    parse_open_close_parenthesis(&query_command, &send_message->status); 
    if(send_message->status == INCORRECT_FORMAT || send_message->status == UNKNOWN_COMMAND) {
        return NULL;
    }

    char* col_name = query_command;
    GeneralizedColumn* col = find_vec_by_name(col_name, context, false);
    if(col == NULL) {
        send_message->status = OBJECT_NOT_FOUND;
        return NULL;
    }

    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->context = context;
    dbo->type = AGGREGATE; 
    if(sum == true) {
        dbo->operator_fields.aggregate_operator.type = SUM;
    }
    else {
        dbo->operator_fields.aggregate_operator.type = AVG;
    }
    strcpy(dbo->operator_fields.aggregate_operator.handle, handle);
    dbo->operator_fields.aggregate_operator.col1 = col;
    dbo->operator_fields.aggregate_operator.col2 = NULL;
    return dbo;
}

DbOperator* parse_min_max(char* query_command, message* send_message, char* handle, ClientContext* context, bool min) {
    parse_open_close_parenthesis(&query_command, &send_message->status); 
    if(send_message->status == INCORRECT_FORMAT || send_message->status == UNKNOWN_COMMAND) {
        return NULL;
    }

    char* col1_name = strsep(&query_command, ",");
    char* col2_name = query_command;

    GeneralizedColumn* col1 = NULL; 
    GeneralizedColumn* col2 = NULL; 
    if(col2_name == NULL) {
        col1 = find_vec_by_name(col1_name, context, false);
        if(col1 == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }
    }
    else if(strncmp(col1_name, "null", 4) == 0) {
        col1 = find_vec_by_name(col2_name, context, false);
        if(col1 == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }
    }
    else {
        col1 = find_vec_by_name(col1_name, context, true);
        if(col1 == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        col2 = find_vec_by_name(col2_name, context, false);
        if(col2 == NULL) {
            free(col1);
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }   
    }
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->context = context;
    dbo->type = AGGREGATE; 
    if(min == true) {
        dbo->operator_fields.aggregate_operator.type = MIN;
    }
    else {
        dbo->operator_fields.aggregate_operator.type = MAX;
    }
    strcpy(dbo->operator_fields.aggregate_operator.handle, handle);
    dbo->operator_fields.aggregate_operator.col1 = col1;
    dbo->operator_fields.aggregate_operator.col2 = col2;
    return dbo;
}

DbOperator* parse_batch_queries(char* query_command, message* send_message, ClientContext* context) {
    parse_open_close_parenthesis(&query_command, &send_message->status); 
    if(send_message->status == INCORRECT_FORMAT || send_message->status == UNKNOWN_COMMAND) {
        return NULL;
    }

    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->context = context;
    dbo->type = BATCH_QUERIES; 
    dbo->operator_fields.batch_operator.selects = malloc(sizeof(SelectOperator*) * DEFAULT_MAX_SELECTS_IN_BATCH);
    dbo->operator_fields.batch_operator.selects_length = 0;
    dbo->operator_fields.batch_operator.selects_capacity = DEFAULT_MAX_SELECTS_IN_BATCH;
    return dbo;
}

DbOperator* parse_join(char* query_command, message* send_message, char* handle, ClientContext* context) {
    parse_open_close_parenthesis(&query_command, &send_message->status); 
    if(send_message->status == INCORRECT_FORMAT || send_message->status == UNKNOWN_COMMAND) {
        return NULL;
    }

    char* values1 = strsep(&query_command, ",");
    char* positions1 = strsep(&query_command, ","); 
    char* values2 = strsep(&query_command, ",");
    char* positions2 = strsep(&query_command, ",");
    char* type = query_command;  
    
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->context = context;
    dbo->type = JOIN; 
    if(strcmp(type, "hash") == 0) {
        dbo->operator_fields.join_operator.type = HASH;
    }
    else if (strcmp(type, "nested-loop") == 0) {
        dbo->operator_fields.join_operator.type = NESTED;
    }
    dbo->operator_fields.join_operator.val_vec1 = lookup_vec(context, values1);
    dbo->operator_fields.join_operator.pos_vec1 = lookup_vec(context, positions1);
    dbo->operator_fields.join_operator.val_vec2 = lookup_vec(context, values2);
    dbo->operator_fields.join_operator.pos_vec2 = lookup_vec(context, positions2);
    char* handle1 = strsep(&handle, ",");
    char* handle2 = handle;
    strcpy(dbo->operator_fields.join_operator.handle1, handle1); 
    strcpy(dbo->operator_fields.join_operator.handle2, handle2); 
    return dbo;
}


DbOperator* parse_command(char* query_command, message* send_message, ClientContext* context, DbOperator* operator) {
    DbOperator* dbo = NULL;
    if (strncmp(query_command, "--", 2) == 0) {
        send_message->status = OK_DONE;
        // The -- signifies a comment line, no operator needed.  
        return NULL;
    }

    char *equals_pointer = strchr(query_command, '=');
    char *handle = query_command;
    if (equals_pointer != NULL) {
        // handle exists, store here. 
        *equals_pointer = '\0';
        query_command = ++equals_pointer;
    } else {
        handle = NULL;
    }

    /* cs165_log(stdout, "QUERY: %s\n", query_command); */

    send_message->status = OK_WAIT_FOR_RESPONSE;
    query_command = trim_whitespace(query_command);
    
    // check what command is given. 
    if (strncmp(query_command, "create", 6) == 0) {
        query_command += 6;
        send_message->status = parse_execute_create(query_command);
        dbo = malloc(sizeof(DbOperator));
        dbo->type = CREATE;
    } else if (strncmp(query_command, "relational_insert", 17) == 0) {
        query_command += 17;
        dbo = parse_insert(query_command, send_message);
    } else if (strncmp(query_command, "select", 6) == 0) {
        query_command += 6;
        dbo = parse_select(query_command, send_message, handle, context, operator);
    } else if (strncmp(query_command, "fetch", 5) == 0) {
        query_command += 5; 
        dbo = parse_fetch(query_command, send_message, handle, context);
    } else if (strncmp(query_command, "min", 3) == 0) {
        query_command += 3;    
        dbo = parse_min_max(query_command, send_message, handle, context, true); 
    } else if (strncmp(query_command, "max", 3) == 0) {
        query_command += 3;    
        dbo = parse_min_max(query_command, send_message, handle, context, false); 
    } else if (strncmp(query_command, "sum", 3) == 0) {
        query_command += 3;    
        dbo = parse_sum_avg(query_command, send_message, handle, context, true); 
    } else if (strncmp(query_command, "avg", 3) == 0) {
        query_command += 3;    
        dbo = parse_sum_avg(query_command, send_message, handle, context, false); 
    } else if (strncmp(query_command, "sub", 3) == 0) {
        query_command += 3;    
        dbo = parse_sub_add(query_command, send_message, handle, context, true); 
    } else if (strncmp(query_command, "add", 3) == 0) {
        query_command += 3;    
        dbo = parse_sub_add(query_command, send_message, handle, context, false); 
    } else if (strncmp(query_command, "shutdown", 8) == 0) {
        dbo = malloc(sizeof(DbOperator));
        dbo->type = SHUTDOWN;
        dbo->context = context;
    } else if (strncmp(query_command, "print", 5) == 0) {
        query_command += 5; 
        dbo = parse_print(query_command, send_message, context);
    } else if (strncmp(query_command, "batch_queries", 13) == 0) {
        query_command += 13; 
        dbo = parse_batch_queries(query_command, send_message, context);
    } else if (strncmp(query_command, "batch_execute", 13) == 0) {
        query_command += 13; 
        dbo = operator;
    } else if (strncmp(query_command, "join", 4) == 0) {
        query_command += 4; 
        dbo = parse_join(query_command, send_message, handle, context); 
    }
    return dbo;
}
