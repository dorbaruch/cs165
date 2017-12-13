#define _BSD_SOURCE
#include <stdio.h>
#include <string.h>

#include "include/client_context.h"
#include "include/utils.h"
/* This is an example of a function you will need to
 * implement in your catalogue. It takes in a string (char *)
 * and outputs a pointer to a table object. Similar methods
 * will be needed for columns and databases. How you choose
 * to implement the method is up to you.
 */
Table* lookup_table(char *name) {
    for(unsigned int i = 0; i < current_db->tables_size; i++) {
        if(strcmp((char*)current_db->tables[i]->name, name) == 0) {
            return current_db->tables[i];
        } 
    }
	return NULL;
}

Column* lookup_column_in_table(Table* table, char* name) {
    for(unsigned int i = 0; i < table->col_count; i++) {
        if(strcmp((char*)table->columns[i]->name, name) == 0) {
            return table->columns[i]; 
        }
    } 
    return NULL; 
}

Result* lookup_vec(ClientContext* context, char* vector_name) {
    for(int i = 0; i < context->chandles_in_use; i++) {
        if(strcmp((char*)context->chandle_table[i].name, vector_name) == 0) {
            return context->chandle_table[i].result;
        }
    }
    return NULL;
}

Column* lookup_column(char* full_column_name) {
    strsep(&full_column_name, ".");
    char* table_name = strsep(&full_column_name, ".");
    char* column_name = full_column_name; 

    Table* table = lookup_table(table_name); 
    Column* column = lookup_column_in_table(table, column_name); 
    return column;
}

size_t lookup_column_index(Column* col) {
    for(size_t i = 0; i < col->table->col_count; i++) {
        if(strcmp((char*)col->table->columns[i]->name, col->name) == 0) {
            return i; 
        }
    } 
    return -1; 
}

void add_result_to_context(ClientContext* context, char* handle, Result* result) {
    // lock context
    pthread_mutex_lock(&context->mutex);
    // look for the handle in the client context
    for(int i = 0; i < context->chandles_in_use; i++) {
        if(strcmp(context->chandle_table[i].name, handle) == 0) {
            free_result(context->chandle_table[i].result);
            context->chandle_table[i].result = result; 
            pthread_mutex_unlock(&context->mutex);
            return;
        }
    }
    // If the handle was not found, add it
    // TODO: What happens when in use equals slots in context
    strcpy(context->chandle_table[context->chandles_in_use].name, handle);
    context->chandle_table[context->chandles_in_use].name[strlen(handle)] = '\0';
    context->chandle_table[context->chandles_in_use++].result = result; 
    if(context->chandles_in_use == context->chandle_slots) {
        context->chandle_slots = context->chandle_slots * 2; 
        ResultHandle* new_chandle_table = (ResultHandle*) malloc(sizeof(ResultHandle) * context->chandle_slots);
        for(int i = 0; i < context->chandles_in_use; i++) {
            new_chandle_table[i] = context->chandle_table[i];
        }
        free(context->chandle_table);
        context->chandle_table = new_chandle_table;
    }
    pthread_mutex_unlock(&context->mutex);
}

