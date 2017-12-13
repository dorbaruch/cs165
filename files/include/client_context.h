#ifndef CLIENT_CONTEXT_H
#define CLIENT_CONTEXT_H

#include "cs165_api.h"

Table* lookup_table(char *name);

Column* lookup_column_in_table(Table* table, char* name); 

Column* lookup_column(char* name); 

Result* lookup_vec(ClientContext* context, char* vector_name); 

size_t lookup_column_index(Column* col);

void add_result_to_context(ClientContext* context, char* handle, Result* result);
#endif
