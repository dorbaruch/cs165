/** server.c
 * CS165 Fall 2015
 *
 * This file provides a basic unix socket implementation for a server
 * used in an interactive client-server database.
 * The client should be able to send messages containing queries to the
 * server.  When the server receives a message, it must:
 * 1. Respond with a status based on the query (OK, UNKNOWN_QUERY, etc.)
 * 2. Process any appropriate queries, if applicable.
 * 3. Return the query response to the client.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>

#include "include/common.h"
#include "include/parse.h"
#include "include/cs165_api.h"
#include "include/message.h"
#include "include/utils.h"
#include "include/client_context.h"

#define DEFAULT_QUERY_BUFFER_SIZE 1024

int* shutdown_flag;


void send_message_to_socket(int client_socket, message* send_message) {
    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    if (send(client_socket, send_message, sizeof(message), 0) == -1) {
        log_err("Failed to send message.");
        exit(1);
    }

    if(send_message->length > 0) {
        // 4. Send response of request
        if (send(client_socket, send_message->payload, send_message->length, 0) == -1) {
            log_err("Failed to send message.");
            exit(1);
        }
    }
}

int receive_message_from_socket(int client_socket, message* recv_message) {
    int length = recv(client_socket, recv_message, sizeof(message), 0);
    if (length < 0) {
        log_err("Client connection closed!\n");
        exit(1);
    } else if (length == 0) {
        return 0;
    }

    if(recv_message->length == 0) {
        recv_message->payload = NULL;
        return 0;
    }
    recv_message->payload = malloc(recv_message->length + 1);
    length = recv(client_socket, recv_message->payload, recv_message->length, 0);
    recv_message->payload[recv_message->length] = '\0';
    return 1; 
}

size_t get_generalized_column_length(GeneralizedColumn* column) {
    if(column->column_type == COLUMN) {
        return column->column_pointer.column->table->table_length;
    }
    else {
        return column->column_pointer.result->num_tuples;
    }
}

void execute_print(DbOperator* query, message* send_message, message* recv_message, int client_socket) {
    size_t num_columns = query->operator_fields.print_operator.num_columns;
    GeneralizedColumn** columns = query->operator_fields.print_operator.columns;
    size_t num_tuples = get_generalized_column_length(columns[0]);
    
    //transmit print meta
    int print_meta[2];
    print_meta[0] = num_columns;
    if(num_tuples == 1) {
        print_meta[1] = 0;
    }
    else {
        print_meta[1] = 1; 
    }
    send_message->payload = (char*)print_meta;
    send_message->length = sizeof(int) * 2;  
    send_message_to_socket(client_socket, send_message);
    receive_message_from_socket(client_socket, recv_message);
    free(recv_message->payload); 

    // when we only print one row, each column can have a different type
    if(num_tuples == 1) {
        int column_types[num_columns];
        int type_sizes[num_columns];
        char* buffers[num_columns];
        for(size_t i = 0; i < num_columns; i++) {
            GeneralizedColumn* cur_col = columns[i];

            if(cur_col->column_type == COLUMN) {
                column_types[i] = 0;
                type_sizes[i] = sizeof(int);
                buffers[i] = malloc(sizeof(int));        
            }
            else {
                Result* res_col = cur_col->column_pointer.result;
                switch(res_col->data_type) {
                    case INT:
                        column_types[i] = 0;
                        type_sizes[i] = sizeof(int);
                        buffers[i] = malloc(sizeof(int));        
                        break;
                    case LONG:
                        column_types[i] = 1;
                        type_sizes[i] = sizeof(long);
                        buffers[i] = malloc(sizeof(long));        
                        break;
                    case DOUBLE:
                        column_types[i] = 2;
                        type_sizes[i] = sizeof(double);
                        buffers[i] = malloc(sizeof(double)); 
                        break;
                }
                memcpy(buffers[i], (char*)res_col->payload, type_sizes[i]);
            }
        }
            
        // trasnmit data types
        send_message->payload = (char*)column_types;
        send_message->length = sizeof(int) * num_columns;  
        send_message_to_socket(client_socket, send_message);
        receive_message_from_socket(client_socket, recv_message);
        free(recv_message->payload);
        
        if(num_columns == 1 && ((column_types[0] == 0 && ((int*)buffers[0])[0] == 0) || 
                    (column_types[0] == 1 && ((long*)buffers[0])[0] == 0) ||
                    (column_types[0] == 2 && ((double*)buffers[0])[0] == 0))) {
            send_message->payload = "";
            send_message->length = 0;
            send_message_to_socket(client_socket, send_message);
            receive_message_from_socket(client_socket, recv_message);
        }       
        else {
            // transmit data
            for(size_t i = 0; i < num_columns; i++) {
                send_message->payload = buffers[i];
                send_message->length = type_sizes[i];
                send_message_to_socket(client_socket, send_message);
                receive_message_from_socket(client_socket, recv_message);
                free(recv_message->payload);
            }
        }
        for(size_t i = 0; i < num_columns; i++) {
            free(buffers[i]);
        }
    }
    else {
        int* data[num_columns];
        for(size_t i = 0; i < num_columns; i++) {
            GeneralizedColumn* cur_col = columns[i];
            if(cur_col->column_type == COLUMN) {
                data[i] = cur_col->column_pointer.column->data;
            }
            else {
                data[i] = (int*)cur_col->column_pointer.result->payload; 
            }
        }

        int num_tuples_per_buffer = 512;
        int column_buffer_size = num_tuples_per_buffer * sizeof(int);
        int total_buffer_size = column_buffer_size * num_columns;
        char buffer[total_buffer_size];
        send_message->payload = buffer;
        send_message->length = total_buffer_size;
        int tuples_sent = 0;
        
        for(size_t i = 0; i + num_tuples_per_buffer < num_tuples; i += num_tuples_per_buffer) {
            for(size_t j = 0; j < num_columns; j++) {
                memcpy(buffer + (j * column_buffer_size), &(data[j][i]), column_buffer_size);
            }
            tuples_sent += num_tuples_per_buffer;

            send_message_to_socket(client_socket, send_message);
            receive_message_from_socket(client_socket, recv_message);
            free(recv_message->payload);
        }

        int tuples_left = num_tuples - tuples_sent;
        num_tuples_per_buffer = tuples_left;
        column_buffer_size = num_tuples_per_buffer * sizeof(int);
        total_buffer_size = column_buffer_size * num_columns;
        char new_buffer[total_buffer_size];
        send_message->payload = new_buffer;
        send_message->length = total_buffer_size;
        for(size_t j = 0; j < num_columns; j++) {
            memcpy(new_buffer + (j * column_buffer_size), &(data[j][tuples_sent]), column_buffer_size);
        }

        send_message_to_socket(client_socket, send_message);
        receive_message_from_socket(client_socket, recv_message);
        free(recv_message->payload);

        // transmit end print
        send_message->payload = "";
        send_message->length = -1; 
        /* send_message_to_socket(client_socket, send_message); */ 
        if (send(client_socket, send_message, sizeof(message), 0) == -1) {
            log_err("Failed to send message.");
            exit(1);
        }
    }
}

/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
int handle_client(int client_socket) {
    int done = 0;
    int shutdown = 0;

    log_info("Connected to socket: %d.\n", client_socket);

    // Create two messages, one from which to read and one from which to receive
    message send_message;
    send_message.payload = NULL;
    message recv_message;
    recv_message.payload = NULL;

    // create the client context here
    ClientContext* client_context = (ClientContext*) malloc(sizeof(ClientContext));
    client_context->chandle_slots = DEFAULT_CLIENT_HANDLES;
    client_context->chandles_in_use = 0; 
    client_context->chandle_table = (ResultHandle*) malloc(sizeof(ResultHandle) * client_context->chandle_slots);
    if (pthread_mutex_init(&client_context->mutex, NULL) != 0) {
        printf("\n mutex init failed\n");
        return 1;
    }
    

    // Continually receive messages from client and execute queries.
    // 1. Parse the command
    // 2. Handle request if appropriate
    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    // 4. Send response of request.
    do {
        if(receive_message_from_socket(client_socket, &recv_message)) {
            if(recv_message.length > 4 && strncmp(recv_message.payload, "load", 4) == 0) {
                free(recv_message.payload);
                char* result = ""; 
                send_message.length = strlen(result);
                send_message.payload = malloc(send_message.length + 1);
                strcpy(send_message.payload, result);
                send_message_to_socket(client_socket, &send_message); 

                // get size of file
                receive_message_from_socket(client_socket, &recv_message);
                send_message_to_socket(client_socket, &send_message);
                int load_file_size = atoi(recv_message.payload);   
                free(recv_message.payload);
                char* buffer = malloc(load_file_size);
                // get file data
                int num_read = 0;
                while(receive_message_from_socket(client_socket, &recv_message)) {
                    memcpy(buffer + num_read, recv_message.payload, recv_message.length);
                    num_read += recv_message.length; 
                    free(recv_message.payload);
                    send_message_to_socket(client_socket, &send_message);
                }
                execute_load(buffer);
                send_message_to_socket(client_socket, &send_message);
                free(buffer);
                free(send_message.payload);
                continue;
            }

            // 1. Parse command
            DbOperator* query = parse_command(recv_message.payload, &send_message, client_context, NULL);
            free(recv_message.payload);
            if(query != NULL && query->type == PRINT) {
                execute_print(query, &send_message, &recv_message, client_socket);
                free_db_operator(query);
                continue;
            }
            else if(query != NULL && query->type == SHUTDOWN) {
                done = 1; 
                shutdown = 1;
            }
            else if(query != NULL && query->type == BATCH_QUERIES) {
                char* empty = "";
                send_message.length = strlen(empty);
                send_message.payload = malloc(send_message.length + 1);
                strcpy(send_message.payload, empty);
                send_message_to_socket(client_socket, &send_message);
                DbOperator* batch_query = NULL;
                do {
                    receive_message_from_socket(client_socket, &recv_message);
                    batch_query = parse_command(recv_message.payload, &send_message, client_context, query);
                    free(recv_message.payload);

                    if(batch_query == NULL) {
                        send_message.length = strlen(empty);
                        send_message.payload = malloc(send_message.length + 1);
                        strcpy(send_message.payload, empty);
                        send_message_to_socket(client_socket, &send_message);
                        free(send_message.payload);
                    }
                } while(batch_query == NULL);
            }

            char* result = execute_db_operator(query);  
            if(result == NULL) {
                result = ""; 
            }
            send_message.length = strlen(result);
            send_message.payload = malloc(send_message.length + 1);
            strcpy(send_message.payload, result);
            send_message_to_socket(client_socket, &send_message);
            free(send_message.payload);
        }
        else {
            done = 1;
        }
    } while (!done);
    log_info("Connection closed at socket %d!\n", client_socket);
    close(client_socket);
    return shutdown;
}

/**
 * setup_server()
 *
 * This sets up the connection on the server side using unix sockets.
 * Returns a valid server socket fd on success, else -1 on failure.
 **/
int setup_server() {
    int server_socket;
    size_t len;
    struct sockaddr_un local;

    log_info("Attempting to setup server...\n");

    if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    local.sun_family = AF_UNIX;
    strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    unlink(local.sun_path);

    /*
    int on = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
    {
        log_err("L%d: Failed to set socket as reusable.\n", __LINE__);
        return -1;
    }
    */

    len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
    if (bind(server_socket, (struct sockaddr *)&local, len) == -1) {
        log_err("L%d: Socket failed to bind.\n", __LINE__);
        return -1;
    }

    if (listen(server_socket, 5) == -1) {
        log_err("L%d: Failed to listen on socket.\n", __LINE__);
        return -1;
    }

    return server_socket;
}

// Currently this main will setup the socket and accept a single client.
// After handling the client, it will exit.
// You will need to extend this to handle multiple concurrent clients
// and remain running until it receives a shut-down command.
int main(void)
{
    /* *shutdown_flag = 0; */
    int server_socket = setup_server();
    if (server_socket < 0) {
        exit(1);
    }
    
    // QUESTION: initializing the database here??
    db_startup();
    int shutdown_flag = 0; 
    while(!shutdown_flag) {
        log_info("Waiting for a connection %d ...\n", server_socket);

        struct sockaddr_un remote;
        socklen_t t = sizeof(remote);
        int client_socket = 0;

        if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1) {
            log_err("L%d: Failed to accept a new connection.\n", __LINE__);
            exit(1);
        }
         
        clock_t start, end;
        double cpu_time_used;
        start = clock();
        shutdown_flag = handle_client(client_socket);
        end = clock(); 
        cpu_time_used = ((double) (end-start)) / CLOCKS_PER_SEC;
        /* log_info("Batched queries took %f seconds.\n", cpu_time_used); */
        cs165_log(stdout, "Total queries took %f seconds. \n", cpu_time_used);
    }
    exit(0);
}

