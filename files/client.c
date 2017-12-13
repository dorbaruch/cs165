/* This line at the top is necessary for compilation on the lab machine and many other Unix machines.
Please look up _XOPEN_SOURCE for more details. As well, if your code does not compile on the lab
machine please look into this as a a source of error. */
#define _XOPEN_SOURCE

/**
 * client.c
 *  CS165 Fall 2015
 *
 * This file provides a basic unix socket implementation for a client
 * used in an interactive client-server database.
 * The client receives input from stdin and sends it to the server.
 * No pre-processing is done on the client-side.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/stat.h>


#include "include/common.h"
#include "include/message.h"
#include "include/utils.h"

#define DEFAULT_STDIN_BUFFER_SIZE 1024

/**
 * connect_client()
 *
 * This sets up the connection on the client side using unix sockets.
 * Returns a valid client socket fd on success, else -1 on failure.
 *
 **/
int connect_client() {
    int client_socket;
    size_t len;
    struct sockaddr_un remote;

    log_info("Attempting to connect...\n");

    if ((client_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    remote.sun_family = AF_UNIX;
    strncpy(remote.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    len = strlen(remote.sun_path) + sizeof(remote.sun_family) + 1;
    if (connect(client_socket, (struct sockaddr *)&remote, len) == -1) {
        log_err("client connect failed: ");
        return -1;
    }

    log_info("Client connected at socket: %d.\n", client_socket);
    return client_socket;
}

void send_message_to_socket(int client_socket, message* send_message) {
    // Send the message_header, which tells server payload size
    if (send(client_socket, send_message, sizeof(message), 0) == -1) {
        log_err("Failed to send message header.");
        exit(1);
    }

    // Send the payload (query) to server
    if (send(client_socket, send_message->payload, send_message->length, 0) == -1) {
        log_err("Failed to send query payload.");
        exit(1);
    }
}

int receive_message_from_socket(int client_socket, message* recv_message) {
        
    int len = 0;
    // Always wait for server response (even if it is just an OK message)
    if ((len = recv(client_socket, recv_message, sizeof(message), 0)) > 0) {
        if(recv_message->length == 0) {
            return 0;
        }
        if ((recv_message->status == OK_WAIT_FOR_RESPONSE || recv_message->status == OK_DONE) &&
            (int) recv_message->length > 0) {
            // Calculate number of bytes in response package
            int num_bytes = (int) recv_message->length;
            char payload[num_bytes + 1];

            // Receive the payload and print it out
            if ((len = recv(client_socket, payload, num_bytes, 0)) > 0) {
                payload[num_bytes] = '\0';
                printf("%s\n", payload);
            }
        }
        return 1;
    }
    else {
        if (len < 0) {
            log_err("Failed to receive message.");
            exit(1);
        }
        else {
            log_info("Server closed connection\n");
            exit(0);
        }
    }
}

void execute_load(int client_socket, char* path_name) {
    int buffer_size = 4096;
    FILE *f = fopen(path_name, "rb");
    // TODO: define better size
    char buffer[buffer_size];
    message send_message; 
    send_message.payload = buffer;
    message recv_message; 
    send_message.status = OK_DONE; 
    int n; 

    // send general load message to let server know it is going to start
    // receiving data
    struct stat st;
    stat(path_name, &st);
    int size = st.st_size;
    sprintf(buffer, "%d", size);
    send_message.length = strlen(buffer); 
    send_message_to_socket(client_socket, &send_message);
    receive_message_from_socket(client_socket, &recv_message);
    
    while (!feof(f)) {
        n = fread(buffer, 1, buffer_size, f);
        send_message.length = n;
        send_message_to_socket(client_socket, &send_message); 
        receive_message_from_socket(client_socket, &recv_message);
    }
    send_message.length = 0; 
    send_message.payload = ""; 
    send_message_to_socket(client_socket, &send_message);
    receive_message_from_socket(client_socket, &recv_message); 
}

void parse_load_and_send_data(int client_socket, char* query) {
    char* query_command = query + 4; 
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;

        query_command = trim_newline(query_command);

        // read and chop off last char, which should be a ')'
        int last_char = strlen(query_command) - 1;
        if (query_command[last_char] != ')') {
            /* send_message->status =  INCORRECT_FORMAT; */
            return;
        }

        // replace the ')' with a null terminating character. 
        query_command[last_char] = '\0';

        char* full_path = trim_quotes(query_command);

        execute_load(client_socket, full_path);

    } else {
        /* send_message->status = UNKNOWN_COMMAND; */
        return;
    }
}

void receive_data_to_print(int client_socket) {
    message send_message; 
    send_message.payload = "Success";
    send_message.length = 7;
    message recv_message; 
    int len = 0;

    // place 0 for number of columns to prine, place 1 for if we have more than one row
    int print_meta[2]; 
    len = recv(client_socket, &recv_message, sizeof(message), 0);
    len = recv(client_socket, &print_meta, sizeof(int) * 2, 0); 
    send_message_to_socket(client_socket, &send_message);
    int num_columns = print_meta[0];
    int more_than_one = print_meta[1];

    
    // in this case we only print one row, can be any type for each column
    if(!more_than_one) {
        // get column types
        int payload_types[num_columns]; 
        len = recv(client_socket, &recv_message, sizeof(message), 0);
        len = recv(client_socket, &payload_types, sizeof(int) * num_columns, 0); 
        send_message_to_socket(client_socket, &send_message);

        for(int j = 0; j < num_columns; j++) {
            switch(payload_types[j]) {
                case 0:
                {
                    len = recv(client_socket, &recv_message, sizeof(message), 0);
                    int num_bytes = (int) recv_message.length;
                    int print_length = num_bytes / sizeof(int); 
                    int payload[print_length];

                    if ((len = recv(client_socket, payload, num_bytes, 0)) > 0) {
                        for(int i = 0; i < print_length; i++) {
                            if(j == num_columns - 1) {
                                printf("%d", payload[i]);
                            }
                            else {
                                printf("%d,", payload[i]);
                            }
                        }
                    }
                    send_message_to_socket(client_socket, &send_message);
                    break;
                }
                case 1: 
                {
                    len = recv(client_socket, &recv_message, sizeof(message), 0);
                    int num_bytes = (int) recv_message.length;
                    int print_length = num_bytes / sizeof(long); 
                    long payload[print_length];

                    if(print_length == 0) {
                        return;
                    }

                    if ((len = recv(client_socket, payload, num_bytes, 0)) > 0) {
                        for(int i = 0; i < print_length; i++) {
                            if(j == num_columns - 1) {
                                printf("%ld", payload[i]);
                            }
                            else {
                                printf("%ld,", payload[i]);
                            }
                        }
                    }
                    send_message_to_socket(client_socket, &send_message);
                    break;
                }
                case 2:
                {
                    len = recv(client_socket, &recv_message, sizeof(message), 0);
                    // Calculate number of bytes in response package
                    int num_bytes = (int) recv_message.length;
                    int print_length = num_bytes / sizeof(double); 
                    double payload[print_length];

                    // Receive the payload and print it out
                    if ((len = recv(client_socket, payload, num_bytes, 0)) > 0) {
                        for(int i = 0; i < print_length; i++) {
                            if(j == num_columns - 1) {
                                printf("%.2f", payload[i]);
                            }
                            else {
                                printf("%.2f,", payload[i]);
                            }
                        }
                    }
                    send_message_to_socket(client_socket, &send_message);
                }
                break;
            }
        }
        printf("\n");
    }
    // we have multiple row to print, it must be columns of ints
    else {
        while ((len = recv(client_socket, &recv_message, sizeof(message), 0)) > 0) {
            // Calculate number of bytes in response package
            int num_bytes = (int) recv_message.length;
            if(num_bytes == -1) {
                break;
            }
            int print_length = num_bytes / sizeof(int) / num_columns; 
            int* payload = malloc(num_bytes);

            // Receive the payload and print it out
            if ((len = recv(client_socket, payload, num_bytes, 0)) > 0) {
                for(int i = 0; i < print_length; i++) {
                    for(int j = 0; j < num_columns - 1; j++) {
                        printf("%d,", payload[i + (j * print_length)]);
                    }
                    printf("%d\n", payload[i + ((num_columns - 1) * print_length)]);
                }
            }
            free(payload);
            send_message_to_socket(client_socket, &send_message);
        }
        // receive the empty message that indicates end of print */
    }
    /* recv(client_socket, &recv_message, sizeof(message), 0); */
}

int main(void)
{
    int client_socket = connect_client();
    if (client_socket < 0) {
        exit(1);
    }

    message send_message;
    message recv_message;

    // Always output an interactive marker at the start of each command if the
    // input is from stdin. Do not output if piped in from file or from other fd
    char* prefix = "";
    if (isatty(fileno(stdin))) {
        prefix = "db_client > ";
    }

    char *output_str = NULL;

    // Continuously loop and wait for input. At each iteration:
    // 1. output interactive marker
    // 2. read from stdin until eof.
    char read_buffer[DEFAULT_STDIN_BUFFER_SIZE];
    send_message.payload = read_buffer;

    while (printf("%s", prefix), output_str = fgets(read_buffer,
           DEFAULT_STDIN_BUFFER_SIZE, stdin), !feof(stdin)) {
        if (output_str == NULL) {
            log_err("fgets failed.\n");
            break;
        }

        // Only process input that is greater than 1 character.
        // Convert to message and send the message and the
        // payload directly to the server.
        send_message.length = strlen(read_buffer);
        if (send_message.length > 1) {
            if(send_message.length > 4 && strncmp(read_buffer, "load", 4) == 0) {
                send_message_to_socket(client_socket, &send_message); 
                receive_message_from_socket(client_socket, &recv_message); 
                parse_load_and_send_data(client_socket, send_message.payload); 
                continue; 
            }
            if(send_message.length > 4 && strncmp(read_buffer, "print", 5) == 0) {
               send_message_to_socket(client_socket, &send_message);  
               receive_data_to_print(client_socket);
               continue;
            }
            send_message_to_socket(client_socket, &send_message); 
            receive_message_from_socket(client_socket, &recv_message); 

        }
    }
    close(client_socket);
    return 0;
}
