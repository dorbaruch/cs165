#ifndef HASHMAP_H
#define HASHMAP_H

#include <stdio.h>
#define HASHMAP_SIZE 4096
#define HASHMAP_NODE_SIZE 1024


typedef struct HashmapNode {
    int keys[HASHMAP_NODE_SIZE]; 
    int positions[HASHMAP_NODE_SIZE]; 
    struct HashmapNode* next_node;  
    size_t num_keys; 
} HashmapNode;

typedef struct Hashmap {
    HashmapNode table[HASHMAP_SIZE]; 
} Hashmap;


/* hashmap_fatnode* hashmap_node_create() */

// get the index into the hashmap array for the input key
size_t hashmap_get_index(int key); 

Hashmap* hashmap_create();

HashmapNode* hashmap_node_create();

int hashmap_get(Hashmap* map, int key);

void hashmap_put(Hashmap* map, int key, int data);

#endif

