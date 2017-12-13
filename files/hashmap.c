#include "include/hashmap.h"
#include "include/murmurhash.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

size_t hashmap_get_index(int key) {
    size_t pos_num = (size_t)key; 
    return pos_num % HASHMAP_SIZE; 
}


Hashmap* hashmap_create() {
    // allocate new hashmap struct
    Hashmap* new_hashmap = malloc(sizeof(Hashmap));
    if(!new_hashmap) {
        return NULL;
    }
    for(int i = 0; i < HASHMAP_SIZE; i++) {
        new_hashmap->table[i].next_node = NULL;
    }
    return new_hashmap; 
}

int hashmap_get(Hashmap* map, int key) {
    size_t index = hashmap_get_index(key); 
    HashmapNode* node = &map->table[index]; 

    while(node != NULL) {
        for(size_t i = 0; i < node->num_keys; i++) {
            if(key == node->keys[i]) {
                return node->positions[i];   
            } 
        }
        node = node->next_node; 
    }

    return -1;
}

void hashmap_put(Hashmap* map, int key, int pos) {
    size_t index = hashmap_get_index(key);
    HashmapNode* node = &map->table[index];
    if(node->num_keys == HASHMAP_NODE_SIZE) {
        HashmapNode* new_node = hashmap_node_create(); 
        new_node->keys[0] = key;
        new_node->positions[0] = pos;  
        new_node->num_keys++;
        node->next_node = new_node;
    }
    else {
        node->keys[node->num_keys] = key;
        node->positions[node->num_keys] = pos; 
        node->num_keys++; 
    }
}

HashmapNode* hashmap_node_create() {
    HashmapNode* new_hashmap_node = malloc(sizeof(HashmapNode));
    if(!new_hashmap_node) {
        return NULL;
    }

    new_hashmap_node->num_keys = 0;
    new_hashmap_node = NULL;
    return new_hashmap_node;
}






