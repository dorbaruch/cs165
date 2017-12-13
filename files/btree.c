#define _BSD_SOURCE
#include <stdio.h>
#include <string.h>
#include "include/cs165_api.h"

BtreeNode* btree_create() {
    BtreeNode* root = malloc(sizeof(BtreeNode)); 
    root->is_leaf = true; 
    root->num_keys = 0;
    root->data.leaf_data.next_leaf = NULL;
    return root;
}

void free_btree(BtreeNode* root) {
    if(!root->is_leaf) {
        for(int i = 0; i < root->num_keys; i++) {
            free_btree(root->data.internal_data.children[i]); 
        }
    }
    free(root); 
}

void get_btree_values(BtreeIndex* index, int* ret) {
    // go to left most leaf node
    BtreeNode* cur_node = index->btree_root; 
    while(!cur_node->is_leaf) {
        cur_node = cur_node->data.internal_data.children[0]; 
    }
    int i; 
    int cur_pos = 0; 
    while(cur_node != NULL) {
        for(i = 0; i < cur_node->num_keys; i++) {
            ret[cur_pos] = cur_node->data.leaf_data.data[i];
            cur_pos++; 
        }     
        cur_node = cur_node->data.leaf_data.next_leaf;
    }
}

int find_key_pos_in_node(BtreeNode* node, int val) {
    int* data;
    int num_keys = node->num_keys; 
    if(node->is_leaf) {
        data = node->data.leaf_data.data; 
    }
    else {
        data = node->data.internal_data.keys; 
    }
    int pos; 
    for(pos = 0; pos < num_keys; pos++) {
        if(data[pos] >= val) {
            return pos; 
        }
    }
    // if we didn't find a key that is greater than the one we're looking for
    // it means we need to go to the right most pointer
    return pos; 
}

BtreeNode* get_leaf_node(BtreeIndex* index, int val) {
    BtreeNode* cur_node = index->btree_root;
    int key_pos= -1; 
    while(!cur_node->is_leaf) {
        key_pos = find_key_pos_in_node(cur_node, val);
        cur_node = cur_node->data.internal_data.children[key_pos];
    }
    return cur_node;
}

void select_from_btree_index(BtreeIndex* index, Comparator* comparator, Result* result) {
    int p_low = comparator->p_low;
    int p_high = comparator->p_high;
    // now cur_node is pointing to a leaf node where the value exists
    BtreeNode* cur_node = get_leaf_node(index, p_low);
    int i;
    int* data;
    int* indices; 
    int data_length;
    if(comparator->type1 == NO_COMPARISON && comparator->type2 == NO_COMPARISON) {
        while(cur_node != NULL) {
            data = cur_node->data.leaf_data.data; 
            indices = cur_node->data.leaf_data.indices;
            data_length = cur_node->num_keys; 
            for(i = 0; i < data_length; i++) {
                ((int*)result->payload)[result->num_tuples++] = indices[i]; 
            }
            cur_node = cur_node->data.leaf_data.next_leaf; 
        }
    } else if (comparator->type1 == NO_COMPARISON && comparator->type2 != NO_COMPARISON) {
        while(cur_node != NULL) {
            data = cur_node->data.leaf_data.data; 
            indices = cur_node->data.leaf_data.indices;
            data_length = cur_node->num_keys; 
            for(i = 0; i < data_length; i++) {
                if(data[i] < p_high) {
                    ((int*)result->payload)[result->num_tuples++] = indices[i];
                }
                else {
                    return;
                }
            }
            cur_node = cur_node->data.leaf_data.next_leaf; 
        }
    } else if (comparator->type1 != NO_COMPARISON && comparator->type2 == NO_COMPARISON) {
        while(cur_node != NULL) {
            data = cur_node->data.leaf_data.data; 
            indices = cur_node->data.leaf_data.indices;
            data_length = cur_node->num_keys; 
            for(i = 0; i < data_length; i++) {
                if(data[i] >= p_low) {
                    ((int*)result->payload)[result->num_tuples++] = indices[i];
                }
            }
            cur_node = cur_node->data.leaf_data.next_leaf; 
        }
    } else {
        while(cur_node != NULL) {
            data = cur_node->data.leaf_data.data; 
            indices = cur_node->data.leaf_data.indices;
            data_length = cur_node->num_keys; 
            for(i = 0; i < data_length; i++) {
                if(data[i] >= p_low) {
                    if (data[i] < p_high) {
                        ((int*)result->payload)[result->num_tuples++] = indices[i];
                    }
                    else {
                        return;
                    }
                }
            }
            cur_node = cur_node->data.leaf_data.next_leaf; 
        }
    }
}

BtreeNode* split_node(BtreeNode* node, int* median) {
    int mid_pos = node->num_keys / 2;   

    // create new node to hold all the data after mid_pos
    BtreeNode* new_node = malloc(sizeof(BtreeNode)); 
    if(node->is_leaf) {
        *median = node->data.leaf_data.data[mid_pos];
        new_node->is_leaf = true;  
        // copy the data after mid_pos from the node we split to the new node
        int i; 
        for(i = 0; i < mid_pos; i++) {
            new_node->data.leaf_data.data[i] = node->data.leaf_data.data[mid_pos + i];
            new_node->data.leaf_data.indices[i] = node->data.leaf_data.indices[mid_pos + i];
        }
        // set the new node created as the sibling to the old node
        new_node->data.leaf_data.next_leaf = node->data.leaf_data.next_leaf; 
        node->data.leaf_data.next_leaf = new_node; 
    }
    else {
        *median = node->data.internal_data.keys[mid_pos];
        new_node->is_leaf = false; 
        int i; 
        for(i = 0; i < mid_pos; i++) {
            new_node->data.internal_data.keys[i] = node->data.internal_data.keys[mid_pos + i];
            new_node->data.internal_data.children[i] = node->data.internal_data.children[mid_pos + i];
        }
        // account for the extra pointer at the end
        new_node->data.internal_data.children[i] = node->data.internal_data.children[mid_pos + i];
    }

    // update the new size of node we just split
    new_node->num_keys = node->num_keys - mid_pos; 
    node->num_keys = mid_pos; 
    return new_node;
}

BtreeNode* insert_to_clustered_node(BtreeNode* node, int val, int* median, size_t* insert_pos) {
    // if we insert to a leaf in a clustered node, we just need to find the position to insert
    // and shift the rest of the values in the node 
    if(node->is_leaf) {
        int* data = node->data.leaf_data.data; 
        int* indices = node->data.leaf_data.indices; 
        int first_pos_in_node; 
        // this will only happen when this is the first insert to the index
        if(node->num_keys == 0) {
            first_pos_in_node = 0;
        }
        else {
            first_pos_in_node = indices[0];
        }
        int i; 
        for(i = node->num_keys; i > 0 && data[i-1] > val; i--) {
            data[i] = data[i - 1];
            indices[i] = first_pos_in_node + i;
        }
        data[i] = val; 
        indices[i] = first_pos_in_node + i; 
        node->num_keys++;

        // update the position in the index to which the value was inserted
        // to be later used to insert values in the same position in other columns
        *insert_pos = first_pos_in_node + i; 
        // go to next leaves and update positions because their positions were shifted by 1.
        BtreeNode* cur_node = node->data.leaf_data.next_leaf;
        int data_length; 
        while(cur_node != NULL) {
            data = cur_node->data.leaf_data.data; 
            indices = cur_node->data.leaf_data.indices;
            data_length = cur_node->num_keys; 
            for(i = 0; i < data_length; i++) {
                indices[i]++; 
            }
            cur_node = cur_node->data.leaf_data.next_leaf; 
        }
    }
    else {
        int* keys = node->data.internal_data.keys; 
        BtreeNode** children = node->data.internal_data.children;
        int key_pos = find_key_pos_in_node(node, val);
        int local_median; 
        BtreeNode* new_split_node = insert_to_clustered_node(node->data.internal_data.children[key_pos], val, &local_median, insert_pos); 
        // in this case a child node got split up so we need to set the new
        // node as one of this node's children 
        if(new_split_node) {
            // move all the keys and children after the position of the split node
            int i; 
            for(i = node->num_keys; i > key_pos; i--) {
                keys[i] = keys[i - 1];
            }
            for(i = node->num_keys + 1; i > key_pos + 1; i--) {
                children[i] = children[i-1];
            }
            keys[key_pos] = local_median; 
            children[key_pos + 1] = new_split_node; 
            node->num_keys++;
        }
    }

    // check if the node became full and therefore requires splitting
    if(node->num_keys == MAX_BTREE_NODE_KEYS) {
        return split_node(node, median);
    }

    return NULL;
}

BtreeNode* insert_to_unclustered_node(BtreeNode* node, int val, size_t orig_pos, int* median, bool last_val) {
    if(node->is_leaf) {
        int* data = node->data.leaf_data.data; 
        int* indices = node->data.leaf_data.indices; 
        int i; 
        // in this case we first need to iterate all the positions and update them (because we shifted stuff around in the 
        // original column data) 
        if(!last_val) {
            BtreeNode* cur_node = node; 
            // go to the first leaf node
            while(!cur_node->is_leaf) {
                cur_node = cur_node->data.internal_data.children[0]; 
            }
            // iterate on all leaf nodes and update any position of values that were shifted in the original
            // column data
            while(cur_node != NULL) {
                indices = node->data.leaf_data.indices;
                for(i = 0; i < cur_node->num_keys; i++) {
                    if((size_t)indices[i] >= orig_pos) {
                        indices[i] += 1;
                    }
                }
                cur_node = cur_node->data.leaf_data.next_leaf;
            }   
        }
        // now we updated all the needed positions, we can proceed to insert the values to the index
        for(i = node->num_keys; i > 0 && data[i-1] > val; i--) {
            data[i] = data[i - 1];
            indices[i] = indices[i - 1];
        }
        data[i] = val; 
        indices[i] = orig_pos; 
        node->num_keys++;
    }
    else {
        int* keys = node->data.internal_data.keys; 
        BtreeNode** children = node->data.internal_data.children;
        int key_pos = find_key_pos_in_node(node, val);
        int local_median; 
        // recursively add the data to the children nodes. When a child node splits, we get a pointer to the new right sibling 
        // of the split node and insert it. 
        BtreeNode* new_split_node = insert_to_unclustered_node(node->data.internal_data.children[key_pos], val, orig_pos, &local_median, last_val); 
        // in this case a child node got split up so we need to set the new
        // node as one of this node's children 
        if(new_split_node) {
            // move all the keys and children after the position of the split node
            int i; 
            for(i = node->num_keys; i > key_pos; i--) {
                keys[i] = keys[i - 1];
            }
            for(i = node->num_keys + 1; i > key_pos + 1; i--) {
                children[i] = children[i-1];
            }
            keys[key_pos] = local_median; 
            children[key_pos + 1] = new_split_node; 
            node->num_keys++;
        }
    }

    // we always split when on insert when node becomes full, instead of waiting for the next insert. 
    if(node->num_keys == MAX_BTREE_NODE_KEYS) {
        return split_node(node, median);
    }
    return NULL;
}

void insert_to_unclustered_btree_index(BtreeIndex* index, int val, size_t orig_pos, bool last_val) {
    int median; 
    BtreeNode* split_node = insert_to_unclustered_node(index->btree_root, val, orig_pos, &median, last_val);    
    // in this case the root node split, we need to create a new root node that points to the old
    // root and the new node that was created
    if(split_node != NULL) {
        BtreeNode* new_node = malloc(sizeof(BtreeNode)); 
        new_node->is_leaf = false; 
        new_node->data.internal_data.keys[0] = median; 
        new_node->num_keys = 1; 
        new_node->data.internal_data.children[0] = index->btree_root; 
        new_node->data.internal_data.children[1] = split_node; 
        index->btree_root = new_node;
    }
}

size_t insert_to_clustered_btree_index(BtreeIndex* index, int val) {
    int median; 
    size_t insert_pos;
    BtreeNode* split_node = insert_to_clustered_node(index->btree_root, val, &median, &insert_pos);    
    // in this case the root node split, we need to create a new root node that points to the old
    // root and the new node that was created
    if(split_node != NULL) {
        BtreeNode* new_node = malloc(sizeof(BtreeNode)); 
        new_node->is_leaf = false; 
        new_node->data.internal_data.keys[0] = median; 
        new_node->num_keys = 1; 
        new_node->data.internal_data.children[0] = index->btree_root; 
        new_node->data.internal_data.children[1] = split_node; 
        index->btree_root = new_node;
    }

    return insert_pos;
}


