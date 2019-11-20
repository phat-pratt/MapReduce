#ifndef __mapreduce_h__
#define __mapreduce_h__
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "mapreduce.h"
// Different function pointer types used by MR
typedef char *(*Getter)(char *key, int partition_number);
typedef void (*Mapper)(char *file_name);
typedef void (*Reducer)(char *key, Getter get_func, int partition_number);
typedef unsigned long (*Partitioner)(char *key, int num_partitions);

int partitions = 0;
struct node
{
    char *key;
    int value;
    struct node *next;
};

struct node **part_array;

/**
 * How to add to a linked list:
 *   struct node *temp = part_array[0];
 *   addNode(&temp, "key", 1);
 *   part_array[0] = temp;
 * 
 **/
void addNode(struct node **q, char *key, int value)
{
    if (*q == NULL)
    {
        *q = malloc(sizeof(struct node));
    }
    else
    {
        while ((*q)->next != NULL)
            *q = (*q)->next;

        (*q)->next = malloc(sizeof(struct node));
        *q = (*q)->next;
    }

    (*q)->key = key;
    (*q)->value = value;

    (*q)->next = NULL;
    //printf("q->key: %s\n", (*q)->key);
}


// External functions: these are what *you must implement*

// Takes different key/value pairs from many different mappers and
// stores them in a partition such that the reducers can later access them.

unsigned long MR_DefaultHashPartition(char *key, int num_partitions)
{
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}
void MR_Emit(char *key, char *value)
{
    // printf("key: %s\n", key);
    // printf("value: %s\n", value);
    int p = MR_DefaultHashPartition(key, partitions);
    //printf("Partition: %d\n\n", p);

    if(p > partitions) {
        printf("Something went wrong: partition returned > num_partitions!");
    }
    //add node to partition p:
    struct node *temp = part_array[p];

    addNode(&temp, key, atoi(value));
    part_array[p] = temp;

    return;
}

unsigned long MR_SortedPartition(char *key, int num_partitions)
{
    unsigned long hash = 5381;
    return hash;
}
// Arguments:
//      Mapper map -> pointer to mapper function.
//      int num_mappers -> number of mapper threads the library needs to create
//
//      Reducer reduce -> pointer to a reducer function.
//      int num_redcers -> number of reducer threads to create.
//
//      Partitioner partition -> pointer to a Partition function
//      int num_partition -> number of partitions

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce,
            int num_reducers, Partitioner partition, int num_partitions)
{
    partitions = num_partitions;

    part_array = (struct node **)malloc(sizeof(struct node *) * partitions);

    
    //test printing node 
    // printf("node key: %s\n", part_array[0]->key);
    //create num_mapper threads
    pthread_t mappers[num_mappers];
    //pthread_t reducers[num_reducers];
    //start num_mapper mapper threads.
    pthread_create(&mappers[0], NULL, (void *)map, argv[1]);
    pthread_join(mappers[0], NULL);

    //create reduce thread. 
    // pthread_t reducers[num_reducers];
    // void *rArgs[3];
    // rArgs[0] = (void*)"";
    // rArgs[1] = NULL;
    // rArgs[2] = &partitions;
    // //start num_reducer reducer threads
    // pthread_create(&reducers[0], NULL, (void*)reduce, rArgs);
    // pthread_join(reducers[0],NULL);
    return;
}

#endif // __mapreduce_h__
