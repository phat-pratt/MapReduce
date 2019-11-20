//
//  mapreduce.c
//
//
//  Created by James Pratt on 11/19/19.
//

#include <stdio.h>
#include <pthread.h>
#include "mapreduce.h"

// Takes different key/value pairs from many different mappers and
// stores them in a partition such that the reducers can later access them.
void MR_Emit(char *key, char *value)
{
    return;
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions)
{
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
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
//
//      argv[1] … argv[n-1] (with argc equal to n) all contain file names that
//      will be passed to the mappers.
void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce,
            int num_reducers, Partitioner partition, int num_partitions)
{
    //start num_mapper mapper threads.

    // start mapper threads for each filename.
    // mappers take filename as args
    for (int i = 0; i < argc; i++)
    {
    }

    //start num_reducer reducer threads

    return;
}
