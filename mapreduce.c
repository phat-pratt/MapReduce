#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "mapreduce.h"
// Different function pointer types used by MR

// typedef char *(*Getter)(char *key, int partition_number);
// typedef void (*Mapper)(char *file_name);
// typedef void (*Reducer)(char *key, Getter get_func, int partition_number);
// //typedef void (*Reducer)(Getter get_func);
// typedef unsigned long (*Partitioner)(char *key, int num_partitions);

int partitions = 0;
struct node
{
    char *key;
    int value;
    struct node *next;
};

struct reducerArgs 
{ 
    int numr;
    Reducer reduce;
    Getter get;
    int nump[];
};

struct node **part_array;
int *ends;
int *front;
char ***parray;
pthread_mutex_t lock; 

char *getNext(char *key, int partition_number){
    for(int i = front[partition_number]; i < ends[i]; i++){
        
        if(!strcmp(parray[partition_number][i], key)) {
            front[partition_number] = i+1;
            return key;
        }
    }

    return NULL;
}

void append(int p, char *key, int value) 
{ 
    struct node* new_node = (struct node*) malloc(sizeof(struct node)); 
    struct node *last = part_array[p];  
   
    new_node->key  = key;
    new_node->value  = value;

    new_node->next = NULL; 
    if (part_array[p] == NULL) 
    { 
       part_array[p] = new_node; 
       return; 
    }   
       
    while (last->next != NULL){ 
        last = last->next; 
    }
    last->next = new_node; 
    return;     
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
    int p = MR_DefaultHashPartition(key, partitions);

    if(p > partitions) {
        printf("Something went wrong: partition returned > num_partitions!");
    }
    //add node to partition p:
    pthread_mutex_lock(&lock); 
    append(p, key, atoi(value));
    parray[p][ends[p]++] = key;
    parray[p] = realloc(parray[p], (ends[p]+1)*sizeof(char*));
    //add to end of array
    // for(int i = 0; i< partitions; i++){
    //     parray[i] = malloc(sizeof(char * ));
    //     ends[i] = 0;
    // }

    pthread_mutex_unlock(&lock); 

    return;
}

unsigned long MR_SortedPartition(char *key, int num_partitions)
{
    unsigned long hash = 5381;
    return hash;
}
void reducerHelper(void * arg) {
    struct reducerArgs* args = (struct reducerArgs *) arg;
    
    //for loop that iterates through all the keys that are ass. with this partition.
    //args->reduce(args)
    //printf("%d\n", args->numr);
    // for each partition p (args->nump)
        // for each key in that partition p
            // args-reduce(key, getter, p)
    for(int i = 0; i < args->numr; i++){
        printf("****************%d\n", args->nump[i]);
        for(int j = 0; j < ends[args->nump[i]]; j++){
            if(j<front[args->nump[i]]){
                continue;
            }
            args->reduce(parray[args->nump[i]][j], args->get, args->nump[i]);
        }
        // struct node *temp = part_array[args->nump[i]];
        // while(temp != NULL) {
        //     args->reduce(temp->key, args->get, args->nump[i]);
        //     temp = temp->next;
        // }
    }
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

int compare (const void * a, const void * b ) {
    const char *pa = *(const char**)a;
    const char *pb = *(const char**)b;

    return strcmp(pa,pb);
}
void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce,
            int num_reducers, Partitioner partition, int num_partitions)
{
    partitions = num_partitions;
    part_array = (struct node **)malloc(sizeof(struct node *) * partitions);

    parray = malloc(partitions*sizeof(char **));

    ends = (int*)malloc(sizeof(int) * partitions);
    front = (int*)malloc(sizeof(int) * partitions);

    for(int i = 0; i< partitions; i++){
        parray[i] = malloc(sizeof(char * ));
        ends[i] = 0;
        front[i] = 0;
    }

    // printf("%s", parray[0][9]);  
      //test printing node 
    // printf("node key: %s\n", part_array[0]->key);
    //create num_mapper threads
    pthread_t mappers[num_mappers];
    
    if (pthread_mutex_init(&lock, NULL) != 0) { 
        printf("\n mutex init has failed\n"); 
        return; 
    } 

    //start num_mapper mapper threads.
    for(int i = 0;  i < num_mappers; i++) {
        pthread_create(&mappers[i], NULL, (void *)map, argv[i+1]);
    }
    for(int i = 0;  i < num_mappers; i++) {
        pthread_join(mappers[i], NULL);
    }
    pthread_mutex_destroy(&lock); 

    for(int i = 0; i < num_partitions; i++){
        struct node *temp = part_array[i];
        printf("\t%d) ", i);
        while(temp != NULL) {
            printf("%s ",temp->key);
            temp = temp->next;
        }
        printf("\n");
    }
    for(int i = 0; i < num_partitions; i++) {
        qsort(parray[i], ends[i], sizeof(char*), compare);
    }
    for(int i = 0; i < num_partitions; i++) {
        printf("%d) ", i);
        for(int j = 0; j < ends[i]; j ++){
            printf("%s ",parray[i][j]);
        }
        printf("\n");
    }

    //create reduce thread. 
    pthread_t reducers[num_reducers];

    //void Reduce(char *key, Getter get_next, int partition_number)

    struct reducerArgs args[num_reducers];
    // args->key = "key";
    for(int i = 0; i < num_reducers; i++){
        args[i].numr = 0;
        args[i].reduce = reduce;
        args[i].get = getNext;
    }
    
    
    //iterate to find partitions to reduce.
    
    // for all partitions
        // assign partitions to a reducer
        // add i to args[i % num_reducers].nump  

    for(int i = 0; i < num_partitions; i++){
        printf("reducer: %d  -> %d\n",i%num_reducers, i);
        args[i%num_reducers].nump[args[i%num_reducers].numr++] = i;
    }
    for(int i = 0; i < num_reducers; i++){
        printf("%d) ", i+1);
        for(int j = 0; j < args[i].numr;j++){
            printf("%d ", args[i].nump[j]);
        }
        printf("\n");
    }
    // args->nump = 0;
    for(int i = 0; i < num_reducers; i++) {
        pthread_create(&reducers[0], NULL, (void*)reducerHelper, &args);
    }
    for(int i = 0; i < num_reducers; i++) {
        pthread_join(reducers[0],NULL);
    }
   
    // pthread_create(&reducers[0], NULL, (void*)reduce, (void*)&getNext);
    // pthread_join(reducers[0],NULL);

    return;
}
