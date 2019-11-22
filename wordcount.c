#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mapreduce.h"
#include "mapreduce.c"
void Map(char *file_name)
{
    FILE *fp = fopen(file_name, "r");
    assert(fp != NULL);

    char *line = NULL;
    size_t size = 0;
    while (getline(&line, &size, fp) != -1)
    {
        char *token, *dummy = line;
        //printf("line: %s\n", line);
        // chops line into tokens. Each token is emitted.
        while ((token = strsep(&dummy, " \t\n\r")) != NULL)
        {
            // here the key is the word itself and the token is
            //just a count. In this case 1
            MR_Emit(token, "1");
        }
    }
    free(line);
    fclose(fp);
}

void Reduce(char *key, Getter get_next, int partition_number)
{
    printf("here");
    int count = 0;
    char *value = get_next("hey", 0);
    printf("%s\n", value);
    // while ((value = get_next(key, partition_number)) != NULL) {
    //     printf("%s\n", value);
    //     break;
    //     count++;
    // }
    printf("%s %d\n", key, count);
}

int main(int argc, char *argv[])
{
    MR_Run(argc, argv, Map, 10, Reduce, 10, MR_DefaultHashPartition, 10);
}
