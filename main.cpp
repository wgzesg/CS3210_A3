#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
extern "C" {
  #include "tasks.h"
  #include "utils.h"
}
#include <vector>
#include <iostream>
#include <string>
#include <unordered_map>
#include "helpers.h"

void reduce(char* key_buffer, int* val_buffer, std::unordered_map<std::string, int>& overall_map, int size);

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);

    int world_size, rank;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    // Get command-line params
    char *input_files_dir = argv[1];
    int num_files = atoi(argv[2]);
    int num_map_workers = atoi(argv[3]);
    int num_reduce_workers = atoi(argv[4]);
    char *output_file_name = argv[5];
    int map_reduce_task_num = atoi(argv[6]);

    // Identify the specific map function to use
    MapTaskOutput* (*map) (char*);
    switch(map_reduce_task_num){
        case 1:
            map = &map1;
            break;
        case 2:
            map = &map2;
            break;
        case 3:
            map = &map3;
            break;
    }

    const int MAPPER_OFFSET = 1;
    const int REDUCER_OFFSET = 1 + num_map_workers;
    // Distinguish between master, map workers and reduce workers
    if (rank == 0) {
        // TODO: Implement master process logic
        const int MAPPER_OFFSET = 1;
        MPI_Status status;
        printf("Rank (%d): This is the master process\n", rank);
        for (int i =0; i < num_files + num_map_workers; i++) {
            int target;
            MPI_Recv(&target, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
            printf("[Rank %d]: Sending file %d to %d\n", rank, i, target);
            if(i >= num_files) {
                int dummy = 0;
                printf("[Rank %d]: No more file to %d\n", rank, target);
                MPI_Send(&dummy, 1, MPI_INT, target, 100, MPI_COMM_WORLD);
                continue;
            }
            master_to_mapper(target, i, input_files_dir);
        }
    } else if ((rank >= 1) && (rank <= num_map_workers)) {
        // TODO: Implement map worker process logic
        printf("Rank (%d): This is a map worker process\n", rank);
        int index = rank;
        std::unordered_map<int, std::unordered_map<std::string, int>> mapper;
        while(1) {
            // Receive data
            printf("[Rank %d]: Expecting file from master\n", rank);
            MPI_Send(&rank, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
            char* content = mapper_receive(index);
            printf("[Rank %d]: Receivd file from master\n", rank);
            // No more file
            if(content == NULL) {
                printf("[Rank %d]: No more file\n", rank);
                break;
            }
            // Process data
            MapTaskOutput* output = map(content);
            // Split results
            split(mapper, output, num_reduce_workers);
            // Clean up
            index += num_reduce_workers;
            free(content);
            free_map_task_output(output);
        }
        // Send to reducer
        for(int i = 0; i < num_reduce_workers; i++) {
            auto content = mapper.find(i);
            int target = i % num_reduce_workers + REDUCER_OFFSET;
            printf("[Rank %d]: Sending key %d to %d\n", rank, i, target);
            if (content == mapper.end()) {
                int dummy = 0;
                MPI_Send(&dummy, 1, MPI_INT, target, 1, MPI_COMM_WORLD);
                continue;
            }
            auto mini_map = mapper[i];
            mapper_to_reducer(mini_map, target);
        }
    } else {
        printf("Rank (%d): This is a reduce worker process\n", rank);
        int index = 1;
        int size;
        char *key_buffer;
        int *val_buffer;
        std::unordered_map<std::string, int> overall_map;
        while(index <= num_map_workers) {
            // Receive data
            printf("[Rank %d]: Expecting a message from %d with tag %d\n", rank, index, 0);
            size = reducer_receive(key_buffer, val_buffer);
            printf("[Rank %d]: Received from %d with tag %d\n", rank, index, 0);

            // TODO: Reduce data
            // reduce(key_buffer, val_buffer, overall_map, size);

            // Clean up
            index += 1;
            free(key_buffer);
            free(val_buffer);
        }
        // TODO: Send back to master
    }
    //Clean up
    MPI_Finalize();
    return 0;
}

void reduce(char* key_buffer, int* val_buffer, std::unordered_map<std::string, int>& overall_map, int size) {
    std::string key;
    char* ptr = key_buffer;
    int* val_ptr = val_buffer;
    for(int i = 0; i < size; i++) {
        std::string key;
        int val = *val_ptr;
        key.assign(ptr);
        ptr += 8;
        // std::cout << key << ": " << val << std::endl;
    }
}
