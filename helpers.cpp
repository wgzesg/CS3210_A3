#include "helpers.h"
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
#include <mpi.h>

char* read_file(char* input_files_dir, int index) {
    char file_name[80];
    sprintf(file_name, "%s/%d.txt", input_files_dir, index);
    // printf("file name is %s", file_name);
    FILE *ptr;
    ptr = fopen(file_name,"r");
    if( !ptr ) perror("blah.txt"),exit(1);
    fseek(ptr, 0, SEEK_END);
    int size = ftell(ptr);
    rewind(ptr);
    char *contentBuffer = (char*)malloc((size + 1) * sizeof(char));
    if( 1!=fread( contentBuffer , size, 1, ptr) )
        fclose(ptr),free(contentBuffer),fputs("entire read fails",stderr),exit(1);
    contentBuffer[size] = '\0';
    fclose(ptr);
    return contentBuffer;
}

void split(std::unordered_map<int, std::unordered_map<std::string, int>>& mapper, MapTaskOutput* output, int num_baskets) {
    int size = output->len;
    for(int i= 0; i < size; i++) {
        // std::cout << output->kvs[i].key << ": " << output->kvs[i].val << std::endl;
        int busket = partition(output->kvs[i].key, num_baskets);
        auto entry = mapper.find(busket);
        if (entry != mapper.end()) {
            auto tiny_map = entry->second;
            std::string s;
            s.assign(output->kvs[i].key);
            tiny_map[s] += output->kvs[i].val;
            mapper[entry->first]=tiny_map;
        } else {
            std::unordered_map<std::string, int> new_tiny_map;
            new_tiny_map[output->kvs[i].key] = output->kvs[i].val;
            mapper.insert({busket, new_tiny_map});
        }
    }
}

void master_to_mapper(int target, int file_index, char* input_files_dir) {
    char* content_buffer = read_file(input_files_dir, file_index);
    int content_size = strlen(content_buffer);
    MPI_Send(&content_size, 1, MPI_INT, target, file_index, MPI_COMM_WORLD);
    MPI_Send(content_buffer, content_size, MPI_CHAR, target, file_index, MPI_COMM_WORLD);
    free(content_buffer);
}

char* mapper_receive(int file_index) {
    int content_size = 0;
    MPI_Status status;
    MPI_Recv(&content_size, 1, MPI_INT, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
    if(status.MPI_TAG == 100) {
        return NULL;
    }
    char *content_buffer = (char*)malloc(sizeof(content_buffer) * (content_size + 1));
    MPI_Recv(content_buffer, content_size, MPI_CHAR, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
    content_buffer[content_size] = '\0';
    return content_buffer;
}

void mapper_to_reducer(std::unordered_map<std::string, int>& mini_map, int target) {
    int size = mini_map.size();
    char *keys;
    std::vector<int> vals;
    flattenMap(mini_map, keys, vals);
    int* values = vals.data();

    MPI_Send(&size, 1, MPI_INT, target, 0, MPI_COMM_WORLD);
    MPI_Send(keys, size * 8, MPI_CHAR, target, 0, MPI_COMM_WORLD);
    MPI_Send(values, size, MPI_INT, target, 0, MPI_COMM_WORLD);
    free(keys);
}

void flattenMap(std::unordered_map<std::string, int>& mini_map, char* &keys, std::vector<int> &vals) {
    int size = mini_map.size();
    keys = (char*) malloc(sizeof(char) * 8 * size);
    char* ptr = keys;
    for(auto kv : mini_map) {
        std::cout << kv.first << ": " << kv.second << std::endl;
        int length = kv.first.length();
        std::copy(kv.first.begin(), kv.first.end(), ptr);
        *(ptr+length) = '\0';
        vals.push_back(kv.second);  
        ptr += 8;
    }
}

int reducer_receive(char* &key_buffer, int* &val_buffer) {
    int size = 0;
    MPI_Status status;
    MPI_Recv(&size, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
    if (status.MPI_TAG == 1) {
        std::cout << "no such key" << std::endl;
        key_buffer = nullptr;
        val_buffer = nullptr;
        return 0;
    }
    int index = status.MPI_SOURCE;
    key_buffer = (char*)malloc(sizeof(char) * size * 8);
    val_buffer = (int*)malloc(sizeof(int) * size);
    MPI_Recv(key_buffer, size * 8, MPI_CHAR, index, 0, MPI_COMM_WORLD, &status);
    MPI_Recv(val_buffer, size, MPI_INT, index, 0, MPI_COMM_WORLD, &status);
    return size;
}

void reduce(char* key_buffer, int* val_buffer, std::unordered_map<std::string, int>& overall_map, int size) {
    std::string key;
    char* ptr = key_buffer;
    int* val_ptr = val_buffer;
    // printf("Received keys of size %d\n", size);
    for(int i = 0; i < size; i++) {
        std::string key;
        int val = *val_ptr;
        key.assign(ptr);
        ptr += 8;
        overall_map[key] += val_ptr[i];
    }
}

void reducer_to_master(std::unordered_map<std::string, int>& overall_map) {
    int size = overall_map.size();
    char *keys;
    std::vector<int> vals;
    flattenMap(overall_map, keys, vals);
    int* values = vals.data();

    MPI_Send(&size, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
    MPI_Send(keys, size * 8, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
    MPI_Send(values, size, MPI_INT, 0, 0, MPI_COMM_WORLD);
    free(keys);
}