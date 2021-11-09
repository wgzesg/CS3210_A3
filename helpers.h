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

char* read_file(char* input_files_dir, int index);
void split(std::unordered_map<int, std::unordered_map<std::string, int>>& mapper, MapTaskOutput* output, int num_baskets);
void master_to_mapper(int target, int tag, char* input_files_dir);
char* mapper_receive(int file_index);
void mapper_to_reducer(std::unordered_map<std::string, int>& mapper, int target);
int reducer_receive(char* &key_buffer, int* &val_buffer);
void flattenMap(std::unordered_map<std::string, int>& mini_map, char* &keys, std::vector<int> &vals);
void reduce(char* key_buffer, int* val_buffer, std::unordered_map<std::string, int>& overall_map, int size);
void reducer_to_master(std::unordered_map<std::string, int>& overall_map);
