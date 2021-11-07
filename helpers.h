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