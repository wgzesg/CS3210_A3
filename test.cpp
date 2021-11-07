#include <stdio.h>
#include <stdlib.h>
extern "C" {
  #include "tasks.h"
  #include "utils.h"
}
#include "helpers.h"
#include <vector>
#include <iostream>
#include <string>
#include <unordered_map>

int main(int argc, char** argv) {
    char *input_files_dir = argv[1];
    char * content = NULL;
    content = read_file(input_files_dir, 0);
    MapTaskOutput* output = map1(content);
    std::unordered_map<int, std::unordered_map<std::string, int>> mapper;
    std::unordered_map<int,int> mapper_test;
    // printf("size of output is %d\n", size);
    split(mapper, output);
    for(auto u : mapper){
        std::cout << u.first << " length: " <<  u.second.size() << std::endl;
    }
    std::cout << mapper_test[100] << std::endl;
}
