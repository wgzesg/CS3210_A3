#include <stdio.h>
#include <stdlib.h>
extern "C" {
  #include "tasks.h"
  #include "utils.h"
}
#include <vector>
#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>

void accumulate(std::unordered_map<std::string, std::vector<int>>& mapp, MapTaskOutput* output);
void reduce_map(std::unordered_map<std::string, std::vector<int>>& overall_map, std::unordered_map<std::string, int>& reduced_map) {
    for (auto entry : overall_map) {
        std::string key_str = entry.first;
        char key[8];
        std::copy(key_str.begin(), key_str.end(), key);
        int* vals = entry.second.data();
        int length = entry.second.size();
        KeyValue reduced = reduce(key, vals, length);
        reduced_map[key_str] = reduced.val;
    }
}
char* read_file(char* input_files_dir, int index) {
    char file_name[80];
    sprintf(file_name, "%s/%d.txt", input_files_dir, index);
    // printf("file name is %s", file_name);
    FILE *ptr;
    ptr = fopen(file_name,"r");
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

int main(int argc, char** argv) {
    char *input_files_dir = argv[1];
    int num_files = atoi(argv[2]);
    char *output_file_name = argv[3];
    int map_reduce_task_num = atoi(argv[4]);

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
    std::unordered_map<std::string, std::vector<int>> mapp;
    std::unordered_map<std::string, int> reduced_mapp;
    for(int i = 0; i < num_files; i++) {
      char *content = read_file(input_files_dir, i);
      MapTaskOutput* output = map(content);
      accumulate(mapp, output);
    }

    reduce_map(mapp, reduced_mapp);

    std::ofstream output_file;
    output_file.open(output_file_name);
}

void accumulate(std::unordered_map<std::string, std::vector<int>>& mapp, MapTaskOutput* output) {
  int len = output->len;
  for(int i = 0; i < len; i++) {
    std::string s;
    s.assign(output->kvs[i].key);
    auto entry = mapp.find(s);
    if (entry != mapp.end()) {
        auto curr_vect = entry->second;
        curr_vect.push_back(output->kvs[i].val);
        mapp[entry->first]=curr_vect;
    } else {
        std::vector<int> new_vect;
        new_vect.push_back(output->kvs[i].val);
        mapp[entry->first]=new_vect;
    }
  }
}
