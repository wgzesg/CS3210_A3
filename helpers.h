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

/**
 * Read a file into a char*
 * @param input_files_dir path to files folder
 * @param index file index to be read
 * @return char* of file content read
 */
char* read_file(char* input_files_dir, int index);

/**
 * Split output from one file to different reducers
 * @param mapper containers for each reducers input; key: reducer index; val: hashmap of key-val pairs
 * @param output output from one file
 * @param num_baskets number of reducers
 */
void split(std::unordered_map<int, std::unordered_map<std::string, int>>& mapper, MapTaskOutput* output, int num_baskets);

/**
 * master send to mappers
 * @param target target mapper rank
 * @param file_index file index
 * @param input_files_dir path to files folder 
 */
void master_to_mapper(int target, int file_index, char* input_files_dir);

/**
 * mappers receive from master
 * @param file_index receiving file index
 */
char* mapper_receive(int file_index);

/**
 * mappers sends to reducers
 * @param mapper all key-val pairs for a specific reducer
 * @param target target reducer rank
 */
void mapper_to_reducer(std::unordered_map<std::string, int>& mapper, int target);

/**
 * reducers receive from a mapper
 * @param key_buffer received list of keys 
 * @param val_buffer received list of values
 * @return number of key-val pairs
 */
int reducer_receive(char* &key_buffer, int* &val_buffer);

/**
 * convert a map to two buffers of keys and vals
 * @param mini_map hashmap to be converted to two arrays
 * @param keys bufferes to store all keys
 * @param vals bufferes to store all values
 */
void flattenMap(std::unordered_map<std::string, int>& mini_map, char* &keys, std::vector<int> &vals);

/**
 * reduce key-vals pairs from different mappers
 * @param key_buffer new received keys buffer
 * @param val_buffer new received val buffer
 * @param overall_map currently reduced values
 */
void reduce(char* key_buffer, int* val_buffer, std::unordered_map<std::string, int>& overall_map, int size);

/**
 * Send reduced result to master
 * @param overall_map all key-val pairs to be sent
 */
void reducer_to_master(std::unordered_map<std::string, int>& overall_map);

/**
 * Write key-val pairs to file
 * @param output_file designated output file
 * @param key_buffer keys to be written 
 * @param val_buffer values to be written 
 * @param size number of key-value pairs
 */
void writeToFile(std::ofstream& output_file, char* key_buffer, int* val_buffer, int size);

/**
 * Master receive results from reducer and store to file
 * @param output_file designated output file
 */
void master_receive(std::ofstream& output_file);
