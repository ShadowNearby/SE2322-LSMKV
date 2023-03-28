//
// Created by yanjs on 2023/3/26.
//
#include "global.h"

uint64_t current_timestamp = 0;
std::map<uint32_t, std::pair<uint32_t, LevelType>> config_level = std::map<uint32_t, std::pair<uint32_t, LevelType>>();
uint32_t merge_file_count = 0;
//haha
std::map<uint32_t, std::map<std::string, IndexData>> all_sst_index = std::map<uint32_t, std::map<std::string, IndexData>>();

void long_to_bytes(uint64_t num, char **des)
{
    char *head = (char *) (&num);
    for (int i = 0; i < 8; ++i) {
        **des = *(head + i);
        (*des) += 1;
    }
}

void int_to_bytes(uint32_t num, char **des)
{
    char *head = (char *) (&num);
    for (int i = 0; i < 4; ++i) {
        **des = *(head + i);
        (*des) += 1;
    }
}

void string_to_bytes(const std::string &str, char **des)
{
    strcpy(*des, str.c_str());
    *des += str.size();
}

uint64_t bytes_to_long(char **src)
{
    uint64_t num = 0;
    for (int i = 0; i < 8; ++i) {
        *((char *) (&num) + i) = *(*src + i);
    }
    (*src) += 8;
    return num;
}

uint32_t bytes_to_int(char **src)
{
    uint32_t num = 0;
    for (int i = 0; i < 4; ++i) {
        *((char *) (&num) + i) = *(*src + i);
    }
    (*src) += 4;
    return num;
}

std::string bytes_to_string(char **src, uint32_t len)
{
    std::string str;
    for (uint32_t i = 0; i < len; ++i) {
        char ch = *(*src + i);
        str.push_back(ch);
    }
    (*src) += len;
    return str;
}