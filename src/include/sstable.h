//
// Created by yanjs on 2023/3/7.
//

#pragma once

#include <iostream>
#include <string>
#include <map>
#include <cstring>
#include <fstream>
#include <vector>

const int TABLE_BYTES = 10240;

class SSTable
{
public:
    static uint64_t current_time_stamp;
    uint64_t timestamp;
    uint64_t count;
    uint64_t max_key;
    uint64_t min_key;

public:
    std::map<uint64_t, std::string> table_map;

    SSTable();

    void to_sst_file();

    void read_sst_file();


};

void long_to_bytes(uint64_t num, char **des);

void int_to_bytes(uint32_t num, char **des);

void string_to_bytes(const std::string &str, char **des);

uint64_t bytes_to_long(char **src);

uint32_t bytes_to_int(char **src);

std::string bytes_to_string(char **src, uint32_t len);
