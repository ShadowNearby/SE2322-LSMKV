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
#include <bitset>
#include "MurmurHash3.h"
#include "memtable.h"

const int TABLE_BYTES = 1024 * 1024 * 2;
const int HEADER_BYTES = 10240 + 8 * 4;
const int FILTER_LONGS = 10240 / 64;

class SSTable
{
public:
    static uint64_t current_timestamp;
    uint64_t timestamp;
    uint64_t count;
    uint64_t max_key;
    uint64_t min_key;
    std::bitset<10240> filter;

public:
    std::map<uint64_t, std::string> table_map;

    SSTable();

    explicit SSTable(const MemTable &mem_table);

    void to_sst_file(const std::string &dir);

    void read_sst_file(const std::string &file_path);

    void read_sst_header(const std::string &file_path);

    static bool key_exist(const std::string &file_path, uint64_t key);

    void filter_hash(uint64_t key);

    bool filter_exist(uint64_t key);
};

void long_to_bytes(uint64_t num, char **des);

void int_to_bytes(uint32_t num, char **des);

void string_to_bytes(const std::string &str, char **des);

uint64_t bytes_to_long(char **src);

uint32_t bytes_to_int(char **src);

std::string bytes_to_string(char **src, uint32_t len);
