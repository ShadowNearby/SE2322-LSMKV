//
// Created by yanjs on 2023/3/7.
//

#pragma once

#include "MurmurHash3.h"
#include "memtable.h"


class SSTable
{
public:
    uint64_t timestamp;
    uint64_t count;
    uint64_t max_key;
    uint64_t min_key;
    std::bitset<FILTER_BITS> filter;

public:
    std::map<uint64_t, std::string> table_map;


    SSTable();

    explicit SSTable(const MemTable &mem_table);

    void to_sst_file(const std::string &dir);

    bool read_sst_file(const std::string &file_path);

    bool read_sst_header(const std::string &file_path);

    static bool key_exist(const std::string &file_path, uint64_t key);

    void filter_hash(uint64_t key);

    static std::string get_value_all_disk(const std::string &file_path, uint64_t key);

    static std::string get_value_index(const std::string &file_path, uint64_t key, const IndexData &indexData);

    static void
    scan_value(const std::string &file_path, uint64_t key1, uint64_t key2, std::map<uint64_t, std::string> &list);
};

void long_to_bytes(uint64_t num, char **des);

void int_to_bytes(uint32_t num, char **des);

void string_to_bytes(const std::string &str, char **des);

uint64_t bytes_to_long(char **src);

uint32_t bytes_to_int(char **src);

std::string bytes_to_string(char **src, uint32_t len);

