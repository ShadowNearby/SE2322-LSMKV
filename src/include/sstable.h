//
// Created by yanjs on 2023/3/7.
//

#pragma once

#include "MurmurHash3.h"
#include "memtable.h"
#include "utils.h"

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

    bool read_sst_file_index(const std::string &file_path);

    static void read_sst_header_index(uint32_t level, const std::string &file_path);

    static bool key_exist(const std::string &file_path, uint64_t key);

    static void
    read_sst_to_map(const std::string &file_path, const IndexData &index_data, std::map<uint64_t, std::string> &target);

    void filter_hash(uint64_t key);

    static std::string get_value_all_disk(const std::string &file_path, uint64_t key);

    static std::string get_value_index(const std::string &file_path, uint64_t key, const IndexData &index_data);

    static void
    scan_value(const std::string &file_path, uint64_t key1, uint64_t key2, std::map<uint64_t, std::string> &list);

    static void merge(const std::string &data_dir);

    static void maps_to_sst(uint32_t level, uint64_t timestamp, const std::string &data_dir,
                            const std::map<uint64_t, std::string> &target);

    static void
    one_map_to_sst(uint32_t level, const std::string &file_path, const std::map<uint64_t, std::string> &target,
                   uint64_t timestamp);

    static bool is_current_level(uint32_t level, const std::string &file_path);

    static void get_newest_sst(uint32_t n, uint32_t level, std::vector<std::pair<uint64_t, std::string>> &target);
};



