//
// Created by yanjs on 2023/3/26.
//

#pragma once

#include <string>
#include <utility>
#include <random>
#include <list>
#include <fstream>
#include <map>
#include <iostream>
#include <cstring>
#include <vector>
#include <bitset>
#include <memory>
#include <algorithm>
#include <filesystem>
#include <chrono>
#include <cassert>
#include <cmath>

const double TABLE_P = 0.25;
const uint32_t TABLE_MAX_LEVEL = 32;
const uint32_t TABLE_MASK = 0xfff;
const uint32_t INIT_BYTES = 10240 + 32;
const uint32_t MAX_DATA_BYTES = 1024 * 1024 * 2 - INIT_BYTES;
const uint32_t TABLE_BYTES = 1024 * 1024 * 2;
const uint32_t HEADER_BYTES = 10240 + 8 * 4;
const uint32_t FILTER_BYTES = 10240;
const uint32_t FILTER_LONGS = FILTER_BYTES / 8;
const uint32_t FILTER_BITS = FILTER_BYTES * 8;
const std::string TIERING_STR = "Tiering";
const std::string LEVEL_STR = "Leveling";
const std::string DATA_PATH = "../data/";

void long_to_bytes(uint64_t num, char **des);

void int_to_bytes(uint32_t num, char **des);

void string_to_bytes(const std::string &str, char **des);

uint64_t bytes_to_long(char **src);

uint32_t bytes_to_int(char **src);

std::string bytes_to_string(char **src, uint32_t len);

struct IndexData
{
    uint64_t timestamp;
    uint64_t max_key;
    uint64_t min_key;
    uint64_t count;
    std::bitset<FILTER_BITS> filter;
    std::vector<uint64_t> key_list;
    std::vector<uint32_t> offset_list;
};
enum LevelType
{
    Leveling,
    Tiering,
};
extern uint64_t current_timestamp;
extern std::map<uint32_t, std::pair<uint32_t, LevelType>> config_level;
extern uint32_t merge_file_count;
extern std::map<uint32_t, std::map<std::string, IndexData>> all_sst_index;
