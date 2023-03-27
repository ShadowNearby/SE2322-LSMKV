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

extern uint64_t current_timestamp;
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