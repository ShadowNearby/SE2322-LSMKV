//
// Created by yanjs on 2023/3/7.
//

#include "sstable.h"


uint64_t SSTable::current_timestamp = 0;

SSTable::SSTable() : count(0), max_key(0), min_key(0)
{
    current_timestamp++;
    timestamp = current_timestamp;
}

void SSTable::to_sst_file(const std::string &dir)
{
    count = table_map.size();
    max_key = table_map.end()->first;
    min_key = table_map.begin()->first;
    size_t offset = 4 * 8 + count * 12 + 10240;
    std::fstream f;
    f.open(dir + std::to_string(timestamp) + ".sst", std::ios::binary | std::ios::out);
    char *data = new char[TABLE_BYTES];
    char *current = data;
    long_to_bytes(timestamp, &current);
    long_to_bytes(count, &current);
    long_to_bytes(max_key, &current);
    long_to_bytes(min_key, &current);
    std::string filter_str = filter.to_string();
    for (int i = 0; i < FILTER_LONGS; ++i) {
        long_to_bytes(std::bitset<64>(filter_str.substr(64 * i, 64)).to_ullong(), &current);
    }
    char *pos_offset = data + offset;
    for (auto &it: table_map) {
        long_to_bytes(it.first, &current);
        int_to_bytes(offset, &current);
        string_to_bytes(it.second, &pos_offset);
        offset += it.second.size();
    }
    f.write(data, TABLE_BYTES);
    delete[] data;
}

void SSTable::read_sst_file(const std::string &file_path)
{
    std::fstream f;
    f.open(file_path, std::ios::binary | std::ios::in);
    char *data = new char[TABLE_BYTES];
    f.read(data, TABLE_BYTES);
    char *current = data;
    timestamp = bytes_to_long(&current);
    count = bytes_to_long(&current);
    max_key = bytes_to_long(&current);
    min_key = bytes_to_long(&current);
    std::string filter_string;
    for (int i = 0; i < FILTER_LONGS; ++i) {
        filter_string += std::bitset<64>(bytes_to_long(&current)).to_string();
    }
    filter = std::bitset<10240>(filter_string);
    std::vector<uint64_t> key_vec(count);
    std::vector<uint32_t> offset_vec(count + 1);
    offset_vec[count] = TABLE_BYTES;
    for (uint64_t i = 0; i < count; ++i) {
        uint64_t key = bytes_to_long(&current);
        uint32_t offset = bytes_to_int(&current);
        key_vec[i] = key;
        offset_vec[i] = offset;
    }
    for (uint64_t i = 0; i < count; ++i) {
        uint32_t index_begin = offset_vec[i];
        uint32_t index_end = offset_vec[i + 1];
        uint32_t len = index_end - index_begin;
        table_map[key_vec[i]] = bytes_to_string(&current, len);
    }
    delete[] data;
}

void SSTable::read_sst_header(const std::string &file_path)
{
    std::fstream f;
    f.open(file_path, std::ios::binary | std::ios::in);
    char *data = new char[HEADER_BYTES];
    f.read(data, HEADER_BYTES);
    char *current = data;
    timestamp = bytes_to_long(&current);
    count = bytes_to_long(&current);
    max_key = bytes_to_long(&current);
    min_key = bytes_to_long(&current);
    std::string filter_string;
    for (int i = 0; i < FILTER_LONGS; ++i) {
        filter_string += std::bitset<64>(bytes_to_long(&current)).to_string();
    }
    filter = std::bitset<10240>(filter_string);
}

bool SSTable::key_exist(const std::string &file_path, uint64_t key)
{
    std::fstream f;
    f.open(file_path, std::ios::binary | std::ios::in);
    char *data = new char[TABLE_BYTES];
    f.read(data, TABLE_BYTES);
    char *current = data;
    bytes_to_long(&current);
    bytes_to_long(&current);
    auto max_key_exist = bytes_to_long(&current);
    if (key > max_key_exist)
        return false;
    auto min_key_exist = bytes_to_long(&current);
    if (key < min_key_exist)
        return false;
//    可优化
    std::string filter_string;
    for (int i = 0; i < FILTER_LONGS; ++i) {
        filter_string += std::bitset<64>(bytes_to_long(&current)).to_string();
    }
    auto com_filter = std::bitset<10240>(filter_string);
    uint32_t hash[4] = {0};
    MurmurHash3_x64_128(&key, 8, 1, hash);
    for (unsigned int i: hash) {
        if (!com_filter[i % 10240])
            return false;
    }
    return true;
}

SSTable::SSTable(const MemTable &mem_table)
{
    auto it = mem_table.head->next[0];
    while (it != mem_table.tail) {
        table_map[it->key] = it->value;
        it = it->next[0];
    }
    timestamp = current_timestamp;
    current_timestamp++;
    max_key = table_map.end()->first;
    min_key = table_map.begin()->first;
    count = table_map.size();
}

void SSTable::filter_hash(uint64_t key)
{
    uint32_t hash[4] = {0};
    MurmurHash3_x64_128(&key, 8, 1, hash);
    for (unsigned int i: hash) {
        filter[i % 10240] = true;
    }
}

bool SSTable::filter_exist(uint64_t key)
{
    uint32_t hash[4] = {0};
    MurmurHash3_x64_128(&key, 8, 1, hash);
    for (unsigned int i: hash) {
        if (!filter[i % 10240])
            return false;
    }
    return true;
}

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
        if (ch == '\0') break;
        str.push_back(ch);
    }
    (*src) += len;
    return str;
}