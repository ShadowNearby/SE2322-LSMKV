//
// Created by yanjs on 2023/3/7.
//

#include "sstable.h"


uint64_t SSTable::current_time_stamp = 0;

SSTable::SSTable() : count(0), max_key(0), min_key(0)
{
    timestamp = current_time_stamp;
    printf("No.%zu sstable\n", timestamp);
    current_time_stamp++;
}

void SSTable::to_sst_file()
{
    count = table_map.size();
    max_key = table_map.end()->first;
    min_key = table_map.begin()->first;
    size_t offset = 4 * 8 + count * 12;
    std::fstream f;
    f.open("../out.dat", std::ios::binary | std::ios::out);
    char *data = new char[TABLE_BYTES];
    char *current = data;
    long_to_bytes(timestamp, &current);
    long_to_bytes(count, &current);
    long_to_bytes(max_key, &current);
    long_to_bytes(min_key, &current);
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

void SSTable::read_sst_file()
{
    std::fstream f;
    f.open("../out.dat", std::ios::binary | std::ios::in);
    char *data = new char[TABLE_BYTES];
    f.read(data, TABLE_BYTES);
    char *current = data;
    timestamp = bytes_to_long(&current);
    count = bytes_to_long(&current);
    max_key = bytes_to_long(&current);
    min_key = bytes_to_long(&current);
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
        printf("%s\n", table_map[key_vec[i]].c_str());
    }
    delete[] data;
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
        std::cout << ch << std::endl;
    }
    (*src) += len;
    return str;
}