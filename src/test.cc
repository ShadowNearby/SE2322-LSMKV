//
// Created by yanjs on 2023/3/7.
//
#include "memtable.h"
#include "sstable.h"
#include <cassert>

const std::string data_path = "../data/";

int main()
{
    int num = 2 * 1024 * 1024 / 64;
    std::string file_path = "../output.sst";
    std::fstream f;
    f.open(file_path, std::ios::binary | std::ios::out);
    for (int i = 0; i < num; ++i) {
        std::bitset<64> v(0x1);
        f << v;
    }
    f.close();
//    std::string file_path = data_path + std::to_string(0) + ".sst";
//    SSTable table;
//    for (int i = 0; i < 2 * 64 * 1024; ++i) {
//        table.table_map[i] = "1234";
//        table.filter_hash(i);
//    }
//    table.to_sst_file(data_path);
//    SSTable table1;
//    for (int i = 0; i < 100; ++i) {
//        assert(table1.key_exist(file_path, i));
//    }
    return 0;
}