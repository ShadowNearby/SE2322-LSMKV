//
// Created by yanjs on 2023/3/7.
//
#include "memtable.h"
#include "sstable.h"


int main()
{
//    SSTable table;
//    auto &tmap = table.table_map;
//    tmap[0x2] = "12345678";
//    tmap[0x1] = "98765432";
//    table.to_sst_file();
//    SSTable table2;
//    table2.read_sst_file();
    std::map<int, int> m;
    m[0] = 10;
    std::cout << m.erase(0) << std::endl;
    std::cout << m.erase(0) << std::endl;
    return 0;
}