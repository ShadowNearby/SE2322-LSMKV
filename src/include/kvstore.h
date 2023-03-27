#pragma once

#include "kvstore_api.h"
#include "MurmurHash3.h"
#include "sstable.h"

class KVStore : public KVStoreAPI
{
    // You can add your implementation here
private:
//    SSTable table;
    MemTable *table;
    std::string data_dir;
    std::vector<IndexData> index_list;
public:
    KVStore(const std::string &dir);

    ~KVStore();

    void put(uint64_t key, const std::string &s) override;

    std::string get(uint64_t key) override;

    bool del(uint64_t key) override;

    void reset() override;

    void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) override;
};
