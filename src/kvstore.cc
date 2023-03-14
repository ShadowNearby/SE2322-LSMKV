#include "kvstore.h"
#include <string>

KVStore::KVStore(const std::string &dir) : KVStoreAPI(dir)
{
    data_dir = dir;
    table = new MemTable();
}

KVStore::~KVStore()
{
    delete table;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    if (!table->put(key, s)) {
        SSTable new_sstable(*table);
        new_sstable.to_sst_file(data_dir);
        delete table;
        table = new MemTable();
        table->put(key, s);
    }
}

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
    auto from_skiplist = table->get(key);
    if (!from_skiplist.empty())
        return from_skiplist;
    for (int i = 0; i <= SSTable::current_timestamp; ++i) {
        std::string file_path = data_dir + std::to_string(i) + ".sst";
        if (SSTable::key_exist(file_path, key)) {
            SSTable newest_sst;
            newest_sst.read_sst_file(file_path);
            auto key_it = newest_sst.table_map.find(key);
            if (key_it == newest_sst.table_map.end())
                continue;
            if (key_it->second == "~DELETED~")
                return "";
            return key_it->second;
        }
    }
    return "";
}

/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    return table->del(key);
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    delete table;
    table = new MemTable();
    for (int i = 0; i <= SSTable::current_timestamp; ++i) {
        auto file_name = data_dir + std::to_string(i) + ".sst";
        std::remove(file_name.c_str());
    }
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list)
{
    table->scan(key1, key2, list);
}