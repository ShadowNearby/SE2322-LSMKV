#include "kvstore.h"


KVStore::KVStore(const std::string &dir) : KVStoreAPI(dir)
{
    data_dir = dir;
    table = new MemTable();
}

KVStore::~KVStore()
{
    for (uint64_t i = 0; i <= current_timestamp; ++i) {
        auto file_name = data_dir + std::to_string(i) + ".sst";
        std::remove(file_name.c_str());
    }
    delete table;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    if (!table->put(key, s)) {
        IndexData indexData;
        table->to_sst_file_index(data_dir, indexData);
        index_list.push_back(indexData);
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
    auto result = table->get(key);
    if (result == "~DELETE~")
        return "";
    if (!result.empty())
        return result;
    for (uint64_t i = 1; i <= current_timestamp; ++i) {
        std::string file_path = data_dir + std::to_string(i) + ".sst";
        result = SSTable::get_value_index(file_path, key, index_list[i - 1]);
        if (result == "~DELETE~")
            return "";
        if (!result.empty())
            return result;
    }
    return "";
}

/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    std::string find_result = get(key);
    if (find_result.empty())
        return false;
    return table->put(key, "~DELETE~");
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    delete table;
    table = new MemTable();
    for (uint64_t i = 0; i <= current_timestamp; ++i) {
        auto file_name = data_dir + std::to_string(i) + ".sst";
        std::remove(file_name.c_str());
    }
    current_timestamp = 0;
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list)
{
    std::map<uint64_t, std::string> key_map;
    table->scan(key1, key2, key_map);
    for (uint64_t i = 1; i <= current_timestamp; ++i) {
        std::string file_path = data_dir + std::to_string(i) + ".sst";
        SSTable::scan_value(file_path, key1, key2, key_map);
    }
    for (const auto &item: key_map) {
        list.emplace_back(item.first, item.second);
    }
}