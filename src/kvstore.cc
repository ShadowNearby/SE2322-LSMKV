#include "./include/kvstore.h"
#include <string>

KVStore::KVStore(const std::string &dir) : KVStoreAPI(dir)
{
}

KVStore::~KVStore()
{
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    table.table_map[key] = s;
}

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
    std::string val = table.table_map[key];
    if (val.empty())
        table.table_map.erase(key);
    return val;
}

/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    return table.table_map.erase(key);
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    table.table_map.clear();
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list)
{
    for (const auto &it: table.table_map) {
        if (it.first >= key1 && it.first <= key2) {
            list.emplace_back(it.first, it.second);
        }
    }
}