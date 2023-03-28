#include "kvstore.h"


KVStore::KVStore(const std::string &dir) : KVStoreAPI(dir)
{
    data_dir = dir;
    table = new MemTable();
    std::vector<std::string> dir_list;
    utils::scanDir(data_dir, dir_list);
    all_sst_index[0] = std::map<std::string, IndexData>();
    for (const auto &level_str: dir_list) {
        uint32_t level_number = stoul(level_str.substr(level_str.find_last_of('-') + 1));
        std::vector<std::string> file_list;
        std::string dir_path = data_dir + level_str + "/";
        utils::scanDir(dir_path, file_list);
        for (const auto &file: file_list) {
            std::string file_path = dir_path + file;
            SSTable::read_sst_header_index(level_number, file_path);
        }
    }
}

KVStore::~KVStore()
{
    delete table;
    for (auto &sst_dir: all_sst_index) {
        for (const auto &sst: sst_dir.second) {
            utils::rmfile(sst.first.c_str());
        }
        std::string dir_path = data_dir + "level-" + std::to_string(sst_dir.first);
        utils::rmdir(dir_path.c_str());
    }
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    if (!table->put(key, s)) {
        auto &current_level = all_sst_index[0];
        std::string dir_path = data_dir + "level-0/";
        if (current_level.empty())
            utils::mkdir(dir_path.c_str());
        std::string file_path = dir_path + std::to_string(current_level.size()) + ".sst";
        table->to_sst_file_index(file_path);
        SSTable::merge(data_dir);
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
    if (result == "~DELETED~")
        return "";
    if (!result.empty())
        return result;
    uint64_t result_timestamp = 0;
    result = "";
    for (const auto &sst_dir: all_sst_index) {
        for (const auto &sst: sst_dir.second) {
            if (result_timestamp < sst.second.timestamp) {
                std::string value = SSTable::get_value_index(sst.first, key, sst.second);
                if (!value.empty()) {
                    result_timestamp = sst.second.timestamp;
                    result = value;
                }
            }
        }
    }
    if (result == "~DELETED~")
        return "";
    return result;
}

/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    std::string find_result = this->get(key);
    if (find_result.empty())
        return false;
    this->put(key, "~DELETED~");
    return true;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    delete table;
    table = new MemTable();
    for (auto &sst_dir: all_sst_index) {
        for (const auto &sst: sst_dir.second) {
            utils::rmfile(sst.first.c_str());
        }
        std::string dir_path = data_dir + "level-" + std::to_string(sst_dir.first);
        utils::rmdir(dir_path.c_str());
        sst_dir.second.clear();
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
    for (const auto &sst_dir: all_sst_index) {
        for (const auto &sst: sst_dir.second) {
            SSTable::scan_value(sst.first, key1, key2, key_map);
        }
    }
    for (const auto &item: key_map) {
        list.emplace_back(item.first, item.second);
    }
}