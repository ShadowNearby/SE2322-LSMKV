//
// Created by yanjs on 2023/3/7.
//

#include "memtable.h"
#include "sstable.h"

MemTable::MemTable() : level(0), bytes_num(0)
{
    tail = new Node(UINT64_MAX, "", 0);
    for (int i = tail->level; i >= 0; i--) {
        tail->next[i] = nullptr;
    }
    head = new Node(UINT64_MAX, "", TABLE_MAX_LEVEL);
    for (int i = head->level; i >= 0; i--) {
        head->next[i] = tail;
    }
}

MemTable::~MemTable()
{
    auto current = head;
    auto p = current->next[0];
    while (true) {
        delete current;
        current = p;
        if (!current)
            break;
        p = p->next[0];
    }
}

bool MemTable::put(const uint64_t key, const std::string &value)
{
    size_t bytes = 12 + value.size();
    if (bytes + bytes_num > MAX_DATA_BYTES) { return false; }
    bytes_num += bytes;
    Node *update[TABLE_MAX_LEVEL + 1];
    Node *current = head;
    for (int i = level; i >= 0; i--) {
        while (current->next[i]->key < key) {
            current = current->next[i];
        }
        update[i] = current;
    }
    if (current->next[0]->key == key) {
        current->next[0]->value = value;
        return true;
    }
    int newLevel = (int) randomLevel();
    if (newLevel > level) {
        for (int i = level + 1; i < newLevel + 1; i++) {
            update[i] = head;
            level = newLevel;
        }
    }
    Node *newNode = new Node(key, value, newLevel);
    for (int i = newLevel; i >= 0; i--) {
        newNode->next[i] = update[i]->next[i];
        update[i]->next[i] = newNode;
    }
    return true;
}

std::string MemTable::get(uint64_t key) const
{
    Node *current = head;
    for (int i = level; i >= 0; i--) {
        while (current->next[i]->key < key) {
            current = current->next[i];
        }
    }
    if (current->next[0]->key == key) {
        if (current->next[0]->value == "~DELETED~") {
            return "";
        }
        return current->next[0]->value;
    }
    return "";
}

bool MemTable::del(uint64_t key) const
{
    Node *current = head;
    for (int i = level; i >= 0; i--) {
        while (current->next[i]->key < key) {
            current = current->next[i];
        }
    }
    if (current->next[0]->key == key) {
        if (current->next[0]->value == "~DELETED~")
            return false;
        current->next[0]->value = "~DELETED~";
        return true;
    }
    return false;
}

void MemTable::scan(uint64_t key1, uint64_t key2, std::map<uint64_t, std::string> &list) const
{
    Node *current = head;
    for (int i = level; i >= 0; i--) {
        while (current->next[i]->key < key1) {
            current = current->next[i];
        }
    }
    current = current->next[0];
    while (current->key <= key2) {
        if (current->value != "~DELETED~") {
            list[current->key] = current->value;
        }
        current = current->next[0];
    }
}

uint32_t MemTable::randomLevel()
{
    uint32_t cur_level = 1;
    auto random_generator = std::mt19937(std::random_device()());
    std::uniform_int_distribution<int> dist(0, TABLE_MASK);
    while ((uint32_t) (dist(random_generator) & TABLE_MASK) < (TABLE_P * TABLE_MASK)) {
        cur_level++;
    }
    return cur_level < TABLE_MAX_LEVEL ? cur_level : TABLE_MAX_LEVEL;
}

void MemTable::to_sst_file(const std::string &dir) const
{
    auto head_it = head->next[0];
    std::map<uint64_t, std::string> key_map;
    while (head_it != tail) {
        key_map[head_it->key] = head_it->value;
        head_it = head_it->next[0];
    }
    uint64_t max_key_mem = (--key_map.end())->first;
    uint64_t min_key_mem = key_map.begin()->first;
    uint64_t count = key_map.size();
    current_timestamp++;
    uint64_t timestamp = current_timestamp;
    size_t offset = 4 * 8 + count * 12 + FILTER_BYTES;
    std::fstream f;
    f.open(dir + std::to_string(timestamp) + ".sst", std::ios::binary | std::ios::out);
    char *data = new char[TABLE_BYTES];
    char *current = data;
    long_to_bytes(timestamp, &current);
    long_to_bytes(count, &current);
    long_to_bytes(max_key_mem, &current);
    long_to_bytes(min_key_mem, &current);
    current += FILTER_BYTES;
    char *pos_offset = data + offset;
    uint32_t bytes_sum = HEADER_BYTES + count * 12;
    std::bitset<FILTER_BITS> filter;
    for (auto &it: key_map) {
        long_to_bytes(it.first, &current);
        int_to_bytes(offset, &current);
        string_to_bytes(it.second, &pos_offset);
        offset += it.second.size();
        bytes_sum += it.second.size();
        uint32_t hash[4] = {0};
        uint64_t key = it.first;
        MurmurHash3_x64_128(&key, 8, 1, hash);
        for (unsigned int i: hash) {
            filter[i % FILTER_BITS] = true;
        }
    }
    current = 4 * 8 + data;
    std::string filter_str = filter.to_string();
    for (uint32_t i = 0; i < FILTER_LONGS; ++i) {
        long_to_bytes(std::bitset<64>(filter_str.substr(64 * i, 64)).to_ullong(), &current);
    }
    f.write(data, bytes_sum);
    delete[] data;
}

void MemTable::to_sst_file_index(const std::string &dir, IndexData &indexData) const
{
    auto head_it = head->next[0];
    std::map<uint64_t, std::string> key_map;
    while (head_it != tail) {
        key_map[head_it->key] = head_it->value;
        head_it = head_it->next[0];
    }
    uint64_t max_key_mem = (--key_map.end())->first;
    uint64_t min_key_mem = key_map.begin()->first;
    uint64_t count = key_map.size();
    current_timestamp++;
    uint64_t timestamp = current_timestamp;
    size_t offset = 4 * 8 + count * 12 + FILTER_BYTES;
    std::fstream f;
    f.open(dir + std::to_string(timestamp) + ".sst", std::ios::binary | std::ios::out);
    char *data = new char[TABLE_BYTES];
    char *current = data;
    long_to_bytes(timestamp, &current);
    long_to_bytes(count, &current);
    long_to_bytes(max_key_mem, &current);
    long_to_bytes(min_key_mem, &current);
    indexData.count = count;
    indexData.min_key = min_key_mem;
    indexData.max_key = max_key_mem;
    current += FILTER_BYTES;
    char *pos_offset = data + offset;
    uint32_t bytes_sum = HEADER_BYTES + count * 12;
    std::bitset<FILTER_BITS> filter;
    for (auto &it: key_map) {
        indexData.key_list.push_back(it.first);
        indexData.offset_list.push_back(offset);
        long_to_bytes(it.first, &current);
        int_to_bytes(offset, &current);
        string_to_bytes(it.second, &pos_offset);
        offset += it.second.size();
        bytes_sum += it.second.size();
        uint32_t hash[4] = {0};
        uint64_t key = it.first;
        MurmurHash3_x64_128(&key, 8, 1, hash);
        for (unsigned int i: hash) {
            filter[i % FILTER_BITS] = true;
            indexData.filter[i % FILTER_BITS] = true;
        }
    }
    current = 4 * 8 + data;
    std::string filter_str = filter.to_string();
    for (uint32_t i = 0; i < FILTER_LONGS; ++i) {
        long_to_bytes(std::bitset<64>(filter_str.substr(64 * i, 64)).to_ullong(), &current);
    }
    indexData.offset_list.push_back(bytes_sum);
    f.write(data, bytes_sum);
    delete[] data;
}