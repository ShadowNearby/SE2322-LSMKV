//
// Created by yanjs on 2023/3/7.
//

#include "sstable.h"


SSTable::SSTable() : count(0), max_key(0), min_key(0), filter(std::bitset<FILTER_BITS>(0))
{

}

void SSTable::to_sst_file(const std::string &dir)
{
    std::fstream f;
    f.open(dir + std::to_string(timestamp) + ".sst", std::ios::binary | std::ios::out);
    current_timestamp++;
    timestamp = current_timestamp;
    count = table_map.size();
    max_key = (--table_map.end())->first;
    min_key = table_map.begin()->first;
    size_t offset = 4 * 8 + count * 12 + FILTER_BYTES;
    char *data = new char[TABLE_BYTES];
    char *current = data;
    long_to_bytes(timestamp, &current);
    long_to_bytes(count, &current);
    long_to_bytes(max_key, &current);
    long_to_bytes(min_key, &current);
    current += FILTER_BYTES;
    char *pos_offset = data + offset;
    for (auto &it: table_map) {
        long_to_bytes(it.first, &current);
        int_to_bytes(offset, &current);
        string_to_bytes(it.second, &pos_offset);
        offset += it.second.size();
        filter_hash(it.first);
    }
    current = 4 * 8 + data;
    std::string filter_str = filter.to_string();
    for (uint32_t i = 0; i < FILTER_LONGS; ++i) {
        long_to_bytes(std::bitset<64>(filter_str.substr(64 * i, 64)).to_ullong(), &current);
    }
    f.write(data, TABLE_BYTES);
    delete[] data;
}

bool SSTable::read_sst_file(const std::string &file_path)
{
    std::fstream f;
    f.open(file_path, std::ios::binary | std::ios::in);
    if (!f.is_open())
        return false;
    char *data = new char[TABLE_BYTES];
    f.read(data, TABLE_BYTES);
    char *current = data;
    timestamp = bytes_to_long(&current);
    count = bytes_to_long(&current);
    max_key = bytes_to_long(&current);
    min_key = bytes_to_long(&current);
    std::string filter_string;
    for (uint32_t i = 0; i < FILTER_LONGS; ++i) {
        filter_string += std::bitset<64>(bytes_to_long(&current)).to_string();
    }
    filter = std::bitset<FILTER_BITS>(filter_string);
    std::vector<uint64_t> key_vec(count);
    std::vector<uint32_t> offset_vec(count + 1);
    offset_vec[count] = TABLE_BYTES * 8;
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
    return true;
}

bool SSTable::read_sst_header(const std::string &file_path)
{
    std::fstream f;
    f.open(file_path, std::ios::binary | std::ios::in);
    if (!f.is_open())
        return false;
    char *data = new char[HEADER_BYTES];
    f.read(data, HEADER_BYTES);
    char *current = data;
    timestamp = bytes_to_long(&current);
    count = bytes_to_long(&current);
    max_key = bytes_to_long(&current);
    min_key = bytes_to_long(&current);
    std::string filter_string;
    for (uint32_t i = 0; i < FILTER_LONGS; ++i) {
        filter_string += std::bitset<64>(bytes_to_long(&current)).to_string();
    }
    filter = std::bitset<FILTER_BITS>(filter_string);
    delete[] data;
    return true;
}

bool SSTable::key_exist(const std::string &file_path, uint64_t key)
{
    std::fstream f;
    f.open(file_path, std::ios::binary | std::ios::in);
    if (!f.is_open())
        return false;
    char *header_data = new char[HEADER_BYTES];
    f.read(header_data, HEADER_BYTES);
    char *current = header_data + 8;
    auto count_exist = bytes_to_long(&current);
    auto max_key_exist = bytes_to_long(&current);
    if (key > max_key_exist) {
        delete[] header_data;
        return false;
    }
    auto min_key_exist = bytes_to_long(&current);
    if (key < min_key_exist) {
        delete[] header_data;
        return false;
    }
    std::string filter_string;
    for (uint32_t i = 0; i < FILTER_LONGS; ++i) {
        filter_string += std::bitset<64>(bytes_to_long(&current)).to_string();
    }
    auto com_filter = std::bitset<FILTER_BITS>(filter_string);
    uint32_t hash[4] = {0};
    MurmurHash3_x64_128(&key, 8, 1, hash);
    for (unsigned int i: hash) {
        if (!com_filter[i % FILTER_BITS]) {
            delete[] header_data;
            return false;
        }
    }
    delete[] header_data;
    f.seekg(HEADER_BYTES);
    char *keys_data = new char[count_exist * 12];
    f.read(keys_data, (long) count_exist * 12);
    uint64_t left = 0;
    uint64_t right = count_exist;
    while (left <= right) {
        uint64_t mid = (left + right) / 2;
        current = mid * 12 + keys_data;
        uint64_t key_find = bytes_to_long(&current);
        if (key_find == key) {
            delete[] keys_data;
            return true;
        } else if (key_find < key) {
            left = mid + 1;
        } else if (key_find > key) {
            right = mid - 1;
        }
    }
    delete[] keys_data;
    return false;
}

SSTable::SSTable(const MemTable &mem_table)
{
    auto it = mem_table.head->next[0];
    while (it != mem_table.tail) {
        table_map[it->key] = it->value;
        it = it->next[0];
    }
    max_key = (--table_map.end())->first;
    min_key = table_map.begin()->first;
    count = table_map.size();
}

void SSTable::filter_hash(uint64_t key)
{
    uint32_t hash[4] = {0};
    MurmurHash3_x64_128(&key, 8, 1, hash);
    for (unsigned int i: hash) {
        filter[i % FILTER_BITS] = true;
    }
}

std::string SSTable::get_value_all_disk(const std::string &file_path, uint64_t key)
{
    std::fstream f;
    f.open(file_path, std::ios::binary | std::ios::in);
    if (!f.is_open())
        return "";
    char *header_data = new char[HEADER_BYTES];
    f.read(header_data, HEADER_BYTES);
    char *current = header_data + 8;
    uint64_t count_exist = bytes_to_long(&current);
    uint64_t max_key_exist = bytes_to_long(&current);
    if (key > max_key_exist) {
        f.close();
        delete[] header_data;
        return "";
    }
    auto min_key_exist = bytes_to_long(&current);
    if (key < min_key_exist) {
        f.close();
        delete[] header_data;
        return "";
    }
    std::string filter_string;
    for (uint32_t i = 0; i < FILTER_LONGS; ++i) {
        filter_string += std::bitset<64>(bytes_to_long(&current)).to_string();
    }
    auto com_filter = std::bitset<FILTER_BITS>(filter_string);
    uint32_t hash[4] = {0};
    MurmurHash3_x64_128(&key, 8, 1, hash);
    for (unsigned int i: hash) {
        if (!com_filter[i % FILTER_BITS]) {
            f.close();
            delete[] header_data;
            return "";
        }
    }
    delete[] header_data;
    f.seekg(HEADER_BYTES);
    char *keys_data = new char[count_exist * 12];
    f.read(keys_data, (long) count_exist * 12);
    uint64_t left = 0;
    uint64_t right = count_exist - 1;
    while (left <= right) {
        uint64_t mid = (left + right) / 2;
        current = mid * 12 + keys_data;
        uint64_t key_find = bytes_to_long(&current);
        if (key_find == key) {
            f.seekg(0, std::fstream::beg);
            long file_size = f.tellg();
            f.seekg(0, std::fstream::end);
            file_size = f.tellg() - file_size;
            uint32_t offset_left = bytes_to_int(&current);
            current += 8;
            uint32_t offset_right = (count_exist - 1 == mid) ? file_size : bytes_to_int(&current);
            delete[] keys_data;
            uint32_t len = offset_right - offset_left;
            char *result_c_str = new char[len + 1];
            result_c_str[len] = '\0';
            f.seekg(HEADER_BYTES + (long) count_exist * 12);
            f.read(result_c_str, len);
            std::string result(result_c_str);
            delete[] result_c_str;
            f.close();
            return result;
        } else if (key_find < key) {
            left = mid + 1;
        } else if (key_find > key) {
            right = mid - 1;
        }
    }
    f.close();
    delete[] keys_data;
    return "";
}

void
SSTable::scan_value(const std::string &file_path, uint64_t key1, uint64_t key2, std::map<uint64_t, std::string> &list)
{
    std::fstream f;
    f.open(file_path, std::ios::binary | std::ios::in);
    if (!f.is_open())
        return;
    char *header_data = new char[8 * 4];
    f.read(header_data, 8 * 4);
    char *current = header_data + 8;
    uint64_t count_exist = bytes_to_long(&current);
    uint64_t max_key_exist = bytes_to_long(&current);
    uint64_t min_key_exist = bytes_to_long(&current);
    delete[] header_data;
    if (key1 > max_key_exist || key2 < min_key_exist) {
        f.close();
        return;
    }
    f.seekg(0, std::fstream::beg);
    long file_size = f.tellg();
    f.seekg(0, std::fstream::end);
    file_size = f.tellg() - file_size;
    f.seekg(HEADER_BYTES);
    std::vector<uint64_t> keys;
    std::vector<uint32_t> offsets;
    char *keys_data = new char[count_exist * 12];
    f.read(keys_data, (long) count_exist * 12);
    current = keys_data;
    for (uint64_t i = 0; i < count_exist; ++i) {
        uint64_t key = bytes_to_long(&current);
        if (key < key1) {
            current += 4;
            continue;
        }
        if (key > key2)
            break;
        keys.emplace_back(key);
        offsets.emplace_back(bytes_to_int(&current));
    }
    uint64_t target_len = keys.size();
    if (!target_len) {
        delete[] keys_data;
        f.close();
        return;
    }
    if (keys[target_len - 1] == max_key_exist) {
        offsets.emplace_back(file_size);
    } else {
        offsets.emplace_back(bytes_to_int(&current));
    }
    delete[]keys_data;
    uint32_t data_size = offsets[target_len] - offsets[0];
    char *data = new char[data_size];
    f.seekg(offsets[0]);
    f.read(data, data_size);
    current = data;
    for (uint64_t i = 0; i < target_len; ++i) {
        uint32_t len = offsets[i + 1] - offsets[i];
        std::string value = bytes_to_string(&current, len);
        if (value.empty() || value == "~DELETE~")
            continue;
        list[keys[i]] = value;
    }
    delete[] data;
    f.close();
}

std::string SSTable::get_value_index(const std::string &file_path, uint64_t key, const IndexData &index_data)
{
    uint64_t count_exist = index_data.count;
    uint64_t max_key_exist = index_data.max_key;
    if (key > max_key_exist) {
        return "";
    }
    auto min_key_exist = index_data.min_key;
    if (key < min_key_exist) {
        return "";
    }
    uint32_t hash[4] = {0};
    MurmurHash3_x64_128(&key, 8, 1, hash);
    for (unsigned int i: hash) {
        if (!index_data.filter[i % FILTER_BITS]) {
            return "";
        }
    }
    std::fstream f;
    f.open(file_path, std::ios::binary | std::ios::in);
    if (!f.is_open())
        return "";
    const auto &key_list = index_data.key_list;
    const auto &offset_list = index_data.offset_list;
    uint64_t left = 0;
    uint64_t right = count_exist - 1;
    while (left <= right) {
        uint64_t mid = (left + right) / 2;
        uint64_t key_find = key_list[mid];
        if (key_find == key) {
            uint32_t offset_left = offset_list[mid];
            uint32_t offset_right = offset_list[mid + 1];
            uint32_t len = offset_right - offset_left;
            char *result_c_str = new char[len + 1];
            result_c_str[len] = '\0';
            f.seekg(offset_left);
            f.read(result_c_str, len);
            std::string result(result_c_str);
            delete[] result_c_str;
            f.close();
            return result;
        } else if (key_find < key) {
            left = mid + 1;
        } else if (key_find > key) {
            right = mid - 1;
        }
    }
    f.close();
    return "";
}

void SSTable::merge(const std::string &data_dir)
{
    for (const auto &sst_level_it: all_sst_index) {
        const auto &current_sst_level_index = sst_level_it.first;
        const auto &current_sst_level = sst_level_it.second;
        uint32_t current_level_max_size = 1 << (current_sst_level_index + 1);
        if (current_sst_level.size() <= current_level_max_size)
            return;
        if (all_sst_index[current_sst_level_index + 1].empty()) {
            std::string level_dir = data_dir + "level-" + std::to_string(current_sst_level_index + 1);
            utils::mkdir(level_dir.c_str());
        }
        std::vector<std::string> select_current_level_sst_path;
        std::vector<uint64_t> current_level_max_key_list;
        std::vector<uint64_t> current_level_min_key_list;
        std::map<uint64_t, std::string> target;
        uint32_t count = current_sst_level_index == 0 ? current_sst_level.size() : current_sst_level.size() -
                                                                                   current_level_max_size;
        auto sst_it_begin = current_sst_level.begin();
        for (uint32_t i = 0; i < count; ++i, ++sst_it_begin) {
            const auto &sst_index = sst_it_begin->second;
            current_level_max_key_list.emplace_back(sst_index.max_key);
            current_level_min_key_list.emplace_back(sst_index.min_key);
        }
        uint64_t current_level_max_key = *std::max_element(current_level_max_key_list.begin(),
                                                           current_level_max_key_list.end());
        uint64_t current_level_min_key = *std::min_element(current_level_min_key_list.begin(),
                                                           current_level_min_key_list.end());
        const auto next_sst_level = all_sst_index[current_sst_level_index + 1];
        std::vector<std::string> select_next_level_sst_path;
        for (const auto &sst_it: next_sst_level) {
            const auto &sst_index = sst_it.second;
            const auto &sst_path = sst_it.first;
            if (sst_index.max_key <= current_level_max_key || sst_index.min_key >= current_level_min_key) {
                read_sst_to_map(sst_path, sst_index, target);
                select_next_level_sst_path.emplace_back(sst_path);
            }
        }
        sst_it_begin = current_sst_level.begin();
        for (uint32_t i = 0; i < count; ++i, ++sst_it_begin) {
            const auto &sst_path = sst_it_begin->first;
            const auto &sst_index = sst_it_begin->second;
            read_sst_to_map(sst_path, sst_index, target);
            select_current_level_sst_path.emplace_back(sst_path);
        }
        for (const auto &sst_path: select_current_level_sst_path) {
            all_sst_index[current_sst_level_index].erase(sst_path);
            utils::rmfile(sst_path.c_str());
        }
        for (const auto &sst_path: select_next_level_sst_path) {
            all_sst_index[current_sst_level_index + 1].erase(sst_path);
            utils::rmfile(sst_path.c_str());
        }
        maps_to_sst(current_sst_level_index + 1, data_dir, target);
    }
}

void
SSTable::read_sst_to_map(const std::string &file_path, const IndexData &index_data,
                         std::map<uint64_t, std::string> &target)
{
    std::fstream f;
    f.open(file_path, std::ios::binary | std::ios::in);
    if (!f.is_open())
        return;
    char *data = new char[TABLE_BYTES];
    f.read(data, TABLE_BYTES);
    char *current = data + HEADER_BYTES + index_data.count * 12;
    for (uint64_t i = 0; i < index_data.count; ++i) {
        uint32_t index_begin = index_data.offset_list[i];
        uint32_t index_end = index_data.offset_list[i + 1];
        uint32_t len = index_end - index_begin;
        target[index_data.key_list[i]] = bytes_to_string(&current, len);
    }
    delete[] data;
}

void SSTable::get_newest_sst(uint32_t n, uint32_t level, std::vector<std::string> &target)
{
    const auto &current_level = all_sst_index[level];
    std::map<uint64_t, std::string> time_map;
    for (const auto &sst_it: current_level) {
        const auto &sst_path = sst_it.first;
        const auto &sst_timestamp = sst_it.second.timestamp;
        time_map[sst_timestamp] = sst_path;
    }
    auto it = current_level.begin();
    for (uint32_t i = 0; i < n; ++i, ++it) {
        target.emplace_back(it->first);
    }
}

void SSTable::maps_to_sst(uint32_t level, const std::string &data_dir, const std::map<uint64_t, std::string> &target)
{
    auto &current_level = all_sst_index[level];
    int last_sst_number = -1;
    if (!current_level.empty()) {
        auto last = current_level.end()--;
        std::string last_sst_path = last->first;
        last_sst_number = (int) sst_number(last_sst_path);
    }
    std::map<uint64_t, std::string> one_sst_data;
    uint64_t bytes_sum = HEADER_BYTES;
    for (const auto &it: target) {
        auto key = it.first;
        auto &value = it.second;
        uint32_t len = value.size();
        if (bytes_sum + len + 12 > TABLE_BYTES) {
            last_sst_number++;
            std::string file_path =
                    data_dir + "level-" + std::to_string(level) + "/" + std::to_string(last_sst_number) + ".sst";
            one_map_to_sst(level, file_path, one_sst_data);
            one_sst_data.clear();
            bytes_sum = HEADER_BYTES;
        }
        one_sst_data[key] = value;
        bytes_sum += 12 + len;
    }
    if (!one_sst_data.empty()) {
        last_sst_number++;
        std::string file_path =
                data_dir + "level-" + std::to_string(level) + "/" + std::to_string(last_sst_number) + ".sst";
        one_map_to_sst(level, file_path, one_sst_data);
        one_sst_data.clear();
    }
    current_timestamp++;
}

void
SSTable::one_map_to_sst(uint32_t level, const std::string &file_path, const std::map<uint64_t, std::string> &target)
{
    IndexData index_data;
    uint64_t max_key_mem = (--target.end())->first;
    uint64_t min_key_mem = target.begin()->first;
    uint64_t count = target.size();
    current_timestamp++;
    uint64_t timestamp = current_timestamp;
    size_t offset = 4 * 8 + count * 12 + FILTER_BYTES;
    char *data = new char[TABLE_BYTES];
    char *current = data;
    long_to_bytes(timestamp, &current);
    long_to_bytes(count, &current);
    long_to_bytes(max_key_mem, &current);
    long_to_bytes(min_key_mem, &current);
    index_data.timestamp = timestamp;
    index_data.count = count;
    index_data.min_key = min_key_mem;
    index_data.max_key = max_key_mem;
    current += FILTER_BYTES;
    char *pos_offset = data + offset;
    uint32_t bytes_sum = HEADER_BYTES + count * 12;
    std::bitset<FILTER_BITS> filter;
    for (auto &it: target) {
        index_data.key_list.push_back(it.first);
        index_data.offset_list.push_back(offset);
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
            index_data.filter[i % FILTER_BITS] = true;
        }
    }
    current = 4 * 8 + data;
    std::string filter_str = filter.to_string();
    for (uint32_t i = 0; i < FILTER_LONGS; ++i) {
        long_to_bytes(std::bitset<64>(filter_str.substr(64 * i, 64)).to_ullong(), &current);
    }
    std::fstream f;
    f.open(file_path, std::ios::binary | std::ios::out);
    index_data.offset_list.push_back(bytes_sum);
    all_sst_index[level][file_path] = index_data;
    f.write(data, bytes_sum);
    delete[] data;
}

uint32_t SSTable::sst_number(const std::string &file_path)
{
    auto left = file_path.find_last_of('/');
    auto right = file_path.find_last_of('.');
    auto len = right - left;
    auto number_str = file_path.substr(left, len);
    return stoul(number_str);
}
