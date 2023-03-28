//
// Created by yanjs on 2023/3/7.
//


#include "kvstore.h"
#include "global.h"

static const bool tiny = true;
int mode = 0;
static const uint64_t tiny_data_size = 128;
static const uint64_t tiny_size_num = 1024 * 512;
static const uint64_t large_data_size = 1024 * 64;
static const uint64_t large_size_num = 1024;
static const uint64_t data_num = tiny ? tiny_size_num : large_size_num;
static const uint64_t data_size = tiny ? tiny_data_size : large_data_size;

void flush_buffer(KVStore &store, uint64_t size)
{
    uint64_t count = (1024 * 10) / 128;
    for (uint64_t i = size; i < size + count + 1; ++i) {
        store.put(i, std::string(1024 * 128, 's'));
    }
}

void random_put_test(KVStore &store, uint64_t size)
{
    std::vector<uint64_t> arr;
    for (uint64_t i = 0; i < size; ++i) {
        arr.emplace_back(i);
    }
    std::shuffle(arr.begin(), arr.end(), std::mt19937(std::random_device()()));
    auto start = std::chrono::steady_clock::now();
    for (uint64_t i = 0; i < size; ++i) {
        store.put(arr[i], std::string(data_size, 's'));
    }
    auto end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration<double>(end - start).count();
    auto average = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count() / size;
    printf("random put sum:%fs average:%luns\n", duration, average);
}

void series_put_test(KVStore &store, uint64_t size)
{
    auto start = std::chrono::steady_clock::now();
    for (uint64_t i = 0; i < size; ++i) {
        store.put(i, std::string(data_size, 's'));
    }
    auto end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration<double>(end - start).count();
    auto average = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count() / size;
    printf("series put sum:%fs average:%luns\n", duration, average);
}

void random_get_test(KVStore &store, uint64_t size)
{
    std::vector<uint64_t> arr;
    for (uint64_t i = 0; i < size; ++i) {
        arr.emplace_back(i);
    }
    std::shuffle(arr.begin(), arr.end(), std::mt19937(std::random_device()()));
    auto start = std::chrono::steady_clock::now();
    for (uint64_t i = 0; i < size; ++i) {
        store.get(arr[i]);
    }
    auto end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration<double>(end - start).count();
    auto average = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count() / size;
    printf("random get sum:%fs average:%luns\n", duration, average);
}

void series_get_test(KVStore &store, uint64_t size)
{
    auto start = std::chrono::steady_clock::now();
    for (uint64_t i = 0; i < size; ++i) {
        store.get(i);
    }
    auto end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration<double>(end - start).count();
    auto average = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count() / size;
    printf("series get sum:%fs average:%luns\n", duration, average);
}

void random_del_test(KVStore &store, uint64_t size)
{
    std::vector<uint64_t> arr;
    for (uint64_t i = 0; i < size; ++i) {
        arr.emplace_back(i);
    }
    std::shuffle(arr.begin(), arr.end(), std::mt19937(std::random_device()()));
    auto start = std::chrono::steady_clock::now();
    for (uint64_t i = 0; i < size; ++i) {
        store.del(arr[i]);
    }
    auto end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration<double>(end - start).count();
    auto average = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count() / size;
    printf("random del sum:%fs average:%luns\n", duration, average);
}

void series_del_test(KVStore &store, uint64_t size)
{
    auto start = std::chrono::steady_clock::now();
    for (uint64_t i = 0; i < size; ++i) {
        store.del(i);
    }
    auto end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration<double>(end - start).count();
    auto average = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count() / size;
    printf("series del sum:%fs average:%luns\n", duration, average);
}

void range_get_test(KVStore &store, uint64_t size)
{
    std::list<std::pair<uint64_t, std::string>> arr;
    auto start = std::chrono::steady_clock::now();
    store.scan(0, size / 2, arr);
    auto end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration<double>(end - start).count();
    auto average = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count() / (size / 2);
    printf("range sum:%fs average:%luns\n", duration, average);
}

void normal_test(KVStore &store, uint64_t size)
{
    store.reset();
    printf("num:%lu size:%luMB\n", size, data_size * size / (1024 * 1024));
    series_put_test(store, size);
//    flush_buffer(store, size);
    series_get_test(store, size);
    series_del_test(store, size);
    store.reset();
    random_put_test(store, size);
//    flush_buffer(store, size);
    random_get_test(store, size);
    random_del_test(store, size);
}

void compaction_test(KVStore &store)
{
    for (uint64_t i = 0; i < large_size_num / 4; ++i) {
        auto start = std::chrono::steady_clock::now();
        store.put(i, std::string(large_data_size * 4, 's'));
        auto end = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        printf("%lu\n", duration);
    }
}

void config_test(KVStore &store)
{
    uint32_t divide = 20;
    config_level[0] = std::make_pair(2, Tiering);
    for (uint32_t i = 1; i < divide; ++i) {
        config_level[i] = std::make_pair(1 << (i + 1), Tiering);
    }
    config_level[4] = std::make_pair(1 << (20), Leveling);
//    for (uint32_t i = divide; i < 20; ++i) {
//        config_level[i] = std::make_pair(1 << (i + 1), Leveling);
//    }
    normal_test(store, data_num * 4);
//    series_put_test(store, data_num * 4);
//    range_get_test(store, data_num * 4);
//    store.reset();
//    random_put_test(store, data_num * 4);
//    range_get_test(store, data_num * 4);
//    store.reset();
}

void buffer_test(KVStore &store)
{
    normal_test(store, data_num / 8);
}

int main()
{
    KVStore store(DATA_PATH);
    std::vector<uint64_t> size_list;
    for (uint64_t i = 1; i <= 6; ++i) {
        size_list.emplace_back(1 << i);
    }
    switch (mode) {
        case 0:
            normal_test(store, data_num);
            break;
        case 1:
            compaction_test(store);
            break;
        case 2:
            config_test(store);
            break;
        case 3:
            buffer_test(store);
            break;
        default:
            assert(0);
    }

    return 0;
}