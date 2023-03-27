//
// Created by yanjs on 2023/3/7.
//

#pragma once

#include "constant.h"


struct IndexData
{
    uint64_t max_key;
    uint64_t min_key;
    uint64_t count;
    std::bitset<FILTER_BITS> filter;
    std::vector<uint64_t> key_list;
    std::vector<uint32_t> offset_list;
};

class Node
{
public:
    uint64_t key;
    std::string value;
    int level;
    Node **next;

    Node() : key(0), value(""), level(0), next(nullptr)
    {}

    Node(uint64_t k, std::string v, int l) : key(k), value(std::move(v)), level(l)
    {
        next = new Node *[l + 1];
    }

    ~Node()
    {
        delete next;
    }
};

class MemTable
{
public:
    Node *head;
    Node *tail;
    int level;
    size_t bytes_num;

    static uint32_t randomLevel();

public:
    MemTable();

    ~MemTable();

    bool put(uint64_t key, const std::string &value);

    std::string get(uint64_t key) const;

    bool del(uint64_t key) const;

    void scan(uint64_t key1, uint64_t key2, std::map<uint64_t, std::string> &list) const;

    void to_sst_file(const std::string &dir) const;

    void to_sst_file_index(const std::string &dir, IndexData &indexData) const;
};
