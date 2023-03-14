//
// Created by yanjs on 2023/3/7.
//

#pragma once

#include <string>
#include <utility>
#include <random>
#include <list>

const double TABLE_P = 0.25;
const int TABLE_MAX_LEVEL = 32;
const int TABLE_MASK = 0xfff;
const int INIT_BYTES = 10240 + 32;
const int MAX_BYTES = 1024 * 1024 * 2 - INIT_BYTES;

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
    int bytes_num;

    static int randomLevel();

public:
    MemTable();

    ~MemTable();

    bool put(const uint64_t key, const std::string &value);

    std::string get(uint64_t key) const;

    bool del(uint64_t key) const;

    void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) const;
};
