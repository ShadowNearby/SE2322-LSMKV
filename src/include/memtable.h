//
// Created by yanjs on 2023/3/7.
//

#pragma once

#include <string>
#include <utility>
#include <random>

const double TABLE_P = (double) 1 / 3;
const int TABLE_MAX_LEVEL = 32;
const int TABLE_MASK = 0xfff;

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
private:
    Node *head;
    Node *tail;
    int level;

    int randomLevel();

public:
    MemTable();

    ~MemTable()
    {
        delete head;
        delete tail;
    };

    void insert(uint64_t key, const std::string &value);

    std::string find(uint64_t key);
};
