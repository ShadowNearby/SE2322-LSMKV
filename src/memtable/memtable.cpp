//
// Created by yanjs on 2023/3/7.
//

#include "memtable.h"

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
    if (bytes + bytes_num > MAX_BYTES) { return false; }
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
    int newLevel = randomLevel();
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

void MemTable::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) const
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
            list.emplace_back(current->key, current->value);
        }
        current = current->next[0];
    }
}

int MemTable::randomLevel()
{
    int cur_level = 1;
    auto random_generator = std::mt19937(std::random_device()());
    std::uniform_int_distribution<int> dist(0, TABLE_MASK);
    while ((int) (dist(random_generator) & TABLE_MASK) < (TABLE_P * TABLE_MASK)) {
        cur_level++;
    }
    return cur_level < TABLE_MAX_LEVEL ? cur_level : TABLE_MAX_LEVEL;
}
