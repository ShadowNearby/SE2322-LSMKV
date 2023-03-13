//
// Created by yanjs on 2023/3/7.
//

#include "memtable.h"

MemTable::MemTable() : level(0)
{
    srand(time(nullptr));
    tail = new Node(INT32_MAX, "", 0);
    for (int i = tail->level; i >= 0; i--) {
        tail->next[i] = nullptr;
    }
    head = new Node(INT32_MAX, "", TABLE_MAX_LEVEL);
    for (int i = head->level; i >= 0; i--) {
        head->next[i] = tail;
    }
}

void MemTable::insert(const uint64_t key, const std::string &value)
{
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
        return;
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
    // maxLevel = log((double)length) / log(1 / p);
    // maxLevel = maxLevel > MAXLEVEL ? MAXLEVEL : maxLevel;
}

std::string MemTable::find(uint64_t key)
{
    Node *current = head;
    for (int i = level; i >= 0; i--) {
        while (current->next[i]->key < key) {
            current = current->next[i];
        }
    }
    if (current->next[0]->key == key) {
        return current->next[0]->value;
    }
    return tail->value;
}

int MemTable::randomLevel()
{
    int cur_level = 1;
    while ((int) (rand() & TABLE_MASK) < (TABLE_P * TABLE_MASK)) {
        cur_level++;
    }
    return cur_level < TABLE_MAX_LEVEL ? cur_level : TABLE_MAX_LEVEL;
}
