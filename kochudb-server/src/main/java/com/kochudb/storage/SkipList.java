package com.kochudb.storage;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import com.kochudb.types.KochuDBSerde;

/**
 * SkipList is a probabilistic list that offers average O(log n) runtime for
 * insert, search, delete and update operations.
 * 
 * This implementation is thread safe. Delete operation is a soft-delete.
 */
public class SkipList<T extends KochuDBSerde<T>> {

    private AtomicInteger maxLevels;
    private int curLevel, length;
    private AtomicLong size;

    private SkipListNode<T> sentinel, head, tail;

    public final WriteLock writeLock;
    public final ReadLock readLock;
    private Random prob;

    /**
     * Constructor
     */
    public SkipList() {
        head = new SkipListNode<T>(null);
        tail = new SkipListNode<T>(null);

        head.right = tail;
        tail.left = head;

        sentinel = head;

        maxLevels = new AtomicInteger(0);
        curLevel = 0;

        prob = new Random();
        length = 0;
        size = new AtomicLong(0);

        ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();
        readLock = reentrantReadWriteLock.readLock();
        writeLock = reentrantReadWriteLock.writeLock();
    }

    /**
     * find the SkipListNode2<K> by the key. If not present in SkipList, find the
     * immediate left SkipListNode2<T> which has the floor value of key
     * (floorSkipListNode2(key))
     *
     * @param key key
     * @return a SkipListNode2<K> with the 'key' of floor(key)
     */
    public SkipListNode<T> find(T key) {
        SkipListNode<T> cur = head;

        while (true) {
            while (cur.right.data != null && cur.right.data.compareTo(key) <= 0)
                cur = cur.right;

            if (cur.down == null)
                break;

            cur = cur.down;
        }
        return cur;
    }

    /**
     * find a SkipListNode2<K> with input key as key and return it. If not present,
     * return null;
     *
     * @param key key
     * @return a SkipListNode2<T> or null
     */
    public SkipListNode<T> get(T key) {
        readLock.lock();
        try {
            SkipListNode<T> found = find(key);

            if (found.data == null)
                return null;

            if (found.data.compareTo(key) == 0 && !found.isDeleted())
                return found;
        } finally {
            readLock.unlock();
        }
        return null;
    }

    /**
     * Does the skiplist contain a node with given key?
     * 
     * @param key
     * @return
     */
    public boolean containsKey(T key) {
        SkipListNode<T> node = find(key);
        return node.data != null && node.data.compareTo(key) == 0 && !node.isDeleted();
    }

    /**
     * add a new SkipListNode2<K> if a SkipListNode2<K> with same key was not found.
     * Update if a SkipListNode2<K> with same key was found.
     *
     * @param key key
     * @param val value
     */
    public void put(T node) {
        SkipListNode<T> found = null, cur = null;

        writeLock.lock();
        try {
            found = find(node);

            if (found.data != null && found.data.compareTo(node) == 0) {
                found.data = node;
                writeLock.unlock();
                return;
            }

            cur = new SkipListNode<T>(node);
            insertRight(found, cur);
        } finally {
            if (writeLock.isHeldByCurrentThread())
                writeLock.unlock();
        }

        curLevel = 0;
        while (prob.nextDouble() < 0.5) {
            if (curLevel >= maxLevels.get()) {
                addNewLayer();
                maxLevels.getAndIncrement();
            }

            while (found.up == null && found.left != null) {
                found = found.left;
            }

            if (found.up != null) {
                found = found.up;
                cur = addNewNodeToTower(found, cur);
                curLevel++;
            }
        }

        length++;
        size.getAndAdd(node.length());
    }

    /**
     * delete the SkipListNode2<K> identified by the key
     *
     * @param key key
     * @return true if a SkipListNode2<K> was removed, false if SkipListNode2<K> was
     *         not found
     */
    public boolean del(T node) {
        SkipListNode<T> found = find(node);
        if (found.data != null && found.data.compareTo(node) == 0) {
            found.delete();
            length--;
            size.getAndAdd(-(node.length()));
            return true;
        }
        return false;
    }

    /**
     * length of the list
     * 
     * @return
     */
    public int length() {
        return length;
    }

    /**
     * size of all nodes in bytes
     * 
     * @return long
     */
    public long size() {
        return size.get();
    }

    /**
     * add a new layer to the top
     */
    private void addNewLayer() {
        SkipListNode<T> h = new SkipListNode<T>(null);
        SkipListNode<T> t = new SkipListNode<T>(null);

        h.right = t;
        t.left = h;

        h.down = this.head;
        t.down = this.tail;

        this.head.up = h;
        this.tail.up = t;

        this.head = h;
        this.tail = t;
    }

    /**
     * add dummy right of p, above q
     *
     * @param p SkipListNode2
     * @param q SkipListNode2
     * @return SkipListNode2
     */
    private SkipListNode<T> addNewNodeToTower(SkipListNode<T> p, SkipListNode<T> q) {
        SkipListNode<T> dummy = new SkipListNode<T>(q.data);

        dummy.left = p;
        dummy.right = p.right;
        p.right.left = dummy;
        p.right = dummy;

        dummy.down = q;
        q.up = dummy;

        return dummy;
    }

    /**
     * insert q to the right of p
     *
     * @param p SkipListNode2
     * @param q SkipListNode2
     */
    private void insertRight(SkipListNode<T> p, SkipListNode<T> q) {
        p.right.left = q;
        q.right = p.right;

        q.left = p;
        p.right = q;
    }

    /**
     * Not implementing iterable interface, but provides one on-demand
     *
     * @return Iterable<SkipListNode2>
     */
    public Iterator<SkipListNode<T>> iterator() {
        return new Iterator<SkipListNode<T>>() {
            SkipListNode<T> currentNode;

            public Iterator<SkipListNode<T>> init() {
                currentNode = sentinel;
                while (currentNode.down != null)
                    currentNode = currentNode.down;

                return this;
            }

            @Override
            public SkipListNode<T> next() {
                currentNode = currentNode.right;
                return currentNode;
            }

            @Override
            public boolean hasNext() {
                while (currentNode.right != null && currentNode.right.data != null) {
                    if (!currentNode.right.isDeleted())
                        return true;

                    currentNode = currentNode.right;
                }
                return false;
            }
        }.init();
    }

    /**
     * print friendly representation of current state of the skipList
     */
    @Override
    public String toString() {
        List<List<String>> rows = new ArrayList<>();
        SkipListNode<T> curNode = sentinel, temp = curNode;
        int col = 0, curLvl = 0;

        while (temp != null && curLvl <= maxLevels.get()) {
            while (curLvl <= maxLevels.get()) {
                rows.add(new ArrayList<String>());
                String val = "";
                if (temp != null) {
                    val = col == 0 ? "head"
                            : (temp.right == null) ? "tail"
                                    : temp.isDeleted() ? "[" + temp.data.toString() + "]" : temp.data.toString();
                    temp = temp.up;
                }
                rows.get(curLvl++).add(val);
            }

            col++;
            curLvl = 0;
            curNode = curNode.right;
            temp = curNode;
        }

        StringBuilder builder = new StringBuilder("\n");
        for (int r = rows.size() - 1; r >= 0; r--) {
            StringBuilder line = new StringBuilder();

            for (String key : rows.get(r))
                line.append(String.format("%8.8s ", key == null ? "" : key));

            if (!"".equals(line.toString().trim()))
                builder.append(line.append("\n"));
        }

        return builder.toString();
    }
}