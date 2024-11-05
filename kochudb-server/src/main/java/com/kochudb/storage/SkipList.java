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
public class SkipList<K extends KochuDBSerde<K>, V extends KochuDBSerde<V>> {

    private AtomicInteger maxLevels;
    private int curLevel, length;
    private AtomicLong size;

    private SkipListNode<K, V> sentinel, head, tail;

    public final WriteLock writeLock;
    public final ReadLock readLock;
    private Random prob;

    /**
     * Constructor
     */
    public SkipList() {
        head = new SkipListNode<K, V>(null, null);
        tail = new SkipListNode<K, V>(null, null);

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
     * find the SkipListNode<K> by the key. If not present in SkipList, find the
     * immediate left SkipListNode<K, V> which has the floor value of key
     * (floorSkipListNode(key))
     *
     * @param key key
     * @return a SkipListNode<K> with the 'key' of floor(key)
     */
    public SkipListNode<K, V> find(K key) {
        SkipListNode<K, V> cur = head;

        while (true) {
            while (cur.right.key != null && cur.right.key.compareTo(key) <= 0)
                cur = cur.right;

            if (cur.down == null)
                break;

            cur = cur.down;
        }
        return cur;
    }

    /**
     * find a SkipListNode<K> with input key as key and return it. If not present,
     * return null;
     *
     * @param key key
     * @return a SkipListNode<K, V> or null
     */
    public SkipListNode<K, V> get(K key) {
        readLock.lock();
        try {
            SkipListNode<K, V> found = find(key);

            if (found.key == null)
                return null;

            if (found.key.compareTo(key) == 0 && !found.isDeleted())
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
    public boolean containsKey(K key) {
        SkipListNode<K, V> node = find(key);
        return node.key != null && node.key.compareTo(key) == 0 && !node.isDeleted();
    }

    /**
     * add a new SkipListNode<K> if a SkipListNode<K> with same key was not found.
     * Update if a SkipListNode<K> with same key was found.
     *
     * @param key key
     * @param val value
     */
    public void put(K key, V val) {
        SkipListNode<K, V> found = null, cur = null;

        writeLock.lock();
        try {
            found = find(key);

            if (found.key != null && found.key.compareTo(key) == 0) {
                found.setValue(val);
                writeLock.unlock();
                return;
            }

            cur = new SkipListNode<K, V>(key, val);
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
        size.getAndAdd(key.length() + val.length());
    }

    /**
     * delete the SkipListNode<K> identified by the key
     *
     * @param key key
     * @return true if a SkipListNode<K> was removed, false if SkipListNode<K> was
     *         not found
     */
    public boolean del(K key) {
        SkipListNode<K, V> found = find(key);
        if (found.key != null && found.key.compareTo(key) == 0) {
            found.delete();
            length--;
            size.getAndAdd(-(key.length() + found.val.length()));
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
        SkipListNode<K, V> h = new SkipListNode<K, V>(null, null);
        SkipListNode<K, V> t = new SkipListNode<K, V>(null, null);

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
     * @param p SkipListNode
     * @param q SkipListNode
     * @return SkipListNode
     */
    private SkipListNode<K, V> addNewNodeToTower(SkipListNode<K, V> p, SkipListNode<K, V> q) {
        SkipListNode<K, V> dummy = new SkipListNode<K, V>(q.key, null);

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
     * @param p SkipListNode
     * @param q SkipListNode
     */
    private void insertRight(SkipListNode<K, V> p, SkipListNode<K, V> q) {
        p.right.left = q;
        q.right = p.right;

        q.left = p;
        p.right = q;
    }

    /**
     * Not implementing iterable interface, but provides one on-demand
     *
     * @return Iterable<SkipListNode>
     */
    public Iterator<SkipListNode<K, V>> iterator() {
        return new Iterator<SkipListNode<K, V>>() {
            SkipListNode<K, V> currentNode;

            public Iterator<SkipListNode<K, V>> init() {
                currentNode = sentinel;
                while (currentNode.down != null)
                    currentNode = currentNode.down;

                return this;
            }

            @Override
            public SkipListNode<K, V> next() {
                currentNode = currentNode.right;
                return currentNode;
            }

            @Override
            public boolean hasNext() {
                while (currentNode.right != null && currentNode.right.key != null) {
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
        SkipListNode<K, V> curNode = sentinel, temp = curNode;
        int col = 0, curLvl = 0;

        while (temp != null && curLvl <= maxLevels.get()) {
            while (curLvl <= maxLevels.get()) {
                rows.add(new ArrayList<String>());
                String val = "";
                if (temp != null) {
                    val = col == 0 ? "head"
                            : (temp.right == null) ? "tail"
                                    : temp.isDeleted() ? "[" + temp.key.toString() + "]" : temp.key.toString();
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
                line.append(String.format("%4.4s ", key == null ? "" : key));

            if (!"".equals(line.toString().trim()))
                builder.append(line.append("\n"));
        }

        return builder.toString();
    }
}