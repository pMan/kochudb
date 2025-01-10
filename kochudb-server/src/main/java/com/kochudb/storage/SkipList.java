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

import com.kochudb.types.KochuDoc;

/**
 * SkipList is a probabilistic list that offers average O(log n) runtime for
 * insert, search, delete and update operations.
 * 
 * This implementation is thread safe. Delete operation is a soft-delete.
 */
public class SkipList {

    private AtomicInteger maxLevels;
    private int curLevel, length;
    private AtomicLong size;

    private SkipListNode sentinel, head, tail;

    public final WriteLock writeLock;
    public final ReadLock readLock;
    private Random prob;

    /**
     * Constructor
     */
    public SkipList() {
        head = new SkipListNode(null);
        tail = new SkipListNode(null);

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
     * find the SkipListNode2<K> by the doc. If not present in SkipList, find the
     * immediate left SkipListNode2 which has the floor value of doc
     * (floorSkipListNode2(doc))
     *
     * @param doc doc
     * @return a SkipListNode2<K> with the 'doc' of floor(doc)
     */
    public SkipListNode find(KochuDoc doc) {
        SkipListNode cur = head;

        while (true) {
            while (cur.right.data != null && cur.right.compareTo(doc) <= 0)
                cur = cur.right;

            if (cur.down == null)
                break;

            cur = cur.down;
        }
        return cur;
    }

    /**
     * find a SkipListNode2<K> with input doc as doc and return it. If not present,
     * return null;
     *
     * @param doc doc
     * @return a SkipListNode2 or null
     */
    public SkipListNode get(KochuDoc doc) {
        readLock.lock();
        try {
            SkipListNode found = find(doc);

            if (found.data == null)
                return null;

            if (found.compareTo(doc) == 0 && !found.isDeleted())
                return found;
        } finally {
            readLock.unlock();
        }
        return null;
    }

    /**
     * Does the skiplist contain a node with given doc?
     * 
     * @param doc
     * @return
     */
    public boolean containsKey(KochuDoc doc) {
        SkipListNode node = find(doc);
        return node.data != null && node.compareTo(doc) == 0 && !node.isDeleted();
    }

    /**
     * add a new SkipListNode2<K> if a SkipListNode2<K> with same doc was not found.
     * Update if a SkipListNode2<K> with same doc was found.
     *
     * @param doc doc
     * @param val value
     */
    public void put(KochuDoc doc) {
        SkipListNode found = null, cur = null;

        writeLock.lock();
        try {
            found = find(doc);

            if (found.data != null && found.compareTo(doc) == 0) {
                found.data = doc;
                writeLock.unlock();
                return;
            }

            cur = new SkipListNode(doc);
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
        size.getAndAdd(doc.length());
    }

    /**
     * delete the SkipListNode2<K> identified by the doc
     *
     * @param doc doc
     * @return true if a SkipListNode2<K> was removed, false if SkipListNode2<K> was
     *         not found
     */
    public boolean del(KochuDoc doc) {
        SkipListNode found = find(doc);
        if (found.data != null && found.compareTo(doc) == 0) {
            found.delete();
            length--;
            size.getAndAdd(-(doc.length()));
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
        SkipListNode h = new SkipListNode(null);
        SkipListNode t = new SkipListNode(null);

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
    private SkipListNode addNewNodeToTower(SkipListNode p, SkipListNode q) {
        SkipListNode dummy = new SkipListNode(q.data);

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
    private void insertRight(SkipListNode p, SkipListNode q) {
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
    public Iterator<SkipListNode> iterator() {
        return new Iterator<SkipListNode>() {
            SkipListNode currentNode;

            public Iterator<SkipListNode> init() {
                currentNode = sentinel;
                while (currentNode.down != null)
                    currentNode = currentNode.down;

                return this;
            }

            @Override
            public SkipListNode next() {
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
        SkipListNode curNode = sentinel, temp = curNode;
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

            for (String doc : rows.get(r))
                line.append(String.format("%8.8s ", doc == null ? "" : doc));

            if (!"".equals(line.toString().trim()))
                builder.append(line.append("\n"));
        }

        return builder.toString();
    }
}