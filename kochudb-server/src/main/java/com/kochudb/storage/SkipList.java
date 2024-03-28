package com.kochudb.storage;

import java.util.Iterator;
import java.util.Random;

import com.kochudb.types.ByteArray;

/**
 * SkipList is a probabilistic list that offers average O(log n) runtime
 * for insert, search, delete and update operations.
 */
public class SkipList {

    // initial nodes, sentinel references head
    SkipListNode head, tail, sentinel;

    // number of levels, and length of SkipList
    int levels, length;

    // probability function
    Random prob;

    /**
     * Constructor
     */
    public SkipList() {
        head = new SkipListNode(null, null);
        tail = new SkipListNode(null, null);

        head.right = tail;
        tail.left = head;

        levels = 0;
        length = 0;
        prob = new Random();

        sentinel = head;
    }

    /**
     * find the SkipListNode by the key. If not present in SkipList, find the immediate left
     * SkipListNode which has the floor value of key (floorSkipListNode(key))
     *
     * @param key key
     * @return a SkipListNode with the 'key' of floor(key)
     */
    public SkipListNode find(ByteArray key) {
        SkipListNode cur = head;

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
     * find a SkipListNode with input key as key and return it. If not present, return null;
     *
     * @param key key
     * @return a SkipListNode or null
     */
    public SkipListNode get(ByteArray key) {
        SkipListNode found = find(key);

        if (found.key != null && found.key.compareTo(key) == 0)
            return found;

        return null;
    }

    public boolean containsKey(ByteArray key) {
        SkipListNode node = find(key);
        return node.key != null && node.key.compareTo(key) == 0;
    }

    /**
     * add a new SkipListNode if a SkipListNode with same key was not found. Update if a
     * SkipListNode with same key was found.
     *
     * @param key key
     * @param val value
     */
    public void put(ByteArray key, ByteArray val) {
        SkipListNode found = find(key);
        if (found.key != null && found.key.compareTo(key) == 0) {
            found.val = val;
            return;
        }

        SkipListNode cur = new SkipListNode(key, val);

        insertRight(found, cur);

        // update tower
        int curLevel = 0;

        while (prob.nextDouble() < 0.5) {
            if (curLevel == levels)
                addNewLayer();

            while (found.up == null)
                found = found.left;

            found = found.up;

            cur = addNewSkipListNodeToTower(found, cur);
            curLevel++;
        }
        length++;
    }

    /**
     * delete the SkipListNode identified by the key
     *
     * @param key key
     * @return true if a SkipListNode was removed, false if SkipListNode was not found
     */
    public boolean del(ByteArray key) {
        SkipListNode found = find(key);
        if (found.key != null && found.key.compareTo(key) == 0) {
            while (found != null) {
                unlinkSkipListNode(found);
                found = found.up;
            }
            length--;
            return true;
        }
        return false;
    }

    /**
     * remove left, right, down references
     *
     * @param node skipListNode
     */
    private void unlinkSkipListNode(SkipListNode node) {
        node.left.right = node.right;
        node.right.left = node.left;

        node.left = null;
        node.right = null;
        node.down = null;
    }

    /**
     * add a new layer to the top
     */
    private void addNewLayer() {
        SkipListNode h = new SkipListNode(null, null);
        SkipListNode t = new SkipListNode(null, null);

        h.right = t;
        h.down = head;
        head.up = h;

        t.left = h;
        t.down = tail;
        tail.up = t;

        head = h;
        tail = t;
        levels++;
    }

    /**
     * add dummy right of p, above q
     *
     * @param p SkipListNode
     * @param q SkipListNode
     * @return SkipListNode
     */
    private SkipListNode addNewSkipListNodeToTower(SkipListNode p, SkipListNode q) {
        SkipListNode dummy = new SkipListNode(q.key, null);

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
    private void insertRight(SkipListNode p, SkipListNode q) {
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
                return currentNode.right.key != null;
            }
        }.init();
    }

    /**
     * print friendly representation of current state of the skipList
     */
    @Override
    public String toString() {
        String[][] rows = new String[levels + 1][length + 2];
        SkipListNode k = head;
        while (k.down != null)
            k = k.down;

        int col = 0, curLvl = 0;
        SkipListNode temp = k;

        while (temp != null) {
            while (temp != null) {
                String val = col == 0 ? "head" : (col == length + 1) ? "tail" : temp.key.toString();
                rows[curLvl++][col] = val;
                temp = temp.up;
            }
            col++;
            curLvl = 0;
            k = k.right;
            temp = k;
        }

        StringBuilder builder = new StringBuilder();
        for (int r = rows.length - 1; r >= 0; r--) {
            String[] row = rows[r];
            for (String key : row)
                builder.append(String.format("%4.4s ", key == null ? "" : key));

            builder.append("\n");
        }

        return builder.toString();
    }
}