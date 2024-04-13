package com.kochudb.storage;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import com.kochudb.types.ByteArray;

/**
 * SkipList is a probabilistic list that offers average O(log n) runtime
 * for insert, search, delete and update operations.
 */
public class SkipList {

	private AtomicInteger maxLevels;
	private int curLevel, length;

	private SkipListNode sentinel, head, tail;

	private final WriteLock writeLock;
	private final ReadLock readLock;
	private Random prob;
    
	/**
     * Constructor
     */
    public SkipList() {
		head = new SkipListNode(null, null);
		tail = new SkipListNode(null, null);

		head.right = tail;
		tail.left = head;

		sentinel = head;

		maxLevels = new AtomicInteger(0);
		curLevel = 0;

		prob = new Random();
		length = 0;

		ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();
		readLock = reentrantReadWriteLock.readLock();
		writeLock = reentrantReadWriteLock.writeLock();
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
		readLock.lock();
		try {
			SkipListNode found = find(key);
			
			if (found.key == null)
				return null;
			
			if (found.key.compareTo(key) == 0 && !found.isDeleted())
				return found;
		} finally {
			readLock.unlock();
		}
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
    	SkipListNode found = null, cur = null;

		writeLock.lock();
		try {
			found = find(key);

			if (found.key != null && found.key.compareTo(key) == 0) {
				found.setValue(val);
				writeLock.unlock();
				return;
			}

			cur = new SkipListNode(key, val);
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

			try {
				if (found.up != null) {
					found = found.up;
					cur = addNewNodeToTower(found, cur);
					curLevel++;
				}
			} catch (Exception e) {
				System.out.println(Thread.currentThread().getName() + " exception");
				System.out.println(Thread.currentThread().getName() + " key: " + key);
				System.out.println(
						Thread.currentThread().getName() + " curLevel: " + curLevel + ", levels: " + maxLevels.get());
				System.out.println(Thread.currentThread().getName() + "\n" + this.toString());
				e.printStackTrace();
				System.out.println(Thread.currentThread().getName() + " key: " + found.key);
				// System.exit(1);
			}
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
			found.delete();
			length--;
			return true;
		}
		return false;
	}

	/**
	 * length of the list
	 * @return
	 */
	public int length() {
		return length;
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
    }

    /**
     * add dummy right of p, above q
     *
     * @param p SkipListNode
     * @param q SkipListNode
     * @return SkipListNode
     */
    private SkipListNode addNewNodeToTower(SkipListNode p, SkipListNode q) {
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
		SkipListNode curNode = sentinel, temp = curNode;
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