package com.kochudb.types;

import java.util.Iterator;
import java.util.Random;

/**
 * Skiplist is a probabilistic list that offers average O(log n) runtime
 * for insert, search, delete and update operations.
 * 
 * Coin toss probability is always 0.5 in this implementation
 */
public class Skiplist {

	// initial nodes, sentinel references head
	SkiplistNode head, tail, sentinel;
	
	// number of levels, and length of skiplist
	int levels, length;
	
	// probability function
	Random prob;

	/**
	 * Constructor
	 */
	public Skiplist() {
		head = new SkiplistNode(null, null);
		tail = new SkiplistNode(null, null);
		
		head.right = tail;
		tail.left = head;
		
		levels = 0;
		length = 0;
		prob = new Random();
		
		sentinel = head;
	}

	/**
	 * find the SkiplistNode by the key. If not present in skiplist, find the immediate left
	 * SkiplistNode which has the floor value of key (floorSkiplistNode(key))
	 *
	 * @param key key
	 * @return a SkiplistNode with the 'key' of floor(key)
	 */
	public SkiplistNode find(ByteArray key) {
		SkiplistNode cur = head;

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
	 * find a SkiplistNode with input key as key and return it. If not present, return null;
	 *
	 * @param key key
	 * @return a SkiplistNode or null
	 */
	public SkiplistNode get(ByteArray key) {
		SkiplistNode found = find(key);

		if (found.key.compareTo(key) == 0)
			return found;

		return null;
	}

	public boolean containsKey(ByteArray key) {
		SkiplistNode node = find(key);
		return node.key != null && node.key.compareTo(key) == 0;
	}
	
	/**
	 * add a new SkiplistNode if a SkiplistNode with same key was not found. Update if a SkiplistNode with
	 * same key was found.
	 *
	 * @param key key
	 * @param val value
	 */
	public void put(ByteArray key, byte[] val) {
		SkiplistNode found = find(key);
		if (found.key != null && found.key.compareTo(key) == 0) {
			found.val = val;
			return;
		}

		SkiplistNode cur = new SkiplistNode(key, val);

		insertRight(found, cur);

		// update tower
		int curLevel = 0;

		while (prob.nextDouble() < 0.5) {
			if (curLevel == levels)
				addNewLayer();

			while (found.up == null)
				found = found.left;

			found = found.up;

			cur = addNewSkiplistNodeToTower(found, cur);
			curLevel++;
		}
		length++;
	}

	/**
	 * delete the SkiplistNode identified by the key
	 *
	 * @param key key
	 * @return true if a SkiplistNode was removed, false if SkiplistNode was not found
	 */
	public boolean del(ByteArray key) {
		SkiplistNode found = find(key);
		if (found.key != null && found.key.compareTo(key) == 0) {
			while (found != null) {
				unlinkSkiplistNode(found);
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
	 * @param SkiplistNode
	 */
	void unlinkSkiplistNode(SkiplistNode node) {
		node.left.right = node.right;
		node.right.left = node.left;

		node.left = null;
		node.right = null;
		node.down = null;
	}

	/**
	 * add a new layer to the top
	 */
	void addNewLayer() {
		SkiplistNode h = new SkiplistNode(null, null);
		SkiplistNode t = new SkiplistNode(null, null);

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
	 * @param p
	 * @param q
	 * @return
	 */
	SkiplistNode addNewSkiplistNodeToTower(SkiplistNode p, SkiplistNode q) {
		SkiplistNode dummy = new SkiplistNode(q.key, null);

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
	 * @param p
	 * @param q
	 */
	void insertRight(SkiplistNode p, SkiplistNode q) {
		p.right.left = q;
		q.right = p.right;

		q.left = p;
		p.right = q;
	}


	/**
	 * Not implementing iterable interface, but provides one on-demand
	 * 
	 * @return Iterable<SkiplistNode>
	 */
	public Iterator<SkiplistNode> iterator() {
		
		return new Iterator<SkiplistNode>() {
			SkiplistNode currentNode;
			
			public Iterator<SkiplistNode> init() {
				currentNode = sentinel;
				
				while (currentNode.down != null)
					currentNode = currentNode.down;
				
				return this;
			}
			
			@Override
			public SkiplistNode next() {
				return currentNode.right;
			}
			
			@Override
			public boolean hasNext() {
				return currentNode.right != null;
			}
		}.init();
	}
	
	/**
	 * helper function, prints current state of skiplist
	 */
	@Override
	public String toString() {
		String[][] rows = new String[levels + 1][length + 2];
		SkiplistNode k = head;
		while (k.down != null)
			k = k.down;

		int col = 0, curLvl = 0;
		SkiplistNode temp = k;

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