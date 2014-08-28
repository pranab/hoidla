/*
 * hoidla: various algorithms for Big Data solutions
 * Author: Pranab Ghosh
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hoidla.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;

/**
 * @author pranab
 *
 */
public class BoundedSortedObjects {
	private List<SortableObject> items = new ArrayList<SortableObject>();
	private int maxSize;
	private boolean contTruncation;
	
	/**
	 * @author pranab
	 *
	 */
	public static class SortableObject  implements Comparable<SortableObject> {
		private ImmutablePair<Integer, Object> pair;
		
		public SortableObject(int rank, Object item) {
			pair = new ImmutablePair<Integer, Object>(rank,  item);
		}
		
		public int getRank() {
			return pair.getLeft();
		}
		
		public Object getItem() {
			return pair.getRight();
		}

		@Override
		public int compareTo(SortableObject that) {
			return that.getRank() - this.getRank();
		}
		
		public String toString() {
			return pair.getRight().toString() + ":" + pair.getLeft().toString();
		}
	}
	
	/**
	 * @param maxSize
	 */
	public BoundedSortedObjects(int maxSize) {
		this.maxSize = maxSize;
	}
	
	/**
	 * @param maxSize
	 */
	public BoundedSortedObjects(int maxSize, boolean contTruncation) {
		this.maxSize = maxSize;
		this.contTruncation = contTruncation;
	}
	
	/**
	 * @param contTruncation
	 */
	public void setContTruncation(boolean contTruncation) {
		this.contTruncation = contTruncation;
	}


	/**
	 * @param item
	 * @param rank
	 */
	public void add(Object item, int rank) {
		//remove if exists
		SortableObject foundItem = null;
		for (SortableObject thisItem : items) {
			if (thisItem.getItem().equals(item)) {
				foundItem = thisItem;
				break;
			}
		}
		if (null != foundItem) {
			items.remove(foundItem);
		}
		
		//add
		SortableObject newItem = new SortableObject(rank, item);
		items.add(newItem);
		
		if (contTruncation)  {
			Collections.sort(items);
			if (items.size() > maxSize) {
				items.remove(items.size() - 1);
			}
		}
	}	

	/**
	 * 
	 */
	public void truncate () {
		Collections.sort(items);
		if (items.size() > maxSize) {
			List<SortableObject> tempItems = new ArrayList<SortableObject>();
			for (int i = 0; i < maxSize; ++i) {
				tempItems.add(items.get(i));
			}
			items = tempItems;
		}
	}
	
	/**
	 * @return
	 */
	public List<SortableObject> get() {
		return items;
	}

	public void clear() {
		items.clear();
	}
}
