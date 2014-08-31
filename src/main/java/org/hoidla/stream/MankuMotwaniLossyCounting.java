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

package org.hoidla.stream;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.hoidla.util.BoundedSortedObjects;
import org.hoidla.util.Expirer;
import org.hoidla.util.ObjectCounter;
import org.hoidla.util.SequencedObjectCounter;
import org.hoidla.util.SimpleObjectCounter;

/**
 * @author pranab
 *
 */
public class MankuMotwaniLossyCounting extends FrequentItems.FrequentItemsFinder {
	private int bucketSize;
	private long currentBucket;
	private Map<Object, ImmutablePair<ObjectCounter, Long>> buckets = 
			new HashMap<Object, ImmutablePair<ObjectCounter, Long>>(); 
	private int maxFrequentItems;
	
	public MankuMotwaniLossyCounting(double errorLimit, int maxFrequentItems) {
		bucketSize = (int)(1 / errorLimit);
		this.maxFrequentItems = maxFrequentItems;
	}
	
	@Override
	public void setExpirer(Expirer expirer) {
	}

	@Override
	public void add(Object value) {
		++count;
		currentBucket = count / bucketSize + 1;
		
		//add
		ImmutablePair<ObjectCounter, Long> counterWithError = buckets.get(value);
		if (null == counterWithError) {
			currentBucket = count / bucketSize;
			counterWithError = new ImmutablePair<ObjectCounter, Long>(new SimpleObjectCounter(), currentBucket - 1);
			buckets.put(value, counterWithError);
		} 
		counterWithError.getLeft().increment();
		
		//delete
		delete();
	}

	@Override
	public void add(Object value, long sequence) {
		//expire from window
		ImmutablePair<ObjectCounter, Long> counterWithError = null;
		if (null != expirer) {
			toBeRemoved.clear();
			for (Object key : buckets.keySet()) {
				counterWithError = buckets.get(key);
				counterWithError.getLeft().expire(expirer, sequence);
				
				if (counterWithError.getLeft().isZero()) {
					toBeRemoved.add(key);
				}
			}				
			for (Object item :  toBeRemoved) {
				buckets.remove(item);
			}
		}
		
		++count;
		currentBucket = count / bucketSize + 1;
		
		//add
		counterWithError = buckets.get(value);
		if (null == counterWithError) {
			currentBucket = count / bucketSize;
			counterWithError = new ImmutablePair<ObjectCounter, Long>(new SequencedObjectCounter(), currentBucket - 1);
			buckets.put(value, counterWithError);
		} 
		counterWithError.getLeft().increment(sequence);
		
		//delete
		delete();
	}

	@Override
	public List<BoundedSortedObjects.SortableObject> get() {
		BoundedSortedObjects sortedObjects  = new  BoundedSortedObjects(maxFrequentItems);		
		ImmutablePair<ObjectCounter, Long> counterWithError = null;
		int i = 0;
		for (Object key : buckets.keySet()) {
			counterWithError = buckets.get(key);
			sortedObjects.add(key, counterWithError.getLeft().getCount());
			if (++i  % 1000 == 0) {
				sortedObjects.truncate();
			}
		}		
		sortedObjects.truncate();
		return sortedObjects.get();
	}

	private void delete() {
		//delete
		ImmutablePair<ObjectCounter, Long> counterWithError = null;
		toBeRemoved .clear();
		for (Object item : buckets.keySet()) {
			counterWithError = buckets.get(item);
			if (counterWithError.getLeft().getCount() + counterWithError.getRight() < currentBucket) {
				toBeRemoved.add(item);
			}
		}
		for (Object item :  toBeRemoved) {
			buckets.remove(item);
		}
	}

	@Override
	public void expire() {
		// TODO Auto-generated method stub
	}
}
